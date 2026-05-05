# Databricks notebook source
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # Market Basket Analysis
# MAGIC This notebook implements a **Market Basket Analysis (MBA)** pipeline on historical pizza-chain cart transactions using PySpark's FPGrowth.
# MAGIC
# MAGIC It shares a **common held-out test set** (`test_dataset`) with the ALS notebook (`02_collaborative_filter`) so that Hit@k scores are directly comparable across both approaches.
# MAGIC
# MAGIC ### Compute Requirements
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Runtime** | DBR 17.3 ML |
# MAGIC | **Compute** | Classic cluster |
# MAGIC | **Packages** | `databricks-feature-engineering` (installed in cell 2) |
# MAGIC
# MAGIC | Section | Description |
# MAGIC |---|---|
# MAGIC | **1 — Support Calculation** | Explore item frequencies and calculate support across the **training** set |
# MAGIC | **2 — Model Construction** | Train an FPGrowth model on `train_dataset`, generate association rules, and evaluate Hit@k on the **shared test set** |
# MAGIC | **3 — Deployment** | Log a custom PyFunc to MLflow / Unity Catalog via two patterns: |
# MAGIC | | **3a — Packaged Parquet** — rules bundled as a static artifact (simple, self-contained) |
# MAGIC | | **3b — Lakebase-backed** — rules read from an online table at load time (for large rule sets or always-fresh rules) |
# MAGIC
# MAGIC ### Data Splits
# MAGIC ```
# MAGIC 00_data_preparation outputs (all persisted as Delta tables):
# MAGIC   ├── train_dataset   (80 %, seed=42) → FPGrowth training + support calculation
# MAGIC   └── test_dataset    (20 %, seed=42) → Hit@k evaluation (shared with ALS notebook)
# MAGIC ```
# MAGIC
# MAGIC ### Serving Architecture
# MAGIC ```
# MAGIC Frontend  →  Model Serving endpoint
# MAGIC                ├── 3a: MBARecommenderModel     (rules packaged as parquet artifact)
# MAGIC                └── 3b: MBALakebaseRecommenderModel  (rules read from online table)
# MAGIC                     │
# MAGIC                     └── predict(cart) → cross-join × rules → top-k recommendations
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Install dependencies
# MAGIC %pip install --upgrade databricks-feature-engineering --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Configuration
catalog = 'ioc_sandbox'
schema = 'ai_strategy'

# --- Input tables (from 00_data_preparation) ---
train_table          = f'{catalog}.{schema}.train_dataset'
test_table           = f'{catalog}.{schema}.test_dataset'

# --- Model & experiment names ---
experiment_name      = '/ai-strategy/ryan-marson/RecSysDBX/MBA_recommender_model'
model_name           = f'{catalog}.{schema}.mba_recommender_model'
model_name_lakebase  = f'{catalog}.{schema}.mba_recommender_lakebase'

# --- Rules table ---
rules_table          = f'{catalog}.{schema}.association_rules'

# --- Online store ---
online_store_name    = 'pizza-chain-online-store'
online_store_fallback = 'demo-online-store'
online_table_name    = f'{catalog}.{schema}.association_rules_online'

# COMMAND ----------

# DBTITLE 1,Load cleaned dataset from 00_data_preparation
# Load train and test tables saved by 00_data_preparation
train = spark.read.table(train_table)
test  = spark.read.table(test_table)

print(f'{train.count():,} training orders  |  {test.count():,} test orders (shared with ALS notebook)')
display(train.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 1 — Calculate Support
# MAGIC Support is the proportion of transactions an item shows up in. Values range from 0 to 1; a higher support means a larger share of transactions includes that item (e.g. 0.30 support for `pepperoni-pizza` means it appears in 30 % of all orders).

# COMMAND ----------

# DBTITLE 1,Calculate item support
from pyspark.sql.functions import explode, col, count, split, trim, transform

# Explode order_product_list and compute count + support per product (training data only)
#sdf_unique_count = (
#    train
#    .withColumn('order_product', explode(col('order_product_list')))
#    .groupBy('order_product')
#    .agg(
#        count('*').alias('count'),
#        (count('*') / train.count()).alias('support')
#    )
#    .sort(col('count').desc())
#)

# Test array to clean up duplicates, spaces, etc.
#array_distinct(transform(split(col('ItemNames'), ','), lambda x: trim(x)))

# Explode order_product_list and compute count + support per product (training data only)
sdf_unique_count = (
    train
    #.withColumn('item', explode(split(col('ItemNames'), ',')))
    .withColumn('item', explode(transform(split(col('ItemNames'), ','), lambda x: trim(x))))
    .groupBy('item')
    .agg(
        count('*').alias('count'),
        (count('*') / train.count()).alias('support')
    )
    .sort(col('count').desc())
)

df_eda = sdf_unique_count.toPandas()
display(sdf_unique_count)

# COMMAND ----------

# DBTITLE 1,Visualize top products by support
import matplotlib.pyplot as plt

top_items = df_eda.head(18)
plt.figure(figsize=(10, 4))
plt.bar(top_items['item'], top_items['support'] * 100)
plt.xlabel('Product Slug')
plt.ylabel('Support — % of orders')
plt.title('Product Support Across All Orders')
plt.xticks(rotation=45, ha='right')
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 2 — Model Construction (FPGrowth)
# MAGIC A market basket analysis uses the **Apriori** family of algorithms to discover association rules from transactional data. We use PySpark's `FPGrowth` implementation which requires two key thresholds:
# MAGIC * **Support** — minimum proportion of transactions that must contain an itemset for it to be considered frequent.
# MAGIC * **Confidence** — minimum conditional probability that the consequent is purchased given the antecedent is in the cart (ranges 0 → 1).
# MAGIC
# MAGIC The model is trained on `train_dataset` and evaluated on the **shared held-out test set** (`test_dataset`) — the same split used by the ALS notebook for a fair comparison.

# COMMAND ----------

# DBTITLE 1,Train FPGrowth model
from pyspark.ml.fpm import FPGrowth
from pyspark.sql.functions import split, col, trim, transform, array_distinct
import mlflow

mlflow.set_experiment(experiment_name)

# With ~12 K training orders, set a reasonable floor
min_transactions = 500
num_of_transactions = train.count()
min_confidence = 0.00   # keep all rules; we filter later by score

# ItemNames is a comma-separated string; FPGrowth requires an array of unique items
train_basket = train.withColumn('items', array_distinct(transform(split(col('ItemNames'), ','), lambda x: trim(x))))

fpGrowth = FPGrowth(
    itemsCol='items',
    minSupport=min_transactions / num_of_transactions,
    minConfidence=min_confidence,
)

# Train on the training set (test set is loaded separately for evaluation)
model = fpGrowth.fit(train_basket)
print(f'{model.associationRules.count()} association rules generated')
display(model.associationRules.sort('antecedent', 'consequent'))

# COMMAND ----------

# DBTITLE 1,Save association rules to Unity Catalog
model.associationRules.write.format('delta').mode('overwrite') \
    .option('overwriteSchema', 'true') \
    .saveAsTable(rules_table)

print(f'Association rules saved to {rules_table}')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evaluate with Hit@k
# MAGIC We measure what proportion of the time the **last item added** to a test cart appears in the top-k recommendations. A higher Hit@k means the model is better at predicting what the customer will add next.

# COMMAND ----------

# DBTITLE 1,Define recommendation generator
from pyspark.sql.functions import broadcast, array_intersect, array, expr, size, col, power, row_number, collect_list, struct
from pyspark.sql.window import Window

def generate_recommendations(rules_sdf, cart_sdf, k=5, metric='confidence', testing=False):
    """
    Generate top-k product recommendations based on association rules.
    Matches cart items against rule antecedents and scores by a weighted
    combination of the chosen metric and match coverage.
    """
    # Match cart items with rule antecedents
    baskets_and_rules = (
        cart_sdf.join(
            broadcast(rules_sdf.selectExpr('antecedent', 'consequent', 'lift', 'confidence')),
            on=array_intersect(col('cart'), col('antecedent')) != array()
        )
    )

    # Score each rule: weight by how much of the antecedent overlaps the cart
    score = (
        baskets_and_rules
        .filter(expr('not array_contains(cart, consequent)'))
        .withColumn('intr', expr('size(array_intersect(cart, antecedent))'))
        .withColumn('match_score', expr('power(intr, 2) / (size(antecedent) * size(cart))'))
        .withColumn('rule_score', expr(f'{metric} * match_score'))
    )

    return_columns = ['order_id', 'cart']
    if testing:
        return_columns += ['added']

    # Deduplicate per consequent (keep best score), then take top-k
    top_k = (
        score
        .withColumn('rank', row_number().over(Window.partitionBy('order_id', 'consequent').orderBy(col('rule_score').desc())))
        .filter(col('rank') == 1)
        .withColumn('rank', row_number().over(Window.partitionBy('order_id').orderBy(col('rule_score').desc())))
        .filter(col('rank') <= k)
        .groupBy(return_columns)
        .agg(collect_list(struct('consequent', 'rule_score')).alias('recommendations_with_scores'))
    )
    return top_k

# COMMAND ----------

# DBTITLE 1,Prepare test set and evaluate Hit@k
from pyspark.sql.functions import expr, size, col, array_contains, split, trim, transform, array_distinct

# Use the shared test set from 00_data_preparation (same split as ALS notebook)
test_eval = (
    test
    .selectExpr('EmailAddress', 'CVOrderID as order_id', 'ItemNames')
    # ItemNames is comma-separated; split into a trimmed, deduplicated array
    .withColumn('items', array_distinct(transform(split(col('ItemNames'), ','), lambda x: trim(x))))
)

# Keep only orders with > 1 item, split into cart + last-added item
test_filtered = test_eval.filter(size(col('items')) > 1)

test_transformed = (
    test_filtered
    .withColumn('cart', expr('slice(items, 1, size(items) - 1)'))
    .withColumn('added', expr('items[size(items) - 1]'))
)

# Sample for evaluation — with millions of test orders and 65 K rules the
# broadcast cross-join in generate_recommendations is too expensive to run
# on the full set.  50 K orders gives a tight confidence interval for Hit@k.
EVAL_SAMPLE_SIZE = 50_000
total_eligible = test_transformed.count()
dropped = test_eval.count() - total_eligible

if total_eligible > EVAL_SAMPLE_SIZE:
    sample_fraction = EVAL_SAMPLE_SIZE / total_eligible
    test_sample = test_transformed.sample(fraction=sample_fraction, seed=42).limit(EVAL_SAMPLE_SIZE)
    print(f'{total_eligible:,} eligible test orders (dropped {dropped:,} single-item) — sampling {EVAL_SAMPLE_SIZE:,} for evaluation')
else:
    test_sample = test_transformed
    print(f'{total_eligible:,} test orders (dropped {dropped:,} single-item orders)')

test_sample = test_sample.repartition(spark.sparkContext.defaultParallelism * 4)

# Load rules
rules = (
    spark.read.table(rules_table)
    .selectExpr('antecedent', 'consequent[0] as consequent', 'lift', 'confidence')
)

# --- Static baseline: top-5 most popular items from training data ---
baseline_recommendations = (
    df_eda.head(5)['item'].tolist()
)
print(f'Baseline (top-5 popular): {baseline_recommendations}')

# --- Generate MBA recommendations ---
k = 5
top_k_recommendations = generate_recommendations(rules, test_sample, k=k, metric='confidence', testing=True)

# --- Compare Hit@k ---
top_k_recommendations = top_k_recommendations.withColumn(
    'hit_at_k_mba',
    expr("cast(array_contains(transform(recommendations_with_scores, x -> x.consequent), added) as int)")
).withColumn(
    'hit_at_k_baseline',
    expr("cast(array_contains(slice(array(" +
         ', '.join([f"'{item}'" for item in baseline_recommendations]) +
         f"), 1, {k}), added) as int)")
)

display(top_k_recommendations.agg({'hit_at_k_mba': 'avg', 'hit_at_k_baseline': 'avg'}))

# COMMAND ----------

# DBTITLE 1,Single cart recommendation example
from pyspark.sql import Row

# Load rules (Cell 13 not yet run in this session)
rules = (
    spark.read.table(rules_table)
    .selectExpr('antecedent', 'consequent[0] as consequent', 'lift', 'confidence')
)

cart = [Row(order_id='DEMO-001', cart=['Classic Pepperoni','Classic Cheese'], added='')]
cart_df = spark.createDataFrame(cart)

top_k_recommendations = generate_recommendations(rules, cart_df, k=5, metric='confidence', testing=False)
display(top_k_recommendations)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 — Deployment
# MAGIC
# MAGIC Two patterns for serving MBA recommendations. Choose based on rules-table size and retrain cadence:
# MAGIC
# MAGIC | | **3a — Packaged Parquet** | **3b — Lakebase-backed** |
# MAGIC |---|---|---|
# MAGIC | **Best for** | Rules fit comfortably in serving-container memory | Rules table too large to package as a model artifact |
# MAGIC | **Freshness** | Static snapshot — must re-log the model after each retrain | Always current — `load_context` reads the latest rules at startup |
# MAGIC | **Inference latency** | Fastest — local parquet read, zero network hops | Slight cold-start overhead from the Lakebase query |
# MAGIC | **Infrastructure** | Self-contained; no external dependency at serving time | Requires a Lakebase online table to be provisioned |
# MAGIC | **Reproducibility** | Rules version is pinned to the MLflow run | Rules may change between model loads; pair with table versioning for auditability |
# MAGIC
# MAGIC Both patterns use the **same `predict()` logic**: cross-join the full cart against all rules, score by confidence × match coverage, and return top-k. The only difference is where `load_context` reads the rules from.
# MAGIC
# MAGIC ---
# MAGIC ### 3a — Packaged Parquet (self-contained)
# MAGIC Rules are saved as a parquet file and bundled directly into the MLflow model artifact. At serving time `load_context` reads the local file — zero network dependencies, zero infrastructure beyond the serving endpoint itself.

# COMMAND ----------

# DBTITLE 1,Define custom PyFunc model
import pandas as pd
import mlflow.pyfunc
from mlflow.models.signature import ModelSignature
from mlflow.types import ColSpec, Schema, DataType
from mlflow.types.schema import Array

class MBARecommenderModel(mlflow.pyfunc.PythonModel):
    def __init__(self, k=5, metric='confidence'):
        self.k = k
        self.metric = metric

    def load_context(self, context):
        """Load association rules from packaged parquet artifact."""
        rules_path = context.artifacts['rules_file']
        self.rules_df = pd.read_parquet(rules_path)

    def _calculate_match_score(self, row):
        intersection = set(row['cart']).intersection(row['antecedent'])
        return (len(intersection) ** 2) / (len(row['antecedent']) * len(row['cart']))

    def predict(self, context, model_input):
        merged = model_input.merge(self.rules_df, how='cross')
        if merged.empty:
            return []

        # Keep only rules whose antecedent overlaps the cart
        merged = merged[merged.apply(lambda x: bool(set(x['cart']) & set(x['antecedent'])), axis=1)]
        merged['match_score'] = merged.apply(self._calculate_match_score, axis=1)
        merged['rule_score'] = merged[self.metric] * merged['match_score']

        # Remove consequents already in the cart
        merged = merged[~merged.apply(lambda x: x['consequent'] in x['cart'], axis=1)]

        # Best score per (order_id, consequent), then top-k
        grouped = merged.loc[merged.groupby(['order_id', 'consequent'])['rule_score'].idxmax()]
        top_k = (
            grouped.groupby('order_id')
            .apply(lambda x: x.nlargest(self.k, 'rule_score'))
            .reset_index(drop=True)
        )

        result = top_k.groupby('order_id').agg({
            'cart': 'first',
            'consequent': list,
            'rule_score': list
        }).reset_index()
        result.rename(columns={'consequent': 'recommendations'}, inplace=True)
        return result

# COMMAND ----------

# DBTITLE 1,Log model to MLflow and Unity Catalog
import mlflow
import os

os.makedirs('artifacts', exist_ok=True)
#mlflow.create_experiment(experiment_name)
mlflow.set_experiment(experiment_name)

# Save rules locally as parquet to package as an artifact
rules_pd = (
    spark.read.table(rules_table)
    .selectExpr('antecedent', 'consequent[0] as consequent', 'lift', 'confidence')
).toPandas()
rules_pd.to_parquet('artifacts/rules_table.parquet')

# Define input / output schema
input_schema = Schema([
    ColSpec(DataType.string, 'order_id'),
    ColSpec(Array(DataType.string), 'cart')
])
output_schema = Schema([
    ColSpec(DataType.string, 'order_id'),
    ColSpec(Array(DataType.string), 'cart'),
    ColSpec(Array(DataType.string), 'recommendations'),
    ColSpec(Array(DataType.string), 'rule_score')
])
signature = ModelSignature(inputs=input_schema, outputs=output_schema)

# Sample input for the model
model_input = pd.DataFrame({
    'order_id': ['DEMO-001'],
    'cart': [['pepperoni-pizza', 'crazy-bread']]
})

# Log and register
with mlflow.start_run(run_name='mba_pizza_chain'):
    mlflow.pyfunc.log_model(
        'model',
        python_model=MBARecommenderModel(),
        registered_model_name=model_name,
        artifacts={'rules_file': 'artifacts/rules_table.parquet'},
        input_example=model_input,
        signature=signature
    )
print(f'Model registered as {model_name}')

# COMMAND ----------

# DBTITLE 1,Test inference from logged model
# Load model from latest run and generate recommendations
details = mlflow.last_active_run()
model_uri = f"runs:/{details.info.run_id}/model"
loaded_model = mlflow.pyfunc.load_model(model_uri)

# Use real product names that match the production rules
input_data = pd.DataFrame({
    'order_id': ['DEMO-001', 'DEMO-002'],
    'cart': [
        ['Classic Pepperoni', 'Crazy Bread®'],
        ['Classic Cheese']
    ]
})

recommendations = loaded_model.predict(input_data)
display(recommendations)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### 3b — Lakebase-backed PyFunc
# MAGIC When the rules table is too large to fit in a serving container's memory — or you need rules to stay fresh without re-logging the model — the PyFunc reads from a **Lakebase-backed online table** at `load_context` time.
# MAGIC
# MAGIC The production flow:
# MAGIC 1. Publish the Delta `association_rules` table to a Lakebase online store (one-time setup)
# MAGIC 2. The PyFunc's `load_context` queries the online table via the `databricks-sql-connector` at startup
# MAGIC 3. `predict()` cross-joins the full cart against the fetched rules — identical scoring logic to 3a

# COMMAND ----------

# DBTITLE 1,Create Lakebase online store and publish rules
from databricks.feature_engineering import FeatureEngineeringClient

fe = FeatureEngineeringClient()

# Derive constraint name from the table config (schema-scoped, must be unique)
rules_table_short = rules_table.split('.')[-1]
pk_constraint = f'{rules_table_short}_pk'

# Enable CDF for online store sync
spark.sql(f"""ALTER TABLE {rules_table}
             SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""")

# The rules table uses array-typed columns (antecedent, consequent)
# which cannot serve as primary keys. We add a synthetic rule_id for the
# Online Feature Store, then publish.
spark.sql(f"""CREATE OR REPLACE TABLE {rules_table}_keyed AS
              SELECT monotonically_increasing_id() AS rule_id, *
              FROM {rules_table}""")
spark.sql(f"""ALTER TABLE {rules_table}_keyed ALTER COLUMN rule_id SET NOT NULL""")
spark.sql(f"""ALTER TABLE {rules_table}_keyed DROP CONSTRAINT IF EXISTS {pk_constraint}""")
spark.sql(f"""ALTER TABLE {rules_table}_keyed ADD CONSTRAINT {pk_constraint} PRIMARY KEY (rule_id)""")
spark.sql(f"""ALTER TABLE {rules_table}_keyed
             SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""")
print(f'Created keyed table: {rules_table}_keyed (PK constraint: {pk_constraint})')

# Create or reuse an online store
try:
    store = fe.get_online_store(name=online_store_name)
    print(f'Online store already exists: {online_store_name} (state: {store.state})')
except Exception:
    try:
        print(f'Creating online store: {online_store_name} ...')
        fe.create_online_store(name=online_store_name, capacity='CU_1')
        store = fe.get_online_store(name=online_store_name)
        print(f'Online store created (state: {store.state})')
    except Exception as e:
        store = fe.get_online_store(name=online_store_fallback)
        print(f'Quota reached, reusing shared store: {online_store_fallback} (state: {store.state})')

# Publish the keyed rules table to the online store
fe.publish_table(
    name=f'{rules_table}_keyed',
    online_store=store,
    online_table_name=online_table_name,
)
print(f'Published {rules_table}_keyed as {online_table_name}')

# COMMAND ----------

# DBTITLE 1,Pre-compute per-item recommendation lookup table
import json
import mlflow

class MBALakebaseRecommenderModel(mlflow.pyfunc.PythonModel):
    """
    Same cross-join scoring as RecommenderModel, but reads association
    rules from the Lakebase-backed Delta table at load time rather than
    a packaged parquet artifact.
    """
    def __init__(self, k=5, metric='confidence'):
        self.k = k
        self.metric = metric

    def load_context(self, context):
        """Fetch the latest rules from the Lakebase-backed table."""
        with open(context.artifacts['config']) as f:
            cfg = json.load(f)

        # ---- Production (Model Serving): use databricks-sql-connector ----
        # from databricks import sql
        # with sql.connect(
        #     server_hostname=cfg['server_hostname'],
        #     http_path=cfg['http_path']
        # ) as conn:
        #     cursor = conn.cursor()
        #     cursor.execute(
        #         f"SELECT antecedent, element_at(consequent, 1) AS consequent, "
        #         f"lift, confidence FROM {cfg['rules_table']}"
        #     )
        #     self.rules_df = cursor.fetchall_arrow().to_pandas()

        # ---- Notebook testing: Spark is available ----
        from pyspark.sql import SparkSession
        spark = SparkSession.getActiveSession()
        self.rules_df = (
            spark.table(cfg['rules_table'])
            .selectExpr('antecedent', 'element_at(consequent, 1) as consequent',
                        'lift', 'confidence')
            .toPandas()
        )

    # ---- predict() is identical to MBARecommenderModel ----
    def _calculate_match_score(self, row):
        intersection = set(row['cart']).intersection(row['antecedent'])
        return (len(intersection) ** 2) / (len(row['antecedent']) * len(row['cart']))

    def predict(self, context, model_input):
        merged = model_input.merge(self.rules_df, how='cross')
        if merged.empty:
            return pd.DataFrame()

        merged = merged[merged.apply(
            lambda x: bool(set(x['cart']) & set(x['antecedent'])), axis=1)]
        merged['match_score'] = merged.apply(self._calculate_match_score, axis=1)
        merged['rule_score']  = merged[self.metric] * merged['match_score']
        merged = merged[~merged.apply(lambda x: x['consequent'] in x['cart'], axis=1)]

        grouped = merged.loc[
            merged.groupby(['order_id', 'consequent'])['rule_score'].idxmax()]
        top_k = (
            grouped.groupby('order_id')
            .apply(lambda x: x.nlargest(self.k, 'rule_score'))
            .reset_index(drop=True)
        )
        result = top_k.groupby('order_id').agg({
            'cart': 'first', 'consequent': list, 'rule_score': list
        }).reset_index()
        result.rename(columns={'consequent': 'recommendations'}, inplace=True)
        return result

# COMMAND ----------

# DBTITLE 1,Inference via Lakebase lookup (simulated)
import os

os.makedirs('artifacts', exist_ok=True)

# Config artifact tells load_context which table to query
config = {'rules_table': rules_table}
with open('artifacts/lakebase_config.json', 'w') as f:
    json.dump(config, f)

with mlflow.start_run(run_name='mba_lakebase'):
    mlflow.pyfunc.log_model(
        'model',
        python_model=MBALakebaseRecommenderModel(),
        registered_model_name=model_name_lakebase,
        artifacts={'config': 'artifacts/lakebase_config.json'},
        input_example=model_input,
        signature=signature
    )
print(f'Model registered as {model_name_lakebase}')

# ---- Test inference (same carts as cell 17) ----
details = mlflow.last_active_run()
loaded = mlflow.pyfunc.load_model(f"runs:/{details.info.run_id}/model")

input_data = pd.DataFrame({
    'order_id': ['DEMO-001', 'DEMO-002'],
    'cart': [
        ['pepperoni-pizza', 'crazy-bread'],
        ['cheese-pizza']
    ]
})

recommendations = loaded.predict(input_data)
print('Recommendations (rules read from Lakebase-backed table):')
display(recommendations)
