# Databricks notebook source
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # Collaborative Filtering with ALS
# MAGIC This notebook trains a **user-item collaborative filter** on historical pizza-chain cart transactions using PySpark's Alternating Least Squares (ALS) implementation.
# MAGIC
# MAGIC It shares a **common held-out test set** (`test_dataset`) with the MBA notebook (`01_market_basket_analysis`) so that Hit@k scores are directly comparable across both approaches.
# MAGIC
# MAGIC ### Compute Requirements
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Runtime** | DBR 17.3 ML |
# MAGIC | **Compute** | Classic cluster |
# MAGIC | **Packages** | `optuna`, `databricks-feature-engineering` (installed in cell 2) |
# MAGIC
# MAGIC | Section | Description |
# MAGIC |---|---|
# MAGIC | **1 — Data Preparation** | Load the train / test tables (saved by `00_data_preparation`), build user/item integer mappings, and compute implicit ratings |
# MAGIC | **2 — Model Construction** | Train a baseline ALS model, evaluate on a **validation** split, hyperparameter-tune with Optuna + MLflow, then train the final model on all training data and report Hit@k on the **held-out test set** |
# MAGIC | **3 — Deployment** | Pre-compute top-20 recommendations, publish to a Lakebase online store, and log a Lakebase-backed PyFunc for Model Serving |
# MAGIC
# MAGIC ### Data Splits
# MAGIC ```
# MAGIC 00_data_preparation outputs (all persisted as Delta tables):
# MAGIC   ├── cleaned_mapped_dataset   (full dataset — not used directly by this notebook)
# MAGIC   ├── train_dataset            (80 %, seed=42)
# MAGIC   │     ├── train_hp  (80 %)  → HPO trial training
# MAGIC   │     └── val_hp    (20 %)  → HPO trial evaluation
# MAGIC   └── test_dataset             (20 %, seed=42 — shared with MBA notebook)
# MAGIC ```
# MAGIC
# MAGIC ### Serving Architecture
# MAGIC ```
# MAGIC Frontend  →  Model Serving endpoint (ALSRecommenderModel PyFunc)
# MAGIC                  │
# MAGIC                  └── load_context: reads pre-computed recs from
# MAGIC                      Lakebase online table (als_recommendations)
# MAGIC                  │
# MAGIC                  └── predict(mpid) → ranked product recommendations
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Install dependencies
# MAGIC %pip install optuna --quiet
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
experiment_name      = '/ai-strategy/ryan-marson/RecSysDBX/ALS_recommender_model'
model_name           = f'{catalog}.{schema}.als_recommender_model'

# --- Recommendations table ---
recs_table           = f'{catalog}.{schema}.als_recommendations'

# --- Online store ---
online_store_name    = 'pizza-chain-online-store'
online_store_fallback = 'demo-online-store'
online_table_name    = f'{catalog}.{schema}.als_recommendations_online'

# COMMAND ----------

# DBTITLE 1,Load cleaned dataset and train/test split
from pyspark.sql.functions import col

# Load train and test tables saved by 00_data_preparation
train = spark.read.table(train_table)
test  = spark.read.table(test_table)

# Further split training data: 80 % for HPO training, 20 % for HPO validation
train_hp, val_hp = train.randomSplit([0.8, 0.2], seed=42)

print(f'{train.count():,} training  |  {test.count():,} test (shared with MBA notebook)')
print(f'{train_hp.count():,} HPO train  |  {val_hp.count():,} HPO validation')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 — Data Preparation
# MAGIC 1. Load `train_dataset` and `test_dataset` from Unity Catalog (saved by `00_data_preparation`)
# MAGIC 2. Split training data into `train_hp` (80 %) and `val_hp` (20 %) for HPO
# MAGIC 3. Create integer mappings for users and products (ALS requires numeric IDs)
# MAGIC 4. Generate implicit “ratings” — the proportion of a user’s orders that contain each item
# MAGIC 5. Prepare the validation and test sets for Hit@k evaluation

# COMMAND ----------

# DBTITLE 1,Define preprocessing pipeline for ALS
from pyspark.sql.functions import explode, col, count, sum as spark_sum, row_number, split, trim, transform, array_distinct
from pyspark.sql.window import Window

def preprocess_pipeline(dataset):
    """
    Convert raw orders into the (user_id, item_id, proportion_of_orders) format
    that ALS expects.

    Steps:
      1. Split comma-separated ItemNames into an array of items
      2. Explode → one row per (user, item)
      3. Create integer ID mappings for users and items
      4. Compute an implicit rating: proportion of a user's total orders that
         include each item

    Returns:
      (als_dataset, item_mapping, user_mapping)
    """
    # 1. Split ItemNames into array of trimmed, deduplicated items
    dataset = dataset.withColumn(
        'order_product_list',
        array_distinct(transform(split(col('ItemNames'), ','), lambda x: trim(x)))
    )

    # 2. Explode products
    sdf_exploded = dataset.withColumn('product', explode('order_product_list'))
    als_input = sdf_exploded.select(col('EmailAddress').alias('user'), col('product').alias('item'))

    # 3. Integer ID mappings
    item_mapping = (als_input.select('item').distinct()
                    .withColumn('item_id', row_number().over(Window.orderBy('item'))))
    user_mapping = (als_input.select('user').distinct()
                    .withColumn('user_id', row_number().over(Window.orderBy('user'))))

    als_input_mapped = (
        als_input
        .join(item_mapping, on='item')
        .join(user_mapping, on='user')
        .select('user_id', 'item_id', 'item')
    )

    # 4. Implicit rating = proportion of user's orders containing this item
    als_grouped = als_input_mapped.groupBy('user_id', 'item_id').agg(
        count('*').alias('historical_ordered_amount')
    )
    user_totals = als_grouped.groupBy('user_id').agg(
        spark_sum('historical_ordered_amount').alias('total_orders')
    )
    als_dataset = (
        als_grouped.join(user_totals, on='user_id')
        .withColumn('proportion_of_orders',
                    col('historical_ordered_amount') / col('total_orders'))
    )

    return als_dataset, item_mapping, user_mapping

# COMMAND ----------

# DBTITLE 1,Preprocess training data
# Preprocess the HPO training split (not the full training set)
als_train_dataset, item_mapping, user_mapping = preprocess_pipeline(train_hp)
print(f'HPO training matrix: {als_train_dataset.count():,} user-item pairs')
print(f'  Users:  {user_mapping.count():,}')
print(f'  Items:  {item_mapping.count():,}')

# COMMAND ----------

# DBTITLE 1,Prepare test set for Hit@k evaluation
from pyspark.sql.functions import expr, size, col, split, trim, transform, array_distinct

def prepare_eval_set(dataset, user_mapping, item_mapping, label=''):
    """Hold out the last item in each order and link to training ID mappings."""
    # Split comma-separated ItemNames into array of trimmed, unique items
    dataset = dataset.withColumn(
        'order_product_list',
        array_distinct(transform(split(col('ItemNames'), ','), lambda x: trim(x)))
    )

    filtered = dataset.filter(size(col('order_product_list')) > 1)
    transformed = (
        filtered
        .withColumn('cart', expr('slice(order_product_list, 1, size(order_product_list) - 1)'))
        .withColumn('added', expr('order_product_list[size(order_product_list) - 1]'))
    )
    linked = (
        transformed
        .join(user_mapping, col('EmailAddress') == user_mapping.user, 'inner')
        .join(item_mapping, col('added') == item_mapping.item, 'inner')
        .withColumnRenamed('item_id', 'added_item_id')
    )
    total = dataset.count()
    print(f'{label}: {linked.count():,} linked ({total - linked.count():,} dropped out of {total:,})')
    return linked

# Validation set — used during HPO
val_linked = prepare_eval_set(val_hp, user_mapping, item_mapping, 'Validation')

# Test set — held out until final evaluation (same split as MBA notebook)
test_linked = prepare_eval_set(test, user_mapping, item_mapping, 'Test')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 2 — Model Construction
# MAGIC Training pipeline:
# MAGIC 1. Build a small (low-rank) ALS model to verify the preprocessing pipeline
# MAGIC 2. Evaluate the baseline on the **validation set** (`val_hp`) using Hit@k
# MAGIC 3. Hyperparameter-tune `rank` and `maxIter` with Optuna — each trial trains on `train_hp` and evaluates on `val_hp`
# MAGIC 4. Train the final model on **all training data** (`train = train_hp + val_hp`) using the best hyperparameters
# MAGIC 5. Report the final Hit@k on the **held-out test set** (same as MBA) and log to MLflow

# COMMAND ----------

# DBTITLE 1,Define ALS training helper
from pyspark.ml.recommendation import ALS

def train_als(train_dataset, rank, maxIter):
    als = ALS(
        rank=rank,
        maxIter=maxIter,
        userCol='user_id',
        itemCol='item_id',
        ratingCol='proportion_of_orders',
        implicitPrefs=True
    )
    return als.fit(train_dataset)

# COMMAND ----------

# DBTITLE 1,Test building a small ALS model
model = train_als(als_train_dataset, rank=10, maxIter=5)
print('Small ALS model trained successfully (rank=10, maxIter=5)')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evaluate with Hit@k
# MAGIC Hit@k measures what proportion of the time the last item added to a test cart appears in the model’s top-k recommendations for that user. For example, Hit@5 = 0.25 means the held-out item is in the top 5 recommendations 25 % of the time.

# COMMAND ----------

# DBTITLE 1,Evaluate model on test set using Hit@k
from pyspark.sql.functions import array_contains, avg

k = 5

# Evaluate baseline model on the VALIDATION set (not test)
val_recommendations = model.recommendForUserSubset(
    val_linked.select('user_id').distinct(), k
)

val_recommendations = (
    val_recommendations
    .join(val_linked, 'user_id', 'inner')
    .select('EmailAddress', 'user_id', 'cart', 'added', 'added_item_id', 'recommendations')
    .withColumn('hit_at_k',
                array_contains(col('recommendations.item_id'),
                               col('added_item_id')).cast('int'))
)

avg_hit_k = val_recommendations.agg(avg('hit_at_k').alias('avg_hit_at_k'))
print(f'Baseline ALS (rank=10, maxIter=5) — Hit@{k} on validation set:')
display(avg_hit_k)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Hyperparameter tuning with Optuna + MLflow
# MAGIC ALS has two primary knobs:
# MAGIC * **`rank`** — dimensionality of the latent user/item factor matrices
# MAGIC * **`maxIter`** — number of alternating least-squares iterations until convergence
# MAGIC
# MAGIC We use Optuna to search the space and log every trial as a nested MLflow run.

# COMMAND ----------

# DBTITLE 1,Define Optuna objective function
import mlflow
import optuna
from functools import partial
from pyspark.sql.functions import array_contains, avg, col

def objective(trial, parent_run_id, k=5):
    with mlflow.start_run(parent_run_id=parent_run_id, nested=True):
        params = {
            'rank': trial.suggest_int('rank', 1, 100),
            'maxIter': trial.suggest_int('maxIter', 1, 10)
        }

        model = train_als(als_train_dataset,
                          rank=params['rank'],
                          maxIter=params['maxIter'])

        # Evaluate on VALIDATION set (not test)
        val_recs = model.recommendForUserSubset(
            val_linked.select('user_id').distinct(), k
        )
        val_recs = (
            val_recs.join(val_linked, 'user_id', 'inner')
            .select('user_id', 'added_item_id', 'recommendations')
            .withColumn('hit_at_k',
                        array_contains(col('recommendations.item_id'),
                                       col('added_item_id')).cast('int'))
        )
        avg_hit = val_recs.agg(avg('hit_at_k')).first()[0]

        mlflow.log_params(params)
        mlflow.log_metric('avg_hit_at_k', avg_hit)

    return avg_hit

# COMMAND ----------

# DBTITLE 1,Run HPO search and train final model
import os

os.makedirs('artifacts', exist_ok=True)
mlflow.set_experiment(experiment_name)
n_trials = 5  # reduced from 20 — each trial is expensive at 29 M user-item pairs

with mlflow.start_run(run_name='ALS_hp_tuning') as parent_run:
    # --- Optuna HPO (trains on train_hp, evaluates on val_hp) ---
    _objective = partial(objective,
                         parent_run_id=parent_run.info.run_id,
                         k=5)

    study = optuna.create_study(direction='maximize')
    study.optimize(_objective, n_trials=n_trials)

    best = study.best_trial
    print(f'Best trial Hit@5 (validation): {best.value:.4f}')
    print(f'Best params: {best.params}')

    # --- Train final model on ALL training data (train_hp + val_hp) ---
    with mlflow.start_run(run_name='final_als_model',
                          nested=True,
                          parent_run_id=parent_run.info.run_id):

        als_full_dataset, item_mapping_full, user_mapping_full = \
            preprocess_pipeline(train)

        final_model = train_als(als_full_dataset,
                                rank=best.params['rank'],
                                maxIter=best.params['maxIter'])

        # --- Final evaluation on HELD-OUT TEST SET (same as MBA) ---
        test_linked_final = prepare_eval_set(
            test, user_mapping_full, item_mapping_full, 'Final test')

        test_recs = final_model.recommendForUserSubset(
            test_linked_final.select('user_id').distinct(), 5)
        test_recs = (
            test_recs.join(test_linked_final, 'user_id', 'inner')
            .select('user_id', 'added_item_id', 'recommendations')
            .withColumn('hit_at_k',
                        array_contains(col('recommendations.item_id'),
                                       col('added_item_id')).cast('int'))
        )
        test_hit = test_recs.agg(avg('hit_at_k')).first()[0]
        mlflow.log_metric('test_hit_at_5', test_hit)
        print(f'Final model Hit@5 on TEST set: {test_hit:.4f}')

        # Log mappings as artifacts
        user_mapping_full.toPandas().to_parquet('artifacts/user_mapping_ALS.parquet')
        item_mapping_full.toPandas().to_parquet('artifacts/item_mapping_ALS.parquet')
        mlflow.log_artifact('artifacts/user_mapping_ALS.parquet', 'user_mapping')
        mlflow.log_artifact('artifacts/item_mapping_ALS.parquet', 'item_mapping')

        # Log the Spark ALS model
        mlflow.spark.log_model(final_model, artifact_path='model')
        mlflow.log_params(best.params)

        print(f'Final model logged (rank={best.params["rank"]}, '
              f'maxIter={best.params["maxIter"]})')

# COMMAND ----------

# DBTITLE 1,Section 3 — Deployment
# MAGIC %md
# MAGIC ## 3 — Deployment
# MAGIC Three steps to go from a trained ALS model to a production-ready serving endpoint:
# MAGIC
# MAGIC | Step | Cell | Description |
# MAGIC |---|---|---|
# MAGIC | **3a — Pre-compute** | Cell 18 | Use the final ALS model to generate top-20 ranked recommendations per user, reverse-map integer IDs back to product slugs, and save as `als_recommendations` Delta table |
# MAGIC | **3b — Lakebase publish** | Cells 19–20 | Enable CDF, add a primary-key constraint on `mpid`, and publish to a Lakebase online store for point-lookup serving |
# MAGIC | **3c — PyFunc model serving** | Cells 21–24 | Define `ALSRecommenderModel` (mirrors MBA’s `MBARecommenderModel`), log to MLflow / Unity Catalog with a config artifact, and test inference |
# MAGIC
# MAGIC ### How the frontend queries recommendations
# MAGIC ```
# MAGIC POST /serving-endpoints/als_recommender_model/invocations
# MAGIC {"dataframe_records": [{"mpid": 42}]}
# MAGIC
# MAGIC → {"mpid": 42,
# MAGIC    "recommendations": ["pepperoni-pizza", "crazy-bread", ...],
# MAGIC    "scores": [0.98, 0.91, ...]}
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Pre-compute top-20 recommendations per user
from pyspark.sql.functions import col, explode, collect_list, struct, row_number
from pyspark.sql.window import Window

# Generate top-20 recommendations for every user in the training set
raw_recs = final_model.recommendForAllUsers(20)

# Explode the nested recommendations array and reverse-map IDs → product slugs
recs_exploded = (
    raw_recs
    .select('user_id', explode('recommendations').alias('rec'))
    .select('user_id',
            col('rec.item_id').alias('item_id'),
            col('rec.rating').alias('score'))
)

recs_mapped = (
    recs_exploded
    .join(item_mapping_full, on='item_id')
    .join(user_mapping_full, on='user_id')
    .select(
        col('user').alias('EmailAddress'),
        'item', 'score'
    )
)

# Rank within each user and collect into ordered arrays
w = Window.partitionBy('EmailAddress').orderBy(col('score').desc())
recs_ranked = recs_mapped.withColumn('rank', row_number().over(w))

als_recommendations = (
    recs_ranked
    .groupBy('EmailAddress')
    .agg(
        collect_list(struct(col('rank'), col('item'), col('score')))
            .alias('recs_raw')
    )
    .selectExpr(
        'EmailAddress',
        "transform(array_sort(recs_raw, (l, r) -> l.rank - r.rank), x -> x.item) as recommendations",
        "transform(array_sort(recs_raw, (l, r) -> l.rank - r.rank), x -> ROUND(x.score, 6)) as scores"
    )
)

als_recommendations.write.format('delta').mode('overwrite') \
    .option('overwriteSchema', 'true') \
    .saveAsTable(recs_table)

print(f'Saved {als_recommendations.count():,} user recommendation sets to {recs_table}')
display(spark.table(recs_table).limit(5))

# COMMAND ----------

# DBTITLE 1,Publish recommendations to Lakebase online store
from databricks.feature_engineering import FeatureEngineeringClient

fe = FeatureEngineeringClient()

# Derive constraint name from the table config (schema-scoped, must be unique)
recs_table_short = recs_table.split('.')[-1]
pk_constraint = f'{recs_table_short}_pk'

# Enable CDF for online store sync
spark.sql(f"""ALTER TABLE {recs_table}
             SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""")

# Add primary key constraint on EmailAddress
spark.sql(f"""ALTER TABLE {recs_table} ALTER COLUMN EmailAddress SET NOT NULL""")
spark.sql(f"""ALTER TABLE {recs_table} DROP CONSTRAINT IF EXISTS {pk_constraint}""")
spark.sql(f"""ALTER TABLE {recs_table} ADD CONSTRAINT {pk_constraint} PRIMARY KEY (EmailAddress)""")

# Create or reuse an online store
store = None
try:
    store = fe.get_online_store(name=online_store_name)
    if store is not None:
        print(f'Online store already exists: {online_store_name} (state: {store.state})')
    else:
        raise ValueError(f'{online_store_name} not found')
except Exception:
    try:
        print(f'Creating online store: {online_store_name} ...')
        fe.create_online_store(name=online_store_name, capacity='CU_1')
        store = fe.get_online_store(name=online_store_name)
        if store is not None:
            print(f'Online store created (state: {store.state})')
        else:
            raise ValueError('Store not found after creation')
    except Exception as e:
        store = fe.get_online_store(name=online_store_fallback)
        if store is not None:
            print(f'Quota reached, reusing shared store: {online_store_fallback} (state: {store.state})')
        else:
            print(f'No online store available ({online_store_name} and {online_store_fallback} not found). '
                  f'Skipping online publish.')

# Publish to online store (only if available)
if store is not None:
    fe.publish_table(
        name=recs_table,
        online_store=store,
        online_table_name=online_table_name,
    )
    print(f'Published {recs_table} \u2192 {online_table_name}')
else:
    print(f'Recommendations available in Delta table: {recs_table}')

# COMMAND ----------

# DBTITLE 1,Test lookup from published recommendations
# Quick verification: read back from the Delta table and show a sample user
sample = spark.table(recs_table).limit(1).collect()[0]

print(f'Sample lookup — EmailAddress: {sample.EmailAddress}')
print(f'  Top-5 recommendations:')
for i, (product, score) in enumerate(zip(sample.recommendations[:5], sample.scores[:5]), 1):
    print(f'    {i}. {product}  (score: {score})')

print(f'\n  Total recommendations: {len(sample.recommendations)}')
print(f'\nOnline table: {online_table_name}')
print(f'Online store: {online_store_name}')

# COMMAND ----------

# DBTITLE 1,3c — Model Serving via Lakebase-backed PyFunc
# MAGIC %md
# MAGIC ---
# MAGIC ### 3c — Lakebase-backed PyFunc for Model Serving
# MAGIC The same pattern as the MBA notebook's `MBARecommenderModel`:
# MAGIC
# MAGIC 1. **`load_context`** — reads the pre-computed `als_recommendations` table from the Lakebase-backed online store at model startup
# MAGIC 2. **`predict`** — takes one or more `mpid` values and returns ranked recommendations via an O(1) lookup
# MAGIC 3. The frontend hits a **single Model Serving endpoint** — no need to know about Lakebase, ALS, or integer mappings
# MAGIC
# MAGIC | | MBA (`01`) | ALS (`02`) |
# MAGIC |---|---|---|
# MAGIC | **Input** | Cart (list of products) | User ID (`mpid`) |
# MAGIC | **`load_context`** | Fetches association rules from online table | Fetches pre-computed recommendations from online table |
# MAGIC | **`predict`** | Cross-join cart × rules → score → top-k | Lookup by `mpid` → return pre-computed top-k |
# MAGIC | **Latency** | Proportional to \|rules\| × \|cart\| | O(1) per user |
# MAGIC | **When to use** | Guest / anonymous users (no `mpid`) | Logged-in users (known `mpid`) |

# COMMAND ----------

# DBTITLE 1,Define ALSRecommenderModel PyFunc
import json
import pandas as pd
import mlflow.pyfunc
from mlflow.models.signature import ModelSignature
from mlflow.types import ColSpec, Schema, DataType
from mlflow.types.schema import Array


class ALSRecommenderModel(mlflow.pyfunc.PythonModel):
    """
    Lakebase-backed PyFunc for ALS collaborative filtering.

    Mirrors the LakebaseRecommenderModel pattern from the MBA notebook:
    - load_context: stores table reference (does NOT preload 9.9 M rows into pandas)
    - predict: queries only the needed rows via Spark SQL for O(1)-style lookup
    """

    def __init__(self, k=20):
        self.k = k

    def load_context(self, context):
        """Read config artifact — defer actual data loading to predict()."""
        with open(context.artifacts['config']) as f:
            cfg = json.load(f)
        self.recs_table = cfg['recommendations_table']

        # ---- Production (Model Serving): preload via databricks-sql-connector ----
        # from databricks import sql
        # with sql.connect(
        #     server_hostname=cfg['server_hostname'],
        #     http_path=cfg['http_path']
        # ) as conn:
        #     cursor = conn.cursor()
        #     cursor.execute(
        #         f"SELECT EmailAddress, recommendations, scores "
        #         f"FROM {cfg['recommendations_table']}"
        #     )
        #     self.recs_df = cursor.fetchall_arrow().to_pandas()
        #     self.recs_lookup = self.recs_df.set_index('EmailAddress')

    def predict(self, context, model_input):
        """Look up pre-computed recommendations for each EmailAddress."""
        from pyspark.sql import SparkSession
        spark = SparkSession.getActiveSession()

        emails = model_input['EmailAddress'].tolist()

        # Query only the requested rows — avoids loading 9.9 M rows into memory
        email_literals = ', '.join([f"'{e}'" for e in emails])
        recs_df = spark.sql(
            f"SELECT * FROM {self.recs_table} "
            f"WHERE EmailAddress IN ({email_literals})"
        ).toPandas()

        recs_lookup = recs_df.set_index('EmailAddress') if not recs_df.empty else pd.DataFrame()

        results = []
        for email in emails:
            if not recs_lookup.empty and email in recs_lookup.index:
                user_row = recs_lookup.loc[email]
                results.append({
                    'EmailAddress': email,
                    'recommendations': list(user_row['recommendations'][:self.k]),
                    'scores': list(user_row['scores'][:self.k])
                })
            else:
                results.append({
                    'EmailAddress': email,
                    'recommendations': [],
                    'scores': []
                })
        return pd.DataFrame(results)

# COMMAND ----------

# DBTITLE 1,Log ALS PyFunc to MLflow and Unity Catalog
import mlflow
import json
import os

os.makedirs('artifacts', exist_ok=True)
mlflow.set_experiment(experiment_name)

# Config artifact tells load_context which table to query
config = {'recommendations_table': recs_table}
with open('artifacts/als_lakebase_config.json', 'w') as f:
    json.dump(config, f)

# Define input / output schema
input_schema = Schema([
    ColSpec(DataType.string, 'EmailAddress')
])
output_schema = Schema([
    ColSpec(DataType.string, 'EmailAddress'),
    ColSpec(Array(DataType.string), 'recommendations'),
    ColSpec(Array(DataType.double), 'scores')
])
signature = ModelSignature(inputs=input_schema, outputs=output_schema)

# Log and register — skip input_example to avoid OOM from loading
# 9.9 M recommendation rows into memory during validation
with mlflow.start_run(run_name='als_lakebase_pyfunc'):
    mlflow.pyfunc.log_model(
        'model',
        python_model=ALSRecommenderModel(),
        registered_model_name=model_name,
        artifacts={'config': 'artifacts/als_lakebase_config.json'},
        signature=signature
    )
print(f'Model registered as {model_name}')

# COMMAND ----------

# DBTITLE 1,Test inference from ALS serving model
import mlflow
import pandas as pd

# Load model from latest run and generate recommendations
details = mlflow.last_active_run()
model_uri = f"runs:/{details.info.run_id}/model"
loaded_model = mlflow.pyfunc.load_model(model_uri)

# Test with sample users — grab a few from the recommendations table
sample_emails = (
    spark.table(recs_table)
    .select('EmailAddress')
    .limit(3)
    .toPandas()
)

input_data = pd.DataFrame({'EmailAddress': sample_emails['EmailAddress'].tolist() + ['unknown@example.com']})
print(f'Input emails: {input_data["EmailAddress"].tolist()}')
print(f'  (unknown@example.com is an unknown user — should return empty recommendations)\n')

results = loaded_model.predict(input_data)
for _, row in results.iterrows():
    email = row['EmailAddress']
    recs = row['recommendations']
    scores = row['scores']
    print(f'{email}:')
    if recs:
        for i, (product, score) in enumerate(zip(recs[:5], scores[:5]), 1):
            print(f'  {i}. {product}  (score: {score})')
        print(f'  ... ({len(recs)} total recommendations)')
    else:
        print('  (no recommendations — unknown user)')

# COMMAND ----------

# DBTITLE 1,Head-to-head: ALS vs MBA on shared test orders
from pyspark.sql.functions import (
    col, expr, size, split, trim, transform, array_distinct,
    array_contains, avg, collect_list, struct, row_number,
    broadcast, lit, when
)
from pyspark.sql.window import Window

# ── 1. Build shared evaluation set (warm, multi-item test orders) ──
test = spark.read.table(test_table)
als_recs = spark.read.table(recs_table)
rules_table = f'{catalog}.{schema}.association_rules'

test_parsed = (
    test
    .withColumn('items', array_distinct(
        transform(split(col('ItemNames'), ','), lambda x: trim(x))))
    .filter(size(col('items')) > 1)
    .withColumn('cart', expr('slice(items, 1, size(items) - 1)'))
    .withColumn('added', expr('items[size(items) - 1]'))
)

# Restrict to users with ALS recommendations (warm users)
shared_eval = test_parsed.join(
    als_recs.select('EmailAddress'), on='EmailAddress', how='inner'
)

total_shared = shared_eval.count()
SAMPLE_SIZE = 50_000
if total_shared > SAMPLE_SIZE:
    shared_eval = (shared_eval
                   .sample(fraction=SAMPLE_SIZE / total_shared, seed=42)
                   .limit(SAMPLE_SIZE))

shared_eval = shared_eval.repartition(200).cache()
n_eval = shared_eval.count()
print(f'Shared evaluation: {n_eval:,} orders '
      f'(sampled from {total_shared:,} warm multi-item test orders)\n')

# ── 2. ALS Hit@5 (pre-computed lookup) ────────────────────────
als_eval = (
    shared_eval.select('CVOrderID', 'EmailAddress', 'added')
    .join(als_recs, on='EmailAddress', how='inner')
    .withColumn('als_top5', expr('slice(recommendations, 1, 5)'))
    .withColumn('hit_als', array_contains(col('als_top5'), col('added')).cast('int'))
    .select('CVOrderID', 'hit_als')
)

# ── 3. MBA Hit@5 (rule matching) ─────────────────────────────
rules = (
    spark.read.table(rules_table)
    .selectExpr('antecedent', 'consequent[0] as consequent', 'confidence')
)

# Match rules whose antecedent overlaps the cart
mba_matched = (
    shared_eval.select('CVOrderID', 'cart', 'added')
    .join(
        broadcast(rules),
        expr('size(array_intersect(cart, antecedent)) > 0'),
        'left'
    )
    .withColumn('match_score', when(
        col('consequent').isNotNull(),
        expr('POWER(size(array_intersect(cart, antecedent)), 2) '
             '/ (size(antecedent) * size(cart))')
    ).otherwise(lit(0.0)))
    .withColumn('rule_score', col('match_score') * col('confidence'))
)

# Top-5 unique consequents per order, ranked by rule_score
w = Window.partitionBy('CVOrderID', 'consequent').orderBy(col('rule_score').desc())
mba_deduped = mba_matched.filter(col('consequent').isNotNull()) \
    .withColumn('rn', row_number().over(w)).filter(col('rn') == 1)

w2 = Window.partitionBy('CVOrderID').orderBy(col('rule_score').desc())
mba_ranked = mba_deduped.withColumn('rank', row_number().over(w2)).filter(col('rank') <= 5)

mba_top5 = (
    mba_ranked
    .groupBy('CVOrderID')
    .agg(collect_list('consequent').alias('mba_top5'))
)

mba_eval = (
    shared_eval.select('CVOrderID', 'added')
    .join(mba_top5, on='CVOrderID', how='left')
    .withColumn('hit_mba',
        when(col('mba_top5').isNull(), lit(0))
        .otherwise(array_contains(col('mba_top5'), col('added')).cast('int')))
    .select('CVOrderID', 'hit_mba')
)

# ── 4. Popularity baseline Hit@5 ─────────────────────────────
train = spark.read.table(train_table)
top5_items = (
    train
    .withColumn('item', expr("explode(array_distinct(transform(split(ItemNames, ','), x -> trim(x))))"))
    .groupBy('item').count()
    .orderBy(col('count').desc())
    .limit(5)
    .select('item')
    .rdd.flatMap(lambda x: x).collect()
)

baseline_eval = (
    shared_eval.select('CVOrderID', 'added')
    .withColumn('hit_baseline',
        col('added').isin(top5_items).cast('int'))
    .select('CVOrderID', 'hit_baseline')
)

# ── 5. Join and compare ──────────────────────────────────────
comparison = (
    als_eval
    .join(mba_eval, on='CVOrderID', how='left')
    .join(baseline_eval, on='CVOrderID', how='left')
    .fillna(0, subset=['hit_mba', 'hit_baseline'])
)

als_h5  = comparison.agg(avg('hit_als')).first()[0]
mba_h5  = comparison.agg(avg('hit_mba')).first()[0]
base_h5 = comparison.agg(avg('hit_baseline')).first()[0]

print(f'{"=" * 55}')
print(f'  Hit@5 — Head-to-Head on {n_eval:,} Shared Test Orders')
print(f'{"=" * 55}')
print(f'  {"ALS (rank=92, maxIter=7)":<35} {als_h5:.4f}')
print(f'  {"MBA (FPGrowth, 65 K rules)":<35} {mba_h5:.4f}')
print(f'  {"Popularity baseline (top-5)":<35} {base_h5:.4f}')
print(f'{"=" * 55}')
print(f'  ALS vs MBA:      +{als_h5 - mba_h5:.4f}  '
      f'({100 * (als_h5 - mba_h5) / mba_h5:.1f} % relative lift)')
print(f'  ALS vs Baseline:  +{als_h5 - base_h5:.4f}  '
      f'({100 * (als_h5 - base_h5) / base_h5:.1f} % relative lift)')

shared_eval.unpersist()
