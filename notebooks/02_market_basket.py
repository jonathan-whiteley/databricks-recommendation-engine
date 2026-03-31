# Databricks notebook source
# MAGIC %pip install mlxtend

# COMMAND ----------

# MAGIC %md
# MAGIC # 02 - Market Basket Analysis
# MAGIC Trains FPGrowth model (via mlxtend, single-node), evaluates with Hit@k, logs to
# MAGIC MLflow, and generates pre-computed per-product recommendation lookup table for
# MAGIC Lakebase serving.
# MAGIC
# MAGIC **Compute**: Serverless compatible (single-node Python via mlxtend).

# COMMAND ----------

# MAGIC %run ./config_loader

# COMMAND ----------

# DBTITLE 1,Load config
cfg = load_config()
catalog = cfg["catalog"]
schema = cfg["schema"]
min_transactions = cfg.get("mba_min_transactions", 1000)
min_confidence = cfg.get("mba_min_confidence", 0.0)
k = cfg.get("recommendation_k", 5)
experiment_root = cfg.get("mlflow_experiment_root", "/Shared/recommender-accelerator")

# COMMAND ----------

# DBTITLE 1,Load cleaned orders
sdf_cleaned = spark.read.table(f"{catalog}.{schema}.cleaned_orders")
num_transactions = sdf_cleaned.count()
print(f"Loaded {num_transactions:,} cleaned orders")

# COMMAND ----------

# DBTITLE 1,EDA - Product support
from pyspark.sql.functions import explode, col, count
import matplotlib.pyplot as plt

sdf_support = (
    sdf_cleaned.withColumn("product", explode(col("order_product_list")))
    .groupBy("product")
    .agg(count("*").alias("count"), (count("*") / num_transactions).alias("support"))
    .sort(col("count").desc())
)

df_support = sdf_support.toPandas()
top_items = df_support.head(25)
plt.figure(figsize=(10, 5))
plt.bar(top_items["product"], top_items["support"] * 100)
plt.xlabel("Product Slug")
plt.ylabel("Support (%)")
plt.title("Top 25 Products by Support")
plt.xticks(rotation=90)
plt.tight_layout()
plt.show()

# COMMAND ----------

# DBTITLE 1,Train FPGrowth (mlxtend)
import pandas as pd
from mlxtend.preprocessing import TransactionEncoder
from mlxtend.frequent_patterns import fpgrowth as mlx_fpgrowth, association_rules

# Split into train/test
train_sdf, test_sdf = sdf_cleaned.randomSplit([0.8, 0.2], seed=42)

# Convert train set to pandas list of transactions
train_pd = train_sdf.select("order_product_list").toPandas()
transactions = train_pd["order_product_list"].tolist()

# One-hot encode transactions
te = TransactionEncoder()
te_array = te.fit(transactions).transform(transactions)
df_encoded = pd.DataFrame(te_array, columns=te.columns_)

# Run fpgrowth
min_support = min_transactions / num_transactions
frequent_itemsets = mlx_fpgrowth(df_encoded, min_support=min_support, use_colnames=True)
print(f"Found {len(frequent_itemsets):,} frequent itemsets")

# Generate association rules
rules_df = association_rules(frequent_itemsets, metric="confidence", min_threshold=min_confidence)
rules_count = len(rules_df)
print(f"Generated {rules_count:,} association rules")
rules_df.sort_values(["confidence", "lift"], ascending=False).head(20)

# COMMAND ----------

# DBTITLE 1,Save association rules
# Convert frozensets to lists for Delta storage
rules_for_delta = rules_df.copy()
rules_for_delta["antecedents"] = rules_for_delta["antecedents"].apply(list)
rules_for_delta["consequents"] = rules_for_delta["consequents"].apply(list)

spark.createDataFrame(rules_for_delta).write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(f"{catalog}.{schema}.mba_rules")

# Also save train/test for reproducibility
train_sdf.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{catalog}.{schema}.mba_train_dataset"
)
test_sdf.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{catalog}.{schema}.mba_test_dataset"
)

# COMMAND ----------

# DBTITLE 1,Recommendation scoring function
def generate_recommendations(rules, cart, k=5):
    """
    Generate top-k product recommendations based on association rules.

    Parameters:
        rules: pandas DataFrame with columns antecedents, consequents, confidence, lift
               (antecedents/consequents are frozensets)
        cart: list of product slugs currently in cart
        k: number of top recommendations

    Returns:
        list of dicts with keys: consequent, rule_score
    """
    cart_set = set(cart)
    scored = []

    for _, rule in rules.iterrows():
        antecedent = rule["antecedents"]
        consequent = rule["consequents"]

        # Check if antecedent overlaps with cart
        intersection = antecedent & cart_set
        if not intersection:
            continue

        # Check each consequent item
        for item in consequent:
            if item in cart_set:
                continue

            match_score = (len(intersection) ** 2) / (len(antecedent) * len(cart_set))
            rule_score = rule["confidence"] * match_score

            scored.append({
                "consequent": item,
                "rule_score": rule_score,
            })

    if not scored:
        return []

    # Deduplicate: keep best score per consequent
    scores_by_item = {}
    for s in scored:
        item = s["consequent"]
        if item not in scores_by_item or s["rule_score"] > scores_by_item[item]["rule_score"]:
            scores_by_item[item] = s

    # Sort and return top-k
    top_k = sorted(scores_by_item.values(), key=lambda x: x["rule_score"], reverse=True)[:k]
    return top_k

# COMMAND ----------

# DBTITLE 1,Evaluate Hit@k on test set
from pyspark.sql.functions import size

test_data = spark.read.table(f"{catalog}.{schema}.mba_test_dataset").filter(
    size(col("order_product_list")) > 1
)
test_pd = test_data.select("order_id", "order_product_list").toPandas()

# For each test order: remove last item as "added", use rest as cart
hits = 0
total = 0

for _, row in test_pd.iterrows():
    items = row["order_product_list"]
    cart = items[:-1]
    added = items[-1]

    recs = generate_recommendations(rules_df, cart, k=k)
    rec_items = [r["consequent"] for r in recs]

    if added in rec_items:
        hits += 1
    total += 1

hit_rate = hits / total if total > 0 else 0.0
print(f"Hit@{k}: {hit_rate:.4f} ({hits}/{total})")

# COMMAND ----------

# DBTITLE 1,Log model to MLflow
import mlflow
import mlflow.pyfunc
from mlflow.models.signature import ModelSignature
from mlflow.types import ColSpec, Schema, DataType
from mlflow.types.schema import Array

experiment_name = f"{experiment_root}/market_basket_analysis"
try:
    mlflow.set_experiment(experiment_name)
except Exception:
    mlflow.create_experiment(experiment_name)
    mlflow.set_experiment(experiment_name)


class MBARecommenderModel(mlflow.pyfunc.PythonModel):
    def __init__(self, k=5):
        self.k = k

    def load_context(self, context):
        self.rules_df = pd.read_parquet(context.artifacts["rules_file"])
        # Convert list columns back to frozensets
        self.rules_df["antecedents"] = self.rules_df["antecedents"].apply(
            lambda x: frozenset(x) if isinstance(x, list) else x
        )
        self.rules_df["consequents"] = self.rules_df["consequents"].apply(
            lambda x: frozenset(x) if isinstance(x, list) else x
        )

    def _generate_recs(self, cart, k):
        cart_set = set(cart)
        scored = []
        for _, rule in self.rules_df.iterrows():
            antecedent = rule["antecedents"]
            consequent = rule["consequents"]
            intersection = antecedent & cart_set
            if not intersection:
                continue
            for item in consequent:
                if item in cart_set:
                    continue
                match_score = (len(intersection) ** 2) / (len(antecedent) * len(cart_set))
                rule_score = rule["confidence"] * match_score
                scored.append({"consequent": item, "rule_score": rule_score})
        if not scored:
            return []
        scores_by_item = {}
        for s in scored:
            item = s["consequent"]
            if item not in scores_by_item or s["rule_score"] > scores_by_item[item]["rule_score"]:
                scores_by_item[item] = s
        return sorted(scores_by_item.values(), key=lambda x: x["rule_score"], reverse=True)[:k]

    def predict(self, context, model_input):
        results = []
        for _, row in model_input.iterrows():
            recs = self._generate_recs(row["cart"], self.k)
            results.append({
                "order_id": row["order_id"],
                "cart": row["cart"],
                "recommendations": [r["consequent"] for r in recs],
                "rule_score": [r["rule_score"] for r in recs],
            })
        return pd.DataFrame(results)


# Save rules as parquet artifact (with frozensets converted to lists)
rules_for_artifact = rules_df.copy()
rules_for_artifact["antecedents"] = rules_for_artifact["antecedents"].apply(list)
rules_for_artifact["consequents"] = rules_for_artifact["consequents"].apply(list)
rules_for_artifact.to_parquet("/tmp/mba_rules.parquet")

input_schema = Schema([ColSpec(DataType.string, "order_id"), ColSpec(Array(DataType.string), "cart")])
output_schema = Schema([
    ColSpec(DataType.string, "order_id"),
    ColSpec(Array(DataType.string), "cart"),
    ColSpec(Array(DataType.string), "recommendations"),
    ColSpec(Array(DataType.string), "rule_score"),
])
signature = ModelSignature(inputs=input_schema, outputs=output_schema)

model_input = pd.DataFrame({"order_id": ["12345"], "cart": [["classic-burger", "classic-fries"]]})

with mlflow.start_run(run_name="mba_fpgrowth"):
    mlflow.log_metric("hit_at_k", hit_rate)
    mlflow.log_param("min_transactions", min_transactions)
    mlflow.log_param("min_confidence", min_confidence)
    mlflow.log_param("k", k)
    mlflow.log_param("num_rules", rules_count)
    mlflow.pyfunc.log_model(
        "model",
        python_model=MBARecommenderModel(k=k),
        registered_model_name=f"{catalog}.{schema}.mba_recommender",
        artifacts={"rules_file": "/tmp/mba_rules.parquet"},
        input_example=model_input,
        signature=signature,
    )

print("Model logged to MLflow")

# COMMAND ----------

# DBTITLE 1,Generate pre-computed MBA recommendations lookup table
import json
from pyspark.sql import Row

# For each unique product, generate top-k recommendations
product_catalog = spark.read.table(f"{catalog}.{schema}.product_catalog")
all_slugs = [row.product_slug for row in product_catalog.select("product_slug").collect()]

# Generate recommendations for every product using single-item carts
lookup_rows = []
for slug in all_slugs:
    recs = generate_recommendations(rules_df, [slug], k=k)
    if recs:
        lookup_rows.append(Row(
            product_slug=slug,
            recommendations=json.dumps(recs),
        ))

# Write to Delta
if lookup_rows:
    mba_lookup = spark.createDataFrame(lookup_rows)
    mba_lookup.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        f"{catalog}.{schema}.mba_recommendations"
    )
    recs_count = mba_lookup.count()
    print(f"Wrote {recs_count} product recommendation entries to {catalog}.{schema}.mba_recommendations")
    mba_lookup.display()
else:
    print("No recommendations generated - check rules and support thresholds")

# COMMAND ----------

# DBTITLE 1,Sync mba_recommendations to Lakebase
# MAGIC %md
# MAGIC **Manual step**: Configure Lakebase sync for the `mba_recommendations` table.
# MAGIC Use the Databricks UI or CLI to create a Lakebase managed sync:
# MAGIC ```sql
# MAGIC CREATE OR REPLACE ONLINE TABLE {catalog}.{schema}.mba_recommendations_online
# MAGIC AS SELECT * FROM {catalog}.{schema}.mba_recommendations;
# MAGIC ```
# MAGIC Or configure via `databricks.yml` Lakebase resources (see Task 8).
