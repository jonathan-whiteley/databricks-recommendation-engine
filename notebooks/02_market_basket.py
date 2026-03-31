# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Market Basket Analysis
# MAGIC Trains FPGrowth model, evaluates with Hit@k, logs to MLflow, and generates
# MAGIC pre-computed per-product recommendation lookup table for Lakebase serving.
# MAGIC
# MAGIC **Compute**: Requires single-user ML cluster (PySpark ML).

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

# DBTITLE 1,Train FPGrowth
from pyspark.ml.fpm import FPGrowth

train, test = sdf_cleaned.randomSplit([0.8, 0.2], seed=42)

fpGrowth = FPGrowth(
    itemsCol="order_product_list",
    minSupport=min_transactions / num_transactions,
    minConfidence=min_confidence,
    numPartitions=200 * 100,
)

model = fpGrowth.fit(train)
rules_count = model.associationRules.count()
print(f"Generated {rules_count:,} association rules")
model.associationRules.sort("antecedent", "consequent").display()

# COMMAND ----------

# DBTITLE 1,Save association rules
model.associationRules.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{catalog}.{schema}.mba_rules"
)

# Also save train/test for reproducibility
train.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{catalog}.{schema}.mba_train_dataset"
)
test.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{catalog}.{schema}.mba_test_dataset"
)

# COMMAND ----------

# DBTITLE 1,Recommendation scoring function
from pyspark.sql.functions import (
    broadcast, array_intersect, array, expr, size, power,
    row_number, collect_list, struct, col
)
from pyspark.sql.window import Window


def generate_recommendations(rules_sdf, cart_sdf, k=5, metric="confidence", testing=False):
    """
    Generate top-k product recommendations based on association rules.

    Parameters:
        rules_sdf: DataFrame with columns antecedent, consequent, lift, confidence
        cart_sdf: DataFrame with columns order_id, cart, and optionally 'added' if testing
        k: number of top recommendations
        metric: scoring metric (confidence or lift)
        testing: if True, include 'added' column for evaluation

    Returns:
        DataFrame with order_id, cart, recommendations_with_scores
    """
    baskets_and_rules = cart_sdf.join(
        broadcast(rules_sdf.selectExpr("antecedent", "consequent", "lift", "confidence")),
        on=array_intersect(col("cart"), col("antecedent")) != array(),
    )

    score = (
        baskets_and_rules
        .filter(expr("not array_contains(cart, consequent)"))
        .withColumn("intr", expr("size(array_intersect(cart, antecedent))"))
        .withColumn("match_score", expr("power(intr, 2) / (size(antecedent) * size(cart))"))
        .withColumn("rule_score", expr(f"{metric} * match_score"))
    )

    return_columns = ["order_id", "cart"]
    if testing:
        return_columns += ["added"]

    top_k = (
        score
        .withColumn("rank", row_number().over(Window.partitionBy("order_id", "consequent").orderBy(col("rule_score").desc())))
        .filter(col("rank") == 1)
        .withColumn("rank", row_number().over(Window.partitionBy("order_id").orderBy(col("rule_score").desc())))
        .filter(col("rank") <= k)
        .groupBy(return_columns)
        .agg(collect_list(struct("consequent", "rule_score")).alias("recommendations_with_scores"))
    )

    return top_k

# COMMAND ----------

# DBTITLE 1,Evaluate Hit@k on test set
from pyspark.sql.functions import expr, array_contains

rules = (
    spark.read.table(f"{catalog}.{schema}.mba_rules")
    .selectExpr("antecedent", "consequent[0] as consequent", "lift", "confidence")
)

test_data = spark.read.table(f"{catalog}.{schema}.mba_test_dataset").filter(size(col("order_product_list")) > 1)
test_transformed = (
    test_data
    .withColumn("cart", expr("slice(order_product_list, 1, size(order_product_list) - 1)"))
    .withColumn("added", expr("order_product_list[size(order_product_list) - 1]"))
    .withColumn("order_id", col("order_id"))
)

top_k_recs = generate_recommendations(rules, test_transformed, k=k, metric="confidence", testing=True)

top_k_recs = top_k_recs.withColumn(
    "hit_at_k",
    expr("cast(array_contains(transform(recommendations_with_scores, x -> x.consequent), added) as int)"),
)

hit_rate = top_k_recs.agg({"hit_at_k": "avg"}).collect()[0][0]
print(f"Hit@{k}: {hit_rate:.4f}")

# COMMAND ----------

# DBTITLE 1,Log model to MLflow
import mlflow
import mlflow.pyfunc
import pandas as pd
from mlflow.models.signature import ModelSignature
from mlflow.types import ColSpec, Schema, DataType
from mlflow.types.schema import Array

mlflow.set_experiment(f"{experiment_root}/market_basket_analysis")


class MBARecommenderModel(mlflow.pyfunc.PythonModel):
    def __init__(self, k=5, metric="confidence"):
        self.k = k
        self.metric = metric

    def load_context(self, context):
        self.rules_df = pd.read_parquet(context.artifacts["rules_file"])

    def _calculate_match_score(self, row):
        intersection = set(row["cart"]).intersection(row["antecedent"])
        return (len(intersection) ** 2) / (len(row["antecedent"]) * len(row["cart"]))

    def predict(self, context, model_input):
        merged = model_input.merge(self.rules_df, how="cross")
        if merged.empty:
            return pd.DataFrame(columns=["order_id", "cart", "recommendations", "rule_score"])
        merged = merged[merged.apply(lambda x: bool(set(x["cart"]) & set(x["antecedent"])), axis=1)]
        merged["match_score"] = merged.apply(self._calculate_match_score, axis=1)
        merged["rule_score"] = merged[self.metric] * merged["match_score"]
        merged = merged[~merged.apply(lambda x: x["consequent"] in x["cart"], axis=1)]
        grouped = merged.loc[merged.groupby(["order_id", "consequent"])["rule_score"].idxmax()]
        top_k = grouped.groupby("order_id").apply(lambda x: x.nlargest(self.k, "rule_score")).reset_index(drop=True)
        result = top_k.groupby("order_id").agg({"cart": "first", "consequent": list, "rule_score": list}).reset_index()
        result.rename(columns={"consequent": "recommendations"}, inplace=True)
        return result


# Save rules as parquet artifact
rules_pd = rules.toPandas()
rules_pd.to_parquet("/tmp/mba_rules.parquet")

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

# Build single-item carts for each product
cart_rows = [Row(order_id=slug, cart=[slug]) for slug in all_slugs]
cart_sdf = spark.createDataFrame(cart_rows)

# Generate recommendations for every product
mba_recs = generate_recommendations(rules, cart_sdf, k=k, metric="confidence", testing=False)

# Reshape: order_id is product_slug, extract recs as JSON string
from pyspark.sql.functions import to_json

mba_lookup = (
    mba_recs
    .withColumnRenamed("order_id", "product_slug")
    .withColumn("recommendations", to_json(col("recommendations_with_scores")))
    .select("product_slug", "recommendations")
)

mba_lookup.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{catalog}.{schema}.mba_recommendations"
)

recs_count = mba_lookup.count()
print(f"Wrote {recs_count} product recommendation entries to {catalog}.{schema}.mba_recommendations")
mba_lookup.display()

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
