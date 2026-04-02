# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Market Basket Analysis (PySpark FPGrowth)
# MAGIC Trains distributed FPGrowth model, evaluates with Hit@k, logs to MLflow,
# MAGIC and generates pre-computed per-product recommendation lookup table.
# MAGIC
# MAGIC **Compute**: Requires single-user ML cluster (PySpark ML).

# COMMAND ----------

# MAGIC %run ./config_loader

# COMMAND ----------

# DBTITLE 1,Load config
import time

cfg = load_config()
catalog = cfg["catalog"]
schema = cfg["schema"]
min_transactions = cfg.get("mba_min_transactions", 1000)
min_confidence = cfg.get("mba_min_confidence", 0.0)
k = cfg.get("recommendation_k", 5)
experiment_root = cfg.get("mlflow_experiment_root", "/Shared/recommender-accelerator")

print(f"Config: catalog={catalog}.{schema}, min_transactions={min_transactions}, k={k}")

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

print(f"Training FPGrowth on {train.count():,} orders...")
t0 = time.time()

fpGrowth = FPGrowth(
    itemsCol="order_product_list",
    minSupport=min_transactions / num_transactions,
    minConfidence=min_confidence,
    numPartitions=sc.defaultParallelism * 100,
)

model = fpGrowth.fit(train)
rules_count = model.associationRules.count()
print(f"Generated {rules_count:,} association rules in {time.time()-t0:.1f}s")
model.associationRules.sort("antecedent", "consequent").display()

# COMMAND ----------

# DBTITLE 1,Save rules and train/test
print("Saving association rules and datasets...")
t0 = time.time()

model.associationRules.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(f"{catalog}.{schema}.mba_rules")

train.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{catalog}.{schema}.mba_train_dataset"
)
test.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{catalog}.{schema}.mba_test_dataset"
)
print(f"  Saved in {time.time()-t0:.1f}s")

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
    Fully distributed via Spark broadcast join + window functions.
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
        .withColumn("rank", row_number().over(
            Window.partitionBy("order_id", "consequent").orderBy(col("rule_score").desc())
        ))
        .filter(col("rank") == 1)
        .withColumn("rank", row_number().over(
            Window.partitionBy("order_id").orderBy(col("rule_score").desc())
        ))
        .filter(col("rank") <= k)
        .groupBy(return_columns)
        .agg(collect_list(struct("consequent", "rule_score")).alias("recommendations_with_scores"))
    )

    return top_k

# COMMAND ----------

# DBTITLE 1,Evaluate Hit@k on full test set
from pyspark.sql.functions import size, expr, array_contains

rules = (
    spark.read.table(f"{catalog}.{schema}.mba_rules")
    .selectExpr("antecedent", "consequent[0] as consequent", "lift", "confidence")
)

test_data = spark.read.table(f"{catalog}.{schema}.mba_test_dataset").filter(
    size(col("order_product_list")) > 1
)
test_transformed = (
    test_data
    .withColumn("cart", expr("slice(order_product_list, 1, size(order_product_list) - 1)"))
    .withColumn("added", expr("order_product_list[size(order_product_list) - 1]"))
)

print(f"Evaluating Hit@{k} on {test_transformed.count():,} test orders...")
t0 = time.time()

top_k_recs = generate_recommendations(rules, test_transformed, k=k, metric="confidence", testing=True)
top_k_recs = top_k_recs.withColumn(
    "hit_at_k",
    expr("cast(array_contains(transform(recommendations_with_scores, x -> x.consequent), added) as int)"),
)

hit_rate = top_k_recs.agg({"hit_at_k": "avg"}).collect()[0][0]
print(f"Hit@{k}: {hit_rate:.4f} in {time.time()-t0:.1f}s")

# COMMAND ----------

# DBTITLE 1,Log model to MLflow
import mlflow
import mlflow.pyfunc
import pandas as pd
from mlflow.models.signature import ModelSignature
from mlflow.types import ColSpec, Schema, DataType
from mlflow.types.schema import Array

experiment_name = f"/Users/{spark.sql('SELECT current_user()').collect()[0][0]}/recommender-accelerator-mba"
mlflow.set_experiment(experiment_name)


class MBARecommenderModel(mlflow.pyfunc.PythonModel):
    def __init__(self, k=5):
        self.k = k

    def load_context(self, context):
        self.rules_df = pd.read_parquet(context.artifacts["rules_file"])

    def _generate_recs(self, cart, k):
        cart_set = set(cart)
        scored = []
        for _, rule in self.rules_df.iterrows():
            antecedent = set(rule["antecedent"])
            consequent = rule["consequent"]
            intersection = antecedent & cart_set
            if not intersection:
                continue
            if consequent in cart_set:
                continue
            match_score = (len(intersection) ** 2) / (len(antecedent) * len(cart_set))
            rule_score = rule["confidence"] * match_score
            scored.append({"consequent": consequent, "rule_score": rule_score})
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

# DBTITLE 1,Pre-compute MBA lookup table
import json
from pyspark.sql import Row
from pyspark.sql.functions import to_json

product_catalog = spark.read.table(f"{catalog}.{schema}.product_catalog")
all_slugs = [row.product_slug for row in product_catalog.select("product_slug").collect()]

print(f"Generating top-{k} MBA recommendations for {len(all_slugs)} products...")
t0 = time.time()

cart_rows = [Row(order_id=slug, cart=[slug]) for slug in all_slugs]
cart_sdf = spark.createDataFrame(cart_rows)

mba_recs = generate_recommendations(rules, cart_sdf, k=k, metric="confidence", testing=False)

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
print(f"Wrote {recs_count} product recs in {time.time()-t0:.1f}s")
mba_lookup.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Manual step**: Configure Lakebase sync for `mba_recommendations`.
