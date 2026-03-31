# Databricks notebook source
# MAGIC %pip install optuna

# COMMAND ----------

# MAGIC %md
# MAGIC # 03 - Collaborative Filtering (ALS)
# MAGIC Trains ALS model with Optuna hyperparameter optimization, evaluates with Hit@k,
# MAGIC logs to MLflow, and generates pre-computed per-user recommendation lookup table.
# MAGIC
# MAGIC **Compute**: Requires single-user ML cluster (PySpark ML).

# COMMAND ----------

# DBTITLE 1,Load config
# MAGIC %run ./config_loader

cfg = load_config()
catalog = cfg["catalog"]
schema = cfg["schema"]
k = cfg.get("recommendation_k", 5)
n_trials = cfg.get("als_hpo_trials", 20)
experiment_root = cfg.get("mlflow_experiment_root", "/Shared/recommender-accelerator")

# COMMAND ----------

# DBTITLE 1,Load data
from pyspark.sql.functions import col

sdf_cleaned = spark.read.table(f"{catalog}.{schema}.cleaned_orders")
sdf_ratings = spark.read.table(f"{catalog}.{schema}.user_item_ratings")

train, test = sdf_cleaned.randomSplit([0.8, 0.2], seed=42)
print(f"Train: {train.count():,} | Test: {test.count():,}")

# COMMAND ----------

# DBTITLE 1,Preprocess for ALS
from pyspark.sql.functions import explode, count, sum as spark_sum, row_number
from pyspark.sql.window import Window


def preprocess_pipeline(train_dataset):
    """
    Preprocess training dataset for ALS.
    Creates integer user/item mappings and computes implicit ratings
    (proportion of user's orders containing each item).

    Returns: (als_train_dataset, item_mapping, user_mapping)
    """
    sdf_exploded = train_dataset.withColumn("product", explode("order_product_list"))
    als_input = sdf_exploded.select(col("user_id").alias("user"), col("product").alias("item"))

    item_mapping = als_input.select("item").distinct().withColumn("item_id", row_number().over(Window.orderBy("item")))
    user_mapping = als_input.select("user").distinct().withColumn("user_id_int", row_number().over(Window.orderBy("user")))

    als_input_mapped = (
        als_input
        .join(item_mapping, on="item")
        .join(user_mapping, on="user")
        .select("user_id_int", "item_id", "item")
    )

    als_grouped = als_input_mapped.groupBy("user_id_int", "item_id").agg(count("*").alias("item_count"))
    user_totals = als_grouped.groupBy("user_id_int").agg(spark_sum("item_count").alias("total_orders"))
    als_with_totals = als_grouped.join(user_totals, on="user_id_int")
    als_train = als_with_totals.withColumn("proportion_of_orders", col("item_count") / col("total_orders"))

    return als_train, item_mapping, user_mapping


als_train, item_mapping, user_mapping = preprocess_pipeline(train)
print(f"ALS train: {als_train.count():,} user-item pairs")
print(f"Users: {user_mapping.count():,} | Items: {item_mapping.count():,}")

# COMMAND ----------

# DBTITLE 1,Prepare test set
from pyspark.sql.functions import expr, size, array_contains, avg

test_filtered = test.filter(size(col("order_product_list")) > 1)
test_transformed = (
    test_filtered
    .withColumn("cart", expr("slice(order_product_list, 1, size(order_product_list) - 1)"))
    .withColumn("added", expr("order_product_list[size(order_product_list) - 1]"))
)

test_linked = test_transformed.join(user_mapping, test_transformed.user_id == user_mapping.user, "inner")
test_linked = (
    test_linked
    .join(item_mapping, test_linked.added == item_mapping.item, "inner")
    .withColumnRenamed("item_id", "added_item_id")
)
print(f"Test set: {test_linked.count():,} evaluable rows")

# COMMAND ----------

# DBTITLE 1,Define ALS training function
from pyspark.ml.recommendation import ALS


def train_als(train_dataset, rank, maxIter):
    als = ALS(
        rank=rank,
        maxIter=maxIter,
        userCol="user_id_int",
        itemCol="item_id",
        ratingCol="proportion_of_orders",
        implicitPrefs=True,
    )
    return als.fit(train_dataset)

# COMMAND ----------

# DBTITLE 1,Optuna HPO
import optuna
import mlflow
from functools import partial

mlflow.set_experiment(f"{experiment_root}/als_collaborative_filter")


def objective(trial, parent_run_id, k=5):
    with mlflow.start_run(parent_run_id=parent_run_id, nested=True):
        params = {
            "rank": trial.suggest_int("rank", 1, 100),
            "maxIter": trial.suggest_int("maxIter", 1, 10),
        }
        model = train_als(als_train, rank=params["rank"], maxIter=params["maxIter"])

        test_recs = model.recommendForUserSubset(test_linked.select("user_id_int").distinct(), k)
        test_recs = (
            test_recs.join(test_linked, "user_id_int", "inner")
            .select("user_id_int", "cart", "added", "added_item_id", "recommendations")
            .withColumn("hit_at_k", array_contains(col("recommendations.item_id"), col("added_item_id")).cast("int"))
        )
        avg_hit = test_recs.agg(avg(col("hit_at_k"))).first()[0]

        mlflow.log_params(params)
        mlflow.log_metric("hit_at_k", avg_hit)
        return avg_hit


with mlflow.start_run(run_name="als_hpo") as parent_run:
    obj = partial(objective, parent_run_id=parent_run.info.run_id, k=k)
    study = optuna.create_study(direction="maximize")
    study.optimize(obj, n_trials=n_trials)

    best = study.best_trial
    print(f"Best Hit@{k}: {best.value:.4f}")
    print(f"Best params: rank={best.params['rank']}, maxIter={best.params['maxIter']}")

# COMMAND ----------

# DBTITLE 1,Train final model on full dataset
import os

with mlflow.start_run(run_name="als_final", nested=True, parent_run_id=parent_run.info.run_id):
    als_full, item_map_full, user_map_full = preprocess_pipeline(sdf_cleaned)
    final_model = train_als(als_full, rank=best.params["rank"], maxIter=best.params["maxIter"])

    # Save mappings as parquet artifacts
    user_map_full.write.parquet("/tmp/als_user_mapping.parquet", mode="overwrite")
    item_map_full.write.parquet("/tmp/als_item_mapping.parquet", mode="overwrite")

    mlflow.log_artifact("/tmp/als_user_mapping.parquet", "user_mapping")
    mlflow.log_artifact("/tmp/als_item_mapping.parquet", "item_mapping")
    mlflow.spark.log_model(final_model, artifact_path="model",
                           registered_model_name=f"{catalog}.{schema}.als_recommender")
    mlflow.log_params({"rank": best.params["rank"], "maxIter": best.params["maxIter"]})
    mlflow.log_metric("hit_at_k", best.value)

print("Final model logged to MLflow")

# COMMAND ----------

# DBTITLE 1,Save mappings to Delta
user_map_full.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{catalog}.{schema}.als_user_mapping"
)
item_map_full.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{catalog}.{schema}.als_item_mapping"
)

# COMMAND ----------

# DBTITLE 1,Generate pre-computed per-user recommendations
from pyspark.sql.functions import to_json, collect_list, struct, explode as explode_fn

recs_per_user = final_model.recommendForAllUsers(k)

# Explode recommendations array, join to item_mapping to get product slugs
recs_exploded = (
    recs_per_user
    .withColumn("rec", explode_fn("recommendations"))
    .select("user_id_int", col("rec.item_id").alias("item_id"), col("rec.rating").alias("score"))
)

recs_with_slugs = recs_exploded.join(item_map_full, on="item_id")
recs_with_users = recs_with_slugs.join(user_map_full, on="user_id_int")

# Aggregate back to one row per user with JSON recommendations
als_lookup = (
    recs_with_users
    .groupBy(col("user").alias("user_id"))
    .agg(to_json(collect_list(struct(col("item").alias("product"), "score"))).alias("recommendations"))
)

als_lookup.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{catalog}.{schema}.als_recommendations"
)

print(f"Wrote {als_lookup.count():,} user recommendation entries to {catalog}.{schema}.als_recommendations")
als_lookup.display()

# COMMAND ----------

# DBTITLE 1,Sync als_recommendations to Lakebase
# MAGIC %md
# MAGIC **Manual step**: Configure Lakebase sync for the `als_recommendations` table.
# MAGIC See notebook 02 for instructions and `databricks.yml` for automated config.
