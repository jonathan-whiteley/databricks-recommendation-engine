# Databricks notebook source
# DBTITLE 1,Install optuna for hyperparameter search
# MAGIC %pip install optuna

# COMMAND ----------

# MAGIC %md
# MAGIC # 03 - Collaborative Filtering (PySpark ALS)
# MAGIC Trains an ALS model using **PySpark ML** (distributed) with Optuna hyperparameter
# MAGIC optimization, evaluates with Hit@k, logs to MLflow, and generates pre-computed
# MAGIC per-user recommendation lookup table.
# MAGIC
# MAGIC **Compute**: Requires a single-user ML cluster (not serverless).
# MAGIC PySpark ALS runs distributed across the cluster; no single-node implicit library needed.

# COMMAND ----------

# MAGIC %run ./config_loader

# COMMAND ----------

# DBTITLE 1,Load config
cfg = load_config()
catalog = cfg["catalog"]
schema = cfg["schema"]
k = cfg.get("recommendation_k", 5)
n_trials = cfg.get("als_hpo_trials", 20)
experiment_root = cfg.get("mlflow_experiment_root", "/Shared/recommender-accelerator")

print(f"catalog={catalog}, schema={schema}, k={k}, n_trials={n_trials}")

# COMMAND ----------

# DBTITLE 1,Load data + train/test split
import time

sdf_cleaned = spark.read.table(f"{catalog}.{schema}.cleaned_orders")
sdf_ratings = spark.read.table(f"{catalog}.{schema}.user_item_ratings")

train_sdf, test_sdf = sdf_cleaned.randomSplit([0.8, 0.2], seed=42)
print(f"Train: {train_sdf.count():,} | Test: {test_sdf.count():,}")

# COMMAND ----------

# DBTITLE 1,Define preprocess_pipeline (Spark-native ALS preprocessing)
from pyspark.sql.functions import explode, col, count, sum as spark_sum, row_number
from pyspark.sql.window import Window


def preprocess_pipeline(train_dataset):
    """
    Preprocess training dataset for PySpark ALS (Alternating Least Squares).

    Steps:
    1. Explode order_product_list to one product per row.
    2. Alias user_id -> user, product -> item.
    3. Create integer mappings for item and user via Window/row_number (Spark-native).
    4. Join mappings back; compute implicit ratings as proportion of orders per item.

    Args:
        train_dataset (DataFrame): Cleaned orders DataFrame with user_id (string)
                                   and order_product_list (array<string>).

    Returns:
        tuple:
            - als_train_dataset (DataFrame): user_id_int, item_id, proportion_of_orders
            - item_mapping (DataFrame): item (product slug), item_id (integer)
            - user_mapping (DataFrame): user (original user_id string), user_id_int (integer)
    """
    # Explode order_product_list -> one row per (user, item)
    sdf_exploded = train_dataset.withColumn("product", explode("order_product_list"))
    als_input = sdf_exploded.select(
        col("user_id").alias("user"),
        col("product").alias("item"),
    )

    # Integer mappings via Window/row_number (deterministic, Spark-native)
    item_mapping = (
        als_input.select("item")
        .distinct()
        .withColumn("item_id", row_number().over(Window.orderBy("item")))
    )
    user_mapping = (
        als_input.select("user")
        .distinct()
        .withColumn("user_id_int", row_number().over(Window.orderBy("user")))
    )

    # Join mappings; user_id is a string so use explicit join condition
    als_input_mapped = (
        als_input
        .join(item_mapping, on="item")
        .join(user_mapping, als_input["user"] == user_mapping["user"], "inner")
        .select("user_id_int", "item_id", "item")
    )

    # Implicit rating: proportion of a user's orders that contained each item
    als_input_grouped = als_input_mapped.groupBy("user_id_int", "item_id").agg(
        count("*").alias("historical_ordered_amount")
    )
    user_total_orders = als_input_grouped.groupBy("user_id_int").agg(
        spark_sum("historical_ordered_amount").alias("total_orders")
    )
    als_input_with_totals = als_input_grouped.join(user_total_orders, on="user_id_int")
    als_train_dataset = als_input_with_totals.withColumn(
        "proportion_of_orders",
        col("historical_ordered_amount") / col("total_orders"),
    )

    return als_train_dataset, item_mapping, user_mapping

# COMMAND ----------

# DBTITLE 1,Preprocess training data
print("Preprocessing training data...")
t0 = time.time()
als_train_dataset, item_mapping, user_mapping = preprocess_pipeline(train_sdf)

# Cache mappings - reused during test prep and HPO
item_mapping.cache()
user_mapping.cache()
als_train_dataset.cache()

n_users = user_mapping.count()
n_items = item_mapping.count()
n_pairs = als_train_dataset.count()
print(f"  Done in {time.time()-t0:.1f}s")
print(f"  Users: {n_users:,} | Items: {n_items:,} | User-item pairs: {n_pairs:,}")

# COMMAND ----------

# DBTITLE 1,Prepare test set for evaluation
from pyspark.sql.functions import size, expr, array_contains, avg

# Filter orders to those with more than one item (need a holdout item)
test_filtered = test_sdf.filter(size(col("order_product_list")) > 1)

# cart = all items except the last; added = the last item (holdout)
test_transformed = (
    test_filtered
    .withColumn("cart", expr("slice(order_product_list, 1, size(order_product_list) - 1)"))
    .withColumn("added", expr("order_product_list[size(order_product_list) - 1]"))
)

# Join to user_mapping from training (inner join keeps only known users)
test_linked = test_transformed.join(
    user_mapping,
    test_transformed["user_id"] == user_mapping["user"],
    "inner",
)

# Join added item to item_mapping from training; rename to avoid collision
test_linked = (
    test_linked
    .join(item_mapping, test_linked["added"] == item_mapping["item"], "inner")
    .withColumnRenamed("item_id", "added_item_id")
)

test_linked.cache()

test_total = test_sdf.count()
test_evaluable = test_linked.count()
print(f"Test set: {test_evaluable:,} evaluable rows (dropped {test_total - test_evaluable:,} with unknown users/items or single-item orders)")

# COMMAND ----------

# DBTITLE 1,Define train_als function (PySpark ALS)
from pyspark.ml.recommendation import ALS


def train_als(train_dataset, rank, maxIter):
    """
    Train a PySpark ALS model for implicit feedback.

    Args:
        train_dataset (DataFrame): Must have user_id_int (int), item_id (int),
                                   proportion_of_orders (float) columns.
        rank (int): Number of latent factors.
        maxIter (int): Maximum number of ALS iterations.

    Returns:
        ALSModel: Fitted PySpark ALS model.
    """
    als = ALS(
        rank=rank,
        maxIter=maxIter,
        userCol="user_id_int",
        itemCol="item_id",
        ratingCol="proportion_of_orders",
        implicitPrefs=True,
        coldStartStrategy="drop",
    )
    return als.fit(train_dataset)

# COMMAND ----------

# DBTITLE 1,Optuna HPO with nested MLflow runs
import optuna
import mlflow
from functools import partial

optuna.logging.set_verbosity(optuna.logging.WARNING)

current_user = spark.sql("SELECT current_user()").collect()[0][0]
experiment_name = f"/Users/{current_user}/recommender-accelerator-als"
mlflow.set_experiment(experiment_name)


def objective(trial, parent_run_id, k=5):
    with mlflow.start_run(parent_run_id=parent_run_id, nested=True):
        params = {
            "rank": trial.suggest_int("rank", 1, 100),
            "maxIter": trial.suggest_int("maxIter", 1, 10),
        }

        t0 = time.time()
        model = train_als(als_train_dataset, rank=params["rank"], maxIter=params["maxIter"])
        train_time = time.time() - t0

        # Evaluate: recommendForUserSubset (distributed, avoids sampling to pandas)
        t0 = time.time()
        test_users = test_linked.select("user_id_int").distinct()
        test_recs = model.recommendForUserSubset(test_users, k)
        test_recs = (
            test_recs
            .join(test_linked, "user_id_int", "inner")
            .withColumn(
                "hit@k",
                array_contains(col("recommendations.item_id"), col("added_item_id")).cast("int"),
            )
        )
        avg_hit = test_recs.agg(avg(col("hit@k")).alias("average_hit@k")).first()["average_hit@k"]
        eval_time = time.time() - t0

        print(
            f"  Trial {trial.number + 1}/{n_trials}: rank={params['rank']}, maxIter={params['maxIter']}"
            f" -> Hit@{k}={avg_hit:.4f} (train {train_time:.1f}s, eval {eval_time:.1f}s)"
        )

        mlflow.log_params(params)
        mlflow.log_metric("hit_at_k", avg_hit)
        return avg_hit


print(f"Starting Optuna HPO ({n_trials} trials), experiment: {experiment_name}")
hpo_start = time.time()

with mlflow.start_run(run_name="als_hpo") as parent_run:
    obj = partial(objective, parent_run_id=parent_run.info.run_id, k=k)
    study = optuna.create_study(direction="maximize")
    study.optimize(obj, n_trials=n_trials)

    best = study.best_trial
    print(f"\nHPO complete in {time.time()-hpo_start:.1f}s")
    print(f"Best Hit@{k}: {best.value:.4f}")
    print(f"Best params: rank={best.params['rank']}, maxIter={best.params['maxIter']}")

# COMMAND ----------

# DBTITLE 1,Train final model on full dataset
import os

print("Preprocessing full dataset for final model...")
t0 = time.time()
als_full_dataset, item_mapping_full, user_mapping_full = preprocess_pipeline(sdf_cleaned)
als_full_dataset = als_full_dataset.repartition(sc.defaultParallelism * 10)
print(f"  Full dataset preprocessed in {time.time()-t0:.1f}s")
print(f"  Training final model (rank={best.params['rank']}, maxIter={best.params['maxIter']})...")

t0 = time.time()
with mlflow.start_run(run_name="als_final", nested=True, parent_run_id=parent_run.info.run_id):
    final_model = train_als(als_full_dataset, rank=best.params["rank"], maxIter=best.params["maxIter"])
    print(f"  Final model trained in {time.time()-t0:.1f}s")

    # Write mappings to temp parquet so they can be logged as MLflow artifacts
    tmp_user = os.getcwd() + "/als_user_mapping.parquet"
    tmp_item = os.getcwd() + "/als_item_mapping.parquet"
    user_mapping_full.write.parquet(tmp_user, mode="overwrite")
    item_mapping_full.write.parquet(tmp_item, mode="overwrite")

    mlflow.log_artifact("/dbfs" + tmp_user, "user_mapping")
    mlflow.log_artifact("/dbfs" + tmp_item, "item_mapping")
    mlflow.spark.log_model(final_model, artifact_path="model")
    mlflow.log_params({"rank": best.params["rank"], "maxIter": best.params["maxIter"]})
    mlflow.log_metric("hit_at_k", best.value)

print("Final model logged to MLflow")

# COMMAND ----------

# DBTITLE 1,Save user/item mappings to Delta
user_mapping_full.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(f"{catalog}.{schema}.als_user_mapping")

item_mapping_full.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(f"{catalog}.{schema}.als_item_mapping")

print(f"Mappings written to {catalog}.{schema}.als_user_mapping and als_item_mapping")

# COMMAND ----------

# DBTITLE 1,Pre-compute per-user recommendations
from pyspark.sql.functions import struct, to_json

print(f"Generating top-{k} recommendations for all users...")
t0 = time.time()

# recommendForAllUsers returns: user_id_int, recommendations (array<struct<item_id, rating>>)
# filter_already_liked_items=False: app layer handles cart filtering at serve time
raw_recs = final_model.recommendForAllUsers(k)

# Explode recommendations array -> one row per (user_id_int, item_id, score)
recs_exploded = (
    raw_recs
    .select(
        col("user_id_int"),
        explode(col("recommendations")).alias("rec"),
    )
    .select(
        col("user_id_int"),
        col("rec.item_id").alias("item_id"),
        col("rec.rating").alias("score"),
    )
)

# Join item_mapping_full to get product slugs
recs_with_slugs = recs_exploded.join(item_mapping_full, on="item_id", how="inner")

# Join user_mapping_full to recover original user_id string
recs_with_users = recs_with_slugs.join(user_mapping_full, on="user_id_int", how="inner")

# Aggregate per user: JSON list of {product, score} dicts
from pyspark.sql.functions import collect_list

als_lookup = (
    recs_with_users
    .select(
        col("user").alias("user_id"),
        struct(col("item").alias("product"), col("score")).alias("rec"),
    )
    .groupBy("user_id")
    .agg(to_json(collect_list("rec")).alias("recommendations"))
)

als_lookup.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(f"{catalog}.{schema}.als_recommendations")

n_written = als_lookup.count()
print(f"  Generated and wrote {n_written:,} user recommendation entries in {time.time()-t0:.1f}s")
print(f"  Table: {catalog}.{schema}.als_recommendations")
display(als_lookup.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lakebase Sync
# MAGIC
# MAGIC **Manual step**: After running this notebook, configure a Lakebase online table sync
# MAGIC for `als_recommendations` so the serving app can query it at low latency.
# MAGIC
# MAGIC Steps:
# MAGIC 1. In the Databricks UI, navigate to **Catalog > your schema > als_recommendations**.
# MAGIC 2. Click **Create online table** (or use `databricks.yml` if automated config is in place).
# MAGIC 3. Set the primary key to `user_id`.
# MAGIC 4. Enable continuous sync or triggered sync depending on retraining cadence.
# MAGIC
# MAGIC See `databricks.yml` for the declarative Lakebase sync configuration used in this accelerator.
