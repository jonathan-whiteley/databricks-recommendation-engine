# Databricks notebook source
# MAGIC %pip install optuna implicit scipy

# COMMAND ----------

# MAGIC %md
# MAGIC # 03 - Collaborative Filtering (ALS)
# MAGIC Trains ALS model (via implicit library, single-node) with Optuna hyperparameter
# MAGIC optimization, evaluates with Hit@k, logs to MLflow, and generates pre-computed
# MAGIC per-user recommendation lookup table.
# MAGIC
# MAGIC **Compute**: Serverless compatible (single-node Python via implicit).

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

# COMMAND ----------

# DBTITLE 1,Load data
import pandas as pd
import numpy as np
from scipy import sparse

sdf_cleaned = spark.read.table(f"{catalog}.{schema}.cleaned_orders")
sdf_ratings = spark.read.table(f"{catalog}.{schema}.user_item_ratings")

train_sdf, test_sdf = sdf_cleaned.randomSplit([0.8, 0.2], seed=42)
print(f"Train: {train_sdf.count():,} | Test: {test_sdf.count():,}")

# COMMAND ----------

# DBTITLE 1,Preprocess for ALS
def preprocess_pipeline(dataset_sdf):
    """
    Preprocess dataset for implicit ALS.
    Explodes orders into user-item pairs, creates integer mappings,
    computes implicit ratings (proportion of user's orders containing each item).

    Returns: (sparse_matrix, user_map_df, item_map_df, ratings_pd)
    """
    from pyspark.sql.functions import explode, col, count, sum as spark_sum

    sdf_exploded = dataset_sdf.withColumn("product", explode("order_product_list"))
    als_input = sdf_exploded.select(col("user_id").alias("user"), col("product").alias("item"))

    # Convert to pandas for single-node processing
    ratings_pd = als_input.toPandas()

    # Create integer mappings
    unique_users = ratings_pd["user"].unique()
    unique_items = ratings_pd["item"].unique()

    user_to_idx = {u: i for i, u in enumerate(unique_users)}
    item_to_idx = {it: i for i, it in enumerate(unique_items)}

    user_map_df = pd.DataFrame({"user": unique_users, "user_id_int": range(len(unique_users))})
    item_map_df = pd.DataFrame({"item": unique_items, "item_id": range(len(unique_items))})

    # Map to integer IDs
    ratings_pd["user_id_int"] = ratings_pd["user"].map(user_to_idx)
    ratings_pd["item_id"] = ratings_pd["item"].map(item_to_idx)

    # Compute implicit ratings: proportion of user's orders containing each item
    grouped = ratings_pd.groupby(["user_id_int", "item_id"]).size().reset_index(name="item_count")
    user_totals = grouped.groupby("user_id_int")["item_count"].sum().reset_index(name="total_orders")
    grouped = grouped.merge(user_totals, on="user_id_int")
    grouped["proportion_of_orders"] = grouped["item_count"] / grouped["total_orders"]

    # Build sparse CSR matrix (user x item)
    sparse_matrix = sparse.csr_matrix(
        (grouped["proportion_of_orders"].values,
         (grouped["user_id_int"].values, grouped["item_id"].values)),
        shape=(len(unique_users), len(unique_items)),
    )

    return sparse_matrix, user_map_df, item_map_df, grouped


sparse_train, user_map, item_map, train_ratings = preprocess_pipeline(train_sdf)
print(f"ALS train: {sparse_train.nnz:,} user-item pairs")
print(f"Users: {len(user_map):,} | Items: {len(item_map):,}")

# COMMAND ----------

# DBTITLE 1,Prepare test set
from pyspark.sql.functions import col, size, expr

test_filtered = test_sdf.filter(size(col("order_product_list")) > 1)
test_transformed = (
    test_filtered
    .withColumn("cart", expr("slice(order_product_list, 1, size(order_product_list) - 1)"))
    .withColumn("added", expr("order_product_list[size(order_product_list) - 1]"))
)

test_pd = test_transformed.select("user_id", "cart", "added").toPandas()

# Map test users/items to training indices
user_to_idx = dict(zip(user_map["user"], user_map["user_id_int"]))
item_to_idx = dict(zip(item_map["item"], item_map["item_id"]))

# Filter to users and items that exist in training set
test_pd["user_id_int"] = test_pd["user_id"].map(user_to_idx)
test_pd["added_item_id"] = test_pd["added"].map(item_to_idx)
test_linked = test_pd.dropna(subset=["user_id_int", "added_item_id"]).copy()
test_linked["user_id_int"] = test_linked["user_id_int"].astype(int)
test_linked["added_item_id"] = test_linked["added_item_id"].astype(int)
print(f"Test set: {len(test_linked):,} evaluable rows")

# COMMAND ----------

# DBTITLE 1,Define ALS training function
from implicit.als import AlternatingLeastSquares


def train_als(sparse_matrix, rank, maxIter):
    model = AlternatingLeastSquares(
        factors=rank,
        iterations=maxIter,
        use_gpu=False,
    )
    model.fit(sparse_matrix)
    return model

# COMMAND ----------

# DBTITLE 1,Optuna HPO
import optuna
import mlflow
from functools import partial

experiment_name = f"/Users/{spark.sql('SELECT current_user()').collect()[0][0]}/recommender-accelerator-als"
mlflow.set_experiment(experiment_name)


def evaluate_model(model, sparse_matrix, test_data, k=5):
    """Evaluate Hit@k for an implicit ALS model."""
    hits = 0
    total = 0
    for _, row in test_data.iterrows():
        user_idx = row["user_id_int"]
        added_item_id = row["added_item_id"]

        # Get top-k recommendations for this user
        item_ids, scores = model.recommend(
            user_idx, sparse_matrix[user_idx], N=k, filter_already_liked_items=False
        )

        if added_item_id in item_ids:
            hits += 1
        total += 1

    return hits / total if total > 0 else 0.0


def objective(trial, parent_run_id, k=5):
    with mlflow.start_run(parent_run_id=parent_run_id, nested=True):
        params = {
            "rank": trial.suggest_int("rank", 1, 100),
            "maxIter": trial.suggest_int("maxIter", 1, 10),
        }
        model = train_als(sparse_train, rank=params["rank"], maxIter=params["maxIter"])
        avg_hit = evaluate_model(model, sparse_train, test_linked, k=k)

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
import json

with mlflow.start_run(run_name="als_final", nested=True, parent_run_id=parent_run.info.run_id):
    sparse_full, user_map_full, item_map_full, full_ratings = preprocess_pipeline(sdf_cleaned)
    final_model = train_als(sparse_full, rank=best.params["rank"], maxIter=best.params["maxIter"])

    # Save mappings as parquet artifacts
    user_map_full.to_parquet("/tmp/als_user_mapping.parquet", index=False)
    item_map_full.to_parquet("/tmp/als_item_mapping.parquet", index=False)

    mlflow.log_artifact("/tmp/als_user_mapping.parquet", "user_mapping")
    mlflow.log_artifact("/tmp/als_item_mapping.parquet", "item_mapping")
    mlflow.log_params({"rank": best.params["rank"], "maxIter": best.params["maxIter"]})
    mlflow.log_metric("hit_at_k", best.value)

print("Final model logged to MLflow")

# COMMAND ----------

# DBTITLE 1,Save mappings to Delta
spark.createDataFrame(user_map_full).write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(f"{catalog}.{schema}.als_user_mapping")

spark.createDataFrame(item_map_full).write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(f"{catalog}.{schema}.als_item_mapping")

# COMMAND ----------

# DBTITLE 1,Generate pre-computed per-user recommendations
from pyspark.sql import Row

# Build reverse mapping from item index to slug
idx_to_item = dict(zip(item_map_full["item_id"], item_map_full["item"]))

# Generate recommendations for all users
lookup_rows = []
for _, user_row in user_map_full.iterrows():
    user_idx = user_row["user_id_int"]
    user_id = user_row["user"]

    item_ids, scores = final_model.recommend(
        user_idx, sparse_full[user_idx], N=k, filter_already_liked_items=True
    )

    recs = []
    for item_idx, score in zip(item_ids, scores):
        slug = idx_to_item.get(int(item_idx))
        if slug:
            recs.append({"product": slug, "score": float(score)})

    if recs:
        lookup_rows.append(Row(user_id=user_id, recommendations=json.dumps(recs)))

als_lookup = spark.createDataFrame(lookup_rows)
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
