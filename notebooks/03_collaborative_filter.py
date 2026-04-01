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

    # Build sparse CSR matrix (user x item) using raw counts as confidence signal.
    # implicit's ALS treats values as confidence weights; raw counts work better
    # than proportions since proportions compress the signal range.
    sparse_matrix = sparse.csr_matrix(
        (grouped["item_count"].values.astype(np.float32),
         (grouped["user_id_int"].values, grouped["item_id"].values)),
        shape=(len(unique_users), len(unique_items)),
    )

    return sparse_matrix, user_map_df, item_map_df, grouped


import time

print("Preprocessing training data...")
t0 = time.time()
sparse_train, user_map, item_map, train_ratings = preprocess_pipeline(train_sdf)
print(f"  ALS train: {sparse_train.nnz:,} user-item pairs in {time.time()-t0:.1f}s")
print(f"  Users: {len(user_map):,} | Items: {len(item_map):,}")

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


def train_als(sparse_matrix, rank, maxIter, regularization=0.1):
    model = AlternatingLeastSquares(
        factors=rank,
        regularization=regularization,
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


def evaluate_model(model, sparse_matrix, test_data, k=5, sample_size=2000):
    """Evaluate Hit@k for an implicit ALS model. Samples test data for speed."""
    eval_data = test_data.sample(n=min(sample_size, len(test_data)), random_state=42) if len(test_data) > sample_size else test_data

    # Batch recommend for all unique test users at once
    test_user_ids = eval_data["user_id_int"].unique()
    user_items = sparse_matrix[test_user_ids]
    all_ids, all_scores = model.recommend(test_user_ids, user_items, N=k, filter_already_liked_items=False)

    # Build lookup: user_idx -> set of recommended item_ids
    user_recs = {uid: set(ids) for uid, ids in zip(test_user_ids, all_ids)}

    hits = sum(1 for _, row in eval_data.iterrows() if row["added_item_id"] in user_recs.get(row["user_id_int"], set()))
    return hits / len(eval_data) if len(eval_data) > 0 else 0.0


def objective(trial, parent_run_id, k=5):
    with mlflow.start_run(parent_run_id=parent_run_id, nested=True):
        params = {
            "rank": trial.suggest_int("rank", 1, 100),
            "maxIter": trial.suggest_int("maxIter", 1, 10),
        }
        t0 = time.time()
        model = train_als(sparse_train, rank=params["rank"], maxIter=params["maxIter"])
        train_time = time.time() - t0

        t0 = time.time()
        avg_hit = evaluate_model(model, sparse_train, test_linked, k=k)
        eval_time = time.time() - t0

        print(f"  Trial {trial.number + 1}/{n_trials}: rank={params['rank']}, maxIter={params['maxIter']} -> Hit@{k}={avg_hit:.4f} (train {train_time:.1f}s, eval {eval_time:.1f}s)")

        mlflow.log_params(params)
        mlflow.log_metric("hit_at_k", avg_hit)
        return avg_hit


print(f"Starting Optuna HPO ({n_trials} trials)...")
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
import json

with mlflow.start_run(run_name="als_final", nested=True, parent_run_id=parent_run.info.run_id):
    print("Preprocessing full dataset for final model...")
    t0 = time.time()
    sparse_full, user_map_full, item_map_full, full_ratings = preprocess_pipeline(sdf_cleaned)
    print(f"  Done in {time.time()-t0:.1f}s. Training final model (rank={best.params['rank']}, maxIter={best.params['maxIter']})...")
    t0 = time.time()
    final_model = train_als(sparse_full, rank=best.params["rank"], maxIter=best.params["maxIter"])
    print(f"  Final model trained in {time.time()-t0:.1f}s")

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

# Batch recommend for all users at once (much faster than row-by-row)
print(f"Generating top-{k} recommendations for {len(user_map_full):,} users...")
t0 = time.time()

all_user_ids = np.arange(len(user_map_full))
all_item_ids, all_scores = final_model.recommend(
    all_user_ids, sparse_full[all_user_ids], N=k, filter_already_liked_items=False
)

lookup_rows = []
for i, user_row in user_map_full.iterrows():
    user_id = user_row["user"]
    recs = []
    for item_idx, score in zip(all_item_ids[i], all_scores[i]):
        slug = idx_to_item.get(int(item_idx))
        if slug:
            recs.append({"product": slug, "score": float(score)})
    if recs:
        lookup_rows.append(Row(user_id=user_id, recommendations=json.dumps(recs)))

    if (i + 1) % 500 == 0:
        print(f"  Processed {i+1:,}/{len(user_map_full):,} users...")

print(f"  Generated recs for {len(lookup_rows):,} users in {time.time()-t0:.1f}s")

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
