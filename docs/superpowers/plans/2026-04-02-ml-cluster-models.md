# ML Cluster Models Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rewrite notebooks 02 and 03 to use PySpark ML (FPGrowth + ALS) on single-user ML clusters for better performance and higher model fidelity, while keeping the serverless versions as a fallback.

**Architecture:** Notebooks 00-01 stay on serverless. Notebooks 02-03 switch from single-node Python libraries (mlxtend/implicit) to distributed PySpark ML (FPGrowth/ALS). The bundle config adds ML cluster definitions for these two tasks. All output table schemas stay identical so the app works unchanged.

**Tech Stack:** PySpark ML (FPGrowth, ALS), Optuna, MLflow, Databricks ML Runtime 15.4+

**Spec:** Based on the original KFC notebooks at `/tmp/recommender_model/Recommender Model/`

---

## Why ML Clusters

| Aspect | Serverless (current) | ML Cluster (this branch) |
|---|---|---|
| **MBA library** | mlxtend (single-node, driver-only) | PySpark FPGrowth (distributed) |
| **ALS library** | implicit (single-node, driver-only) | PySpark ALS (distributed) |
| **Data scale** | Limited by driver memory (~1M rows practical max) | Scales to billions of rows |
| **MBA performance** | ~9 min for 50K orders (one-hot encoding bottleneck) | ~1-2 min for 500K orders |
| **ALS performance** | ~3 min for 50K orders (batch recommend is fast) | ~2-3 min for 500K orders |
| **Evaluation** | Sampled (3K/2K rows) | Full test set via distributed joins |
| **Model fidelity** | mlxtend FPGrowth matches PySpark; implicit ALS differs from Spark ALS | Native PySpark ML, same as production deployments |
| **Cluster cost** | None (serverless) | ML cluster provisioning (~5-10 min cold start, ~$2-5/run) |

## File Structure Changes

```
notebooks/
├── 02_market_basket.py          # REWRITE: PySpark FPGrowth
├── 03_collaborative_filter.py   # REWRITE: PySpark ALS + Optuna
├── 02_market_basket_serverless.py    # RENAME: keep current as fallback
└── 03_collaborative_filter_serverless.py  # RENAME: keep current as fallback

databricks.yml                   # UPDATE: add ML cluster config for tasks 02/03
config.yaml                      # UPDATE: restore higher defaults (500K orders, 10K users, 20 trials)
```

---

## Task 1: Preserve Serverless Versions as Fallback

**Files:**
- Rename: `notebooks/02_market_basket.py` -> `notebooks/02_market_basket_serverless.py`
- Rename: `notebooks/03_collaborative_filter.py` -> `notebooks/03_collaborative_filter_serverless.py`

- [ ] **Step 1: Rename current notebooks**

```bash
cd ~/Desktop/Projects/recommender-accelerator
git mv notebooks/02_market_basket.py notebooks/02_market_basket_serverless.py
git mv notebooks/03_collaborative_filter.py notebooks/03_collaborative_filter_serverless.py
```

- [ ] **Step 2: Commit**

```bash
git add -A
git commit -m "refactor: rename serverless ML notebooks as fallback variants"
```

---

## Task 2: Rewrite Notebook 02 - FPGrowth on ML Cluster

**Files:**
- Create: `notebooks/02_market_basket.py`

Key differences from the serverless version:
- Uses `pyspark.ml.fpm.FPGrowth` directly on Spark DataFrames (no `.toPandas()`)
- `numPartitions` via `spark.sparkContext.defaultParallelism` (available on ML clusters)
- `generate_recommendations()` uses distributed Spark: broadcast join, Window functions, array operations
- Evaluation runs on full test set via Spark joins (no sampling)
- Pre-compute lookup uses Spark `generate_recommendations()` with single-item cart DataFrames
- MLflow PyFunc wraps rules parquet with the same `MBARecommenderModel` class

- [ ] **Step 1: Create 02_market_basket.py**

Create `~/Desktop/Projects/recommender-accelerator/notebooks/02_market_basket.py` with these cells:

**Cell 1: Markdown header**
```python
# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Market Basket Analysis (PySpark FPGrowth)
# MAGIC Trains distributed FPGrowth model, evaluates with Hit@k, logs to MLflow,
# MAGIC and generates pre-computed per-product recommendation lookup table.
# MAGIC
# MAGIC **Compute**: Requires single-user ML cluster (PySpark ML).
```

**Cell 2: %run config_loader** (own cell)
```python
# MAGIC %run ./config_loader
```

**Cell 3: Load config**
```python
import time

cfg = load_config()
catalog = cfg["catalog"]
schema = cfg["schema"]
min_transactions = cfg.get("mba_min_transactions", 1000)
min_confidence = cfg.get("mba_min_confidence", 0.0)
k = cfg.get("recommendation_k", 5)
experiment_root = cfg.get("mlflow_experiment_root", "/Shared/recommender-accelerator")

print(f"Config: catalog={catalog}.{schema}, min_transactions={min_transactions}, k={k}")
```

**Cell 4: Load cleaned orders**
```python
sdf_cleaned = spark.read.table(f"{catalog}.{schema}.cleaned_orders")
num_transactions = sdf_cleaned.count()
print(f"Loaded {num_transactions:,} cleaned orders")
```

**Cell 5: EDA - Product support**
```python
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
```

**Cell 6: Train FPGrowth**
```python
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
```

**Cell 7: Save rules and train/test**
```python
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
```

**Cell 8: Recommendation scoring function**
```python
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
```

**Cell 9: Evaluate Hit@k on full test set**
```python
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
```

**Cell 10: Log model to MLflow**
```python
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
```

**Cell 11: Pre-compute MBA lookup table**
```python
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
```

**Cell 12: Lakebase sync note (markdown)**
```python
# MAGIC %md
# MAGIC **Manual step**: Configure Lakebase sync for `mba_recommendations`.
```

- [ ] **Step 2: Commit**

```bash
git add notebooks/02_market_basket.py
git commit -m "feat: rewrite notebook 02 with PySpark FPGrowth for ML cluster"
```

---

## Task 3: Rewrite Notebook 03 - ALS on ML Cluster

**Files:**
- Create: `notebooks/03_collaborative_filter.py`

Key differences from serverless version:
- Uses `pyspark.ml.recommendation.ALS` with distributed training
- `preprocess_pipeline()` stays in Spark (no `.toPandas()` for training data)
- Evaluation via `model.recommendForUserSubset()` (distributed)
- Pre-compute via `model.recommendForAllUsers(k)` (distributed, much faster)
- Optuna HPO with nested MLflow runs (same structure)
- Uses `sc.defaultParallelism` for repartitioning

- [ ] **Step 1: Create 03_collaborative_filter.py**

Create `~/Desktop/Projects/recommender-accelerator/notebooks/03_collaborative_filter.py`. Follow the same pattern as the original KFC notebook but with:
- `%run ./config_loader` in its own cell
- Config-driven parameters from `cfg`
- Progress prints with `time.time()` for each major step
- User-scoped MLflow experiment path
- Pre-computed lookup table written to `als_recommendations` with JSON recommendations
- `filter_already_liked_items=False` on `recommendForAllUsers` (app handles cart filtering)

The notebook should have these cells:
1. `%pip install optuna`
2. Markdown header
3. `%run ./config_loader`
4. Load config
5. Load data + train/test split
6. `preprocess_pipeline()` function (Spark-native: explode, integer mappings via Window/row_number, implicit ratings as proportion)
7. Preprocess training data
8. Prepare test set (filter >1 item, split cart/added, join to user/item mappings)
9. `train_als()` function
10. Optuna HPO with per-trial progress prints
11. Train final model on full dataset, log to MLflow
12. Save user/item mappings to Delta
13. Pre-compute per-user recommendations via `recommendForAllUsers(k)`, explode, join mappings, aggregate as JSON, write to `als_recommendations`
14. Lakebase sync note

Use the exact same `preprocess_pipeline`, `train_als`, and evaluation pattern from the original KFC notebooks at `/tmp/recommender_model/Recommender Model/02_collaborative_filter.py`, adapted for the config-driven catalog/schema and with progress prints.

- [ ] **Step 2: Commit**

```bash
git add notebooks/03_collaborative_filter.py
git commit -m "feat: rewrite notebook 03 with PySpark ALS for ML cluster"
```

---

## Task 4: Update databricks.yml with ML Cluster Config

**Files:**
- Modify: `databricks.yml`

- [ ] **Step 1: Add ML cluster definitions for notebooks 02 and 03**

Update the `market_basket` and `collaborative_filter` tasks in `databricks.yml`:

```yaml
        - task_key: market_basket
          notebook_task:
            notebook_path: ./notebooks/02_market_basket.py
          depends_on:
            - task_key: data_preparation
          new_cluster:
            spark_version: "17.3.x-scala2.13"
            node_type_id: "Standard_E4ds_v4"
            num_workers: 2
            data_security_mode: SINGLE_USER

        - task_key: collaborative_filter
          notebook_task:
            notebook_path: ./notebooks/03_collaborative_filter.py
          depends_on:
            - task_key: data_preparation
          new_cluster:
            spark_version: "17.3.x-scala2.13"
            node_type_id: "Standard_E4ds_v4"
            num_workers: 2
            data_security_mode: SINGLE_USER
```

- [ ] **Step 2: Commit**

```bash
git add databricks.yml
git commit -m "feat: add ML cluster config for notebooks 02 and 03"
```

---

## Task 5: Update Config Defaults for Higher Fidelity

**Files:**
- Modify: `config.yaml`

- [ ] **Step 1: Restore production-scale defaults**

```yaml
order_count: 500000
user_count: 10000
store_count: 50
seed: 42

# Model parameters
mba_min_transactions: 1000
mba_min_confidence: 0.0
als_hpo_trials: 20
recommendation_k: 5
```

- [ ] **Step 2: Commit**

```bash
git add config.yaml
git commit -m "feat: restore production-scale defaults (500K orders, 10K users, 20 HPO trials)"
```

---

## Task 6: Update README

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Update architecture and model sections**

Update to reflect:
- Notebooks 02-03 now require single-user ML cluster (15.4.x-ml-scala2.12)
- Serverless fallback notebooks available as `*_serverless.py`
- Higher defaults: 500K orders, 10K users, 20 HPO trials
- Updated pipeline runtime estimates
- Note about workspace requirements (must support classic clusters)

- [ ] **Step 2: Commit and push**

```bash
git add README.md
git commit -m "docs: update README for ML cluster models branch"
git push -u origin feature/ml-cluster-models
```
