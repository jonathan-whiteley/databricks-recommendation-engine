# Lakebase Sync Notebook Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a pipeline notebook that creates Lakebase tables, generates user_profiles, and bulk-loads all 4 tables from Delta so the app works end-to-end with zero manual steps.

**Architecture:** New notebook `04_lakebase_sync.py` runs on serverless after MBA and ALS tasks complete. It connects to Lakebase via Databricks SDK OAuth, creates tables if missing, builds `user_profiles` from `raw_orders`, and bulk-inserts all 4 tables (truncate + insert pattern). The Lakebase instance name is read from `config.yaml`.

**Tech Stack:** PySpark, psycopg2, Databricks SDK (OAuth), Delta tables

---

## File Structure Changes

```
notebooks/
├── 04_lakebase_sync.py        # NEW: Lakebase table creation + data sync

config.yaml                     # UPDATE: add lakebase_instance field
databricks.yml                  # UPDATE: add lakebase_sync task to pipeline
```

---

## Task 1: Add lakebase_instance to config.yaml

**Files:**
- Modify: `config.yaml`

- [ ] **Step 1: Add lakebase_instance field**

Add after the `mlflow_experiment_root` line in `config.yaml`:

```yaml
# Lakebase instance name (must exist before running pipeline)
lakebase_instance: jdub-lakebase-db-instance
```

- [ ] **Step 2: Commit**

```bash
git add config.yaml
git commit -m "feat: add lakebase_instance to config.yaml"
```

---

## Task 2: Create notebook 04_lakebase_sync.py

**Files:**
- Create: `notebooks/04_lakebase_sync.py`

- [ ] **Step 1: Create 04_lakebase_sync.py**

Create `~/Desktop/Projects/recommender-accelerator/notebooks/04_lakebase_sync.py` with these cells:

**Cell 1: Install psycopg2**
```python
# Databricks notebook source
# MAGIC %pip install psycopg2-binary
```

**Cell 2: Markdown header**
```python
# MAGIC %md
# MAGIC # 04 - Lakebase Sync
# MAGIC Creates PostgreSQL tables in Lakebase and bulk-loads data from Delta.
# MAGIC Generates `user_profiles` from raw order data.
# MAGIC
# MAGIC **Compute**: Serverless compatible.
# MAGIC
# MAGIC **Depends on**: Notebooks 00-03 must have run first (Delta tables must exist).
```

**Cell 3: %run config_loader**
```python
# MAGIC %run ./config_loader
```

**Cell 4: Load config + connect to Lakebase**
```python
import time
import uuid
import psycopg2
from databricks.sdk import WorkspaceClient

cfg = load_config()
catalog = cfg["catalog"]
schema = cfg["schema"]
lakebase_instance = cfg["lakebase_instance"]

w = WorkspaceClient()
instance = w.database.get_database_instance(name=lakebase_instance)
cred = w.database.generate_database_credential(
    request_id=str(uuid.uuid4()),
    instance_names=[lakebase_instance],
)

conn_params = {
    "host": instance.read_write_dns,
    "port": 5432,
    "dbname": "databricks_postgres",
    "user": w.current_user.me().user_name,
    "password": cred.token,
    "sslmode": "require",
}

# Test connection
conn = psycopg2.connect(**conn_params)
cur = conn.cursor()
cur.execute("SELECT version()")
print(f"Connected to Lakebase: {cur.fetchone()[0][:60]}...")
cur.close()
conn.close()

print(f"Instance: {lakebase_instance}")
print(f"Host: {instance.read_write_dns}")
```

**Cell 5: Create tables**
```python
conn = psycopg2.connect(**conn_params)
cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS product_catalog (
    product_id TEXT PRIMARY KEY,
    product_name TEXT,
    product_slug TEXT,
    category TEXT,
    base_price DOUBLE PRECISION,
    popularity_weight DOUBLE PRECISION
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS mba_recommendations (
    product_slug TEXT PRIMARY KEY,
    recommendations JSONB
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS als_recommendations (
    user_id TEXT PRIMARY KEY,
    recommendations JSONB
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS user_profiles (
    user_id TEXT PRIMARY KEY,
    primary_store TEXT,
    store_visits INTEGER,
    total_orders INTEGER
)
""")

conn.commit()
cur.close()
conn.close()
print("All 4 Lakebase tables created (or already exist)")
```

**Cell 6: Generate user_profiles from raw_orders**
```python
from pyspark.sql.functions import col, count, countDistinct, first
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc

print("Generating user_profiles from raw_orders...")
t0 = time.time()

raw_orders = spark.read.table(f"{catalog}.{schema}.raw_orders")

# Primary store = most frequently visited store per user
store_counts = (
    raw_orders
    .groupBy("user_id", "store_id")
    .agg(count("*").alias("visit_count"))
)
w = Window.partitionBy("user_id").orderBy(desc("visit_count"))
primary_stores = (
    store_counts
    .withColumn("rank", row_number().over(w))
    .filter(col("rank") == 1)
    .select(
        col("user_id"),
        col("store_id").alias("primary_store"),
        col("visit_count").alias("store_visits"),
    )
)

# Total orders per user
user_totals = raw_orders.groupBy("user_id").agg(count("*").alias("total_orders"))

# Join
user_profiles_sdf = primary_stores.join(user_totals, on="user_id")

user_profiles_sdf.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(f"{catalog}.{schema}.user_profiles")

profile_count = user_profiles_sdf.count()
print(f"  Wrote {profile_count:,} user profiles in {time.time()-t0:.1f}s")
user_profiles_sdf.display()
```

**Cell 7: Helper function for bulk load**
```python
from psycopg2.extras import execute_values


def sync_table(conn_params, table_name, columns, rows, batch_size=5000):
    """Truncate and bulk-insert rows into a Lakebase table."""
    conn = psycopg2.connect(**conn_params)
    cur = conn.cursor()

    cur.execute(f"TRUNCATE TABLE {table_name}")

    cols_str = ", ".join(columns)
    template = "(" + ", ".join(["%s"] * len(columns)) + ")"

    total = len(rows)
    for i in range(0, total, batch_size):
        batch = rows[i : i + batch_size]
        execute_values(cur, f"INSERT INTO {table_name} ({cols_str}) VALUES %s", batch, template=template)

    conn.commit()
    cur.close()
    conn.close()
    return total
```

**Cell 8: Sync product_catalog**
```python
print("Syncing product_catalog...")
t0 = time.time()

df = spark.read.table(f"{catalog}.{schema}.product_catalog").toPandas()
rows = [
    (r.product_id, r.product_name, r.product_slug, r.category, float(r.base_price), float(r.popularity_weight))
    for _, r in df.iterrows()
]
n = sync_table(
    conn_params, "product_catalog",
    ["product_id", "product_name", "product_slug", "category", "base_price", "popularity_weight"],
    rows,
)
print(f"  Synced {n} products in {time.time()-t0:.1f}s")
```

**Cell 9: Sync mba_recommendations**
```python
import json

print("Syncing mba_recommendations...")
t0 = time.time()

df = spark.read.table(f"{catalog}.{schema}.mba_recommendations").toPandas()
rows = [
    (r.product_slug, r.recommendations if isinstance(r.recommendations, str) else json.dumps(r.recommendations))
    for _, r in df.iterrows()
]
n = sync_table(conn_params, "mba_recommendations", ["product_slug", "recommendations"], rows)
print(f"  Synced {n} MBA recommendations in {time.time()-t0:.1f}s")
```

**Cell 10: Sync als_recommendations**
```python
print("Syncing als_recommendations...")
t0 = time.time()

df = spark.read.table(f"{catalog}.{schema}.als_recommendations").toPandas()
rows = [
    (r.user_id, r.recommendations if isinstance(r.recommendations, str) else json.dumps(r.recommendations))
    for _, r in df.iterrows()
]
n = sync_table(conn_params, "als_recommendations", ["user_id", "recommendations"], rows)
print(f"  Synced {n} ALS recommendations in {time.time()-t0:.1f}s")
```

**Cell 11: Sync user_profiles**
```python
print("Syncing user_profiles...")
t0 = time.time()

df = spark.read.table(f"{catalog}.{schema}.user_profiles").toPandas()
rows = [
    (r.user_id, r.primary_store, int(r.store_visits), int(r.total_orders))
    for _, r in df.iterrows()
]
n = sync_table(conn_params, "user_profiles", ["user_id", "primary_store", "store_visits", "total_orders"], rows)
print(f"  Synced {n} user profiles in {time.time()-t0:.1f}s")
```

**Cell 12: Verify counts**
```python
print("\n=== Lakebase Sync Complete ===")
conn = psycopg2.connect(**conn_params)
cur = conn.cursor()

for table in ["product_catalog", "mba_recommendations", "als_recommendations", "user_profiles"]:
    cur.execute(f"SELECT COUNT(*) FROM {table}")
    count = cur.fetchone()[0]
    print(f"  {table}: {count:,} rows")

cur.close()
conn.close()
print("\nAll tables synced. App is ready to serve.")
```

- [ ] **Step 2: Commit**

```bash
git add notebooks/04_lakebase_sync.py
git commit -m "feat: add notebook 04 for automated Lakebase table creation and sync"
```

---

## Task 3: Update databricks.yml with lakebase_sync task

**Files:**
- Modify: `databricks.yml`

- [ ] **Step 1: Add lakebase_sync task to the pipeline**

Add after the `collaborative_filter` task in the `recommender_training_pipeline` job:

```yaml
        - task_key: lakebase_sync
          notebook_task:
            notebook_path: ./notebooks/04_lakebase_sync.py
          depends_on:
            - task_key: market_basket
            - task_key: collaborative_filter
```

This task runs on serverless (no `new_cluster` block) and depends on both model notebooks.

- [ ] **Step 2: Commit**

```bash
git add databricks.yml
git commit -m "feat: add lakebase_sync task to training pipeline"
```

---

## Task 4: Update README

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Update Quick Start and pipeline sections**

Changes needed:
1. In Quick Start step 3: update notebook count from 4 to 5, mention Lakebase sync is automated
2. Replace the manual "Set up Lakebase" section (step 4) with a simpler note that the pipeline handles table creation and data loading automatically. Keep the Lakebase instance creation CLI command and the SP grant commands (those are still pre-requisites).
3. In the "What Gets Deployed" table: update pipeline description from "4-notebook" to "5-notebook"
4. In "Configuration" table: add `lakebase_instance` row
5. In "Pipeline Runtime" table: add Lakebase Sync row (~1 min, serverless)
6. In "Architecture" diagram: add lakebase_sync step

- [ ] **Step 2: Commit and push**

```bash
git add README.md
git commit -m "docs: update README for automated Lakebase sync"
git push
```
