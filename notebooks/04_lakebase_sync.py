# Databricks notebook source
# MAGIC %pip install psycopg2-binary pyyaml "databricks-sdk>=0.81.0"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC # 04 - Lakebase Sync
# MAGIC Creates PostgreSQL tables in Lakebase and bulk-loads data from Delta.
# MAGIC Generates `user_profiles` from raw order data.
# MAGIC
# MAGIC **Compute**: Serverless compatible.
# MAGIC
# MAGIC **Depends on**: Notebooks 00-03 must have run first (Delta tables must exist).

# COMMAND ----------

# MAGIC %run ./config_loader

# COMMAND ----------

# DBTITLE 1,Load config and connect to Lakebase
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

# COMMAND ----------

# DBTITLE 1,Create Lakebase tables
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

# COMMAND ----------

# DBTITLE 1,Generate user_profiles from raw_orders
from pyspark.sql.functions import col, count, desc
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

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

# COMMAND ----------

# DBTITLE 1,Bulk load helper
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

# COMMAND ----------

# DBTITLE 1,Sync product_catalog
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

# COMMAND ----------

# DBTITLE 1,Sync mba_recommendations
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

# COMMAND ----------

# DBTITLE 1,Sync als_recommendations
print("Syncing als_recommendations...")
t0 = time.time()

df = spark.read.table(f"{catalog}.{schema}.als_recommendations").toPandas()
rows = [
    (r.user_id, r.recommendations if isinstance(r.recommendations, str) else json.dumps(r.recommendations))
    for _, r in df.iterrows()
]
n = sync_table(conn_params, "als_recommendations", ["user_id", "recommendations"], rows)
print(f"  Synced {n} ALS recommendations in {time.time()-t0:.1f}s")

# COMMAND ----------

# DBTITLE 1,Sync user_profiles
print("Syncing user_profiles...")
t0 = time.time()

df = spark.read.table(f"{catalog}.{schema}.user_profiles").toPandas()
rows = [
    (r.user_id, r.primary_store, int(r.store_visits), int(r.total_orders))
    for _, r in df.iterrows()
]
n = sync_table(conn_params, "user_profiles", ["user_id", "primary_store", "store_visits", "total_orders"], rows)
print(f"  Synced {n} user profiles in {time.time()-t0:.1f}s")

# COMMAND ----------

# DBTITLE 1,Grant app service principal access to tables
app_name = cfg.get("app_name", "recommender-accelerator")
target = dbutils.widgets.get("__bundle_target") if "__bundle_target" in [w.name for w in dbutils.widgets.getAll()] else "dev"
full_app_name = f"{app_name}-{target}"

print(f"Looking up service principal for app: {full_app_name}")
try:
    app_info = w.apps.get(full_app_name)
    sp_id = app_info.service_principal_id
    # Lakebase roles use the SP's application_id (UUID), not the display name
    sp = w.service_principals.get(sp_id)
    sp_role = sp.application_id
    print(f"  Found SP: {sp.display_name} (application_id={sp_role})")

    conn = psycopg2.connect(**conn_params)
    cur = conn.cursor()
    tables = ["product_catalog", "mba_recommendations", "als_recommendations", "user_profiles"]
    for table in tables:
        cur.execute(f'GRANT SELECT ON {table} TO "{sp_role}"')
        print(f"  Granted SELECT on {table}")
    conn.commit()
    cur.close()
    conn.close()
    print("All grants applied")
except Exception as e:
    print(f"  Warning: Could not grant SP access: {e}")
    print("  You may need to run GRANT SELECT manually (see README)")

# COMMAND ----------

# DBTITLE 1,Verify Lakebase sync
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
