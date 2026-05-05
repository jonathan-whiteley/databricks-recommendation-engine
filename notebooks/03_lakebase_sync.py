# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Lakebase Sync (LCE Bridge)
# MAGIC
# MAGIC Reads LCE Delta tables produced by notebooks 00-02 (`ioc_sandbox.ai_strategy`) and bulk-loads
# MAGIC four Postgres tables in the Lakebase instance configured in `config.yaml`. These tables are
# MAGIC the exact schema the FastAPI app reads from (`product_catalog`, `als_recommendations`,
# MAGIC `mba_recommendations`, `user_profiles`).
# MAGIC
# MAGIC **Source tables (Unity Catalog)**
# MAGIC | Table | Produced by |
# MAGIC |---|---|
# MAGIC | `ioc_sandbox.ai_strategy.cleaned_mapped_dataset` | 00_lce_data_preparation |
# MAGIC | `ioc_sandbox.ai_strategy.association_rules` | 01_lce_market_basket |
# MAGIC | `ioc_sandbox.ai_strategy.als_recommendations` | 02_lce_collaborative_filter |
# MAGIC
# MAGIC **Target tables (Lakebase Postgres)**
# MAGIC | Table | Key |
# MAGIC |---|---|
# MAGIC | `product_catalog` | `product_id` TEXT PRIMARY KEY |
# MAGIC | `als_recommendations` | `user_id` TEXT PRIMARY KEY |
# MAGIC | `mba_recommendations` | `product_slug` TEXT PRIMARY KEY |
# MAGIC | `user_profiles` | `user_id` TEXT PRIMARY KEY |
# MAGIC
# MAGIC **Compute**: Serverless compatible.
# MAGIC
# MAGIC **Idempotent**: each sync truncates before inserting — running twice produces the same state.

# COMMAND ----------

# MAGIC %pip install psycopg2-binary pyyaml "databricks-sdk>=0.81.0"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Inline config loader
import os
import yaml


def load_config() -> dict:
    """Load config.yaml from the repo root. Tries multiple path candidates to work
    both when run interactively (cwd may be the repo root) and when executed as a
    DABS job task (cwd is set to the bundle root by the Databricks runtime)."""
    candidates = [
        os.path.join(os.getcwd(), "config.yaml"),
        os.path.join(os.getcwd(), "..", "config.yaml"),
        "../config.yaml",
        "config.yaml",
    ]
    for path in candidates:
        try:
            resolved = os.path.realpath(path)
            if os.path.exists(resolved):
                with open(resolved) as f:
                    return yaml.safe_load(f)
        except Exception:
            continue
    raise FileNotFoundError(
        "config.yaml not found. Searched: " + ", ".join(candidates)
    )


cfg = load_config()
print(f"Config loaded — lakebase_instance: {cfg['lakebase_instance']}, app_name: {cfg.get('app_name', 'recommender-accelerator')}")

# COMMAND ----------

# DBTITLE 1,LCE source table references
# These are hardcoded because the LCE notebooks themselves hardcode them.
# The 00/01/02 notebooks always write to ioc_sandbox.ai_strategy.
LCE_CATALOG = "ioc_sandbox"
LCE_SCHEMA  = "ai_strategy"

CLEANED_TABLE  = f"{LCE_CATALOG}.{LCE_SCHEMA}.cleaned_mapped_dataset"
RULES_TABLE    = f"{LCE_CATALOG}.{LCE_SCHEMA}.association_rules"
ALS_RECS_TABLE = f"{LCE_CATALOG}.{LCE_SCHEMA}.als_recommendations"

print(f"Source tables:")
print(f"  {CLEANED_TABLE}")
print(f"  {RULES_TABLE}")
print(f"  {ALS_RECS_TABLE}")

# COMMAND ----------

# DBTITLE 1,Connect to Lakebase
import time
import uuid
import psycopg2
from databricks.sdk import WorkspaceClient

lakebase_instance = cfg["lakebase_instance"]

ws = WorkspaceClient()
instance = ws.database.get_database_instance(name=lakebase_instance)
cred = ws.database.generate_database_credential(
    request_id=str(uuid.uuid4()),
    instance_names=[lakebase_instance],
)

conn_params = {
    "host": instance.read_write_dns,
    "port": 5432,
    "dbname": "databricks_postgres",
    "user": ws.current_user.me().user_name,
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

print(f"Instance:  {lakebase_instance}")
print(f"Host:      {instance.read_write_dns}")

# COMMAND ----------

# DBTITLE 1,Create Lakebase tables (DDL)
conn = psycopg2.connect(**conn_params)
cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS product_catalog (
    product_id         TEXT PRIMARY KEY,
    product_name       TEXT,
    product_slug       TEXT,
    category           TEXT,
    base_price         DOUBLE PRECISION,
    popularity_weight  DOUBLE PRECISION
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS als_recommendations (
    user_id         TEXT PRIMARY KEY,
    recommendations JSONB
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS mba_recommendations (
    product_slug    TEXT PRIMARY KEY,
    recommendations JSONB
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS user_profiles (
    user_id       TEXT PRIMARY KEY,
    primary_store TEXT,
    store_visits  INTEGER,
    total_orders  INTEGER
)
""")

conn.commit()
cur.close()
conn.close()
print("All 4 Lakebase tables created (or already exist)")

# COMMAND ----------

# DBTITLE 1,Slugify helper
import re


def slugify(name: str) -> str:
    """Stable slug: lowercase, replace every non-alphanumeric character with '-'.

    Examples:
      'Classic Pepperoni' -> 'classic-pepperoni'
      'Crazy Bread®'      -> 'crazy-bread-'
      'Pepsi'             -> 'pepsi'

    The trailing '-' on names ending with a special character (e.g. ®) is kept
    intentionally so the function is injective — two distinct names never produce
    the same slug unless they differ only in casing or whitespace.
    """
    return re.sub(r"[^a-z0-9]+", "-", name.lower())


# Spot-check
assert slugify("Classic Pepperoni") == "classic-pepperoni", slugify("Classic Pepperoni")
assert slugify("Crazy Bread®")      == "crazy-bread-",      slugify("Crazy Bread®")
print("slugify spot-checks passed")

# COMMAND ----------

# DBTITLE 1,Category / price heuristic helper
def categorize(name: str):
    """Return (category, base_price) based on keyword matching on the lowercased name.
    Rules are checked in order; first match wins.
    """
    n = name.lower()
    if "pizza" in n or n.startswith("classic "):
        return "Pizza", 9.99
    if "bread" in n or "wings" in n or "crazy" in n:
        return "Sides", 4.99
    if any(k in n for k in ("pepsi", "dew", "fanta", "coke", "drink")):
        return "Drinks", 2.49
    if any(k in n for k in ("cookie", "brownie", "cinnamon", "cake")):
        return "Desserts", 5.99
    return "Other", 4.99

# COMMAND ----------

# DBTITLE 1,Build product_catalog DataFrame
from pyspark.sql.functions import (
    col, explode, split, trim, transform, array_distinct,
    count as spark_count, countDistinct,
    udf, lit, round as spark_round,
)
from pyspark.sql.types import StringType, DoubleType, StructType, StructField

print("Building product_catalog...")
t0 = time.time()

cleaned = spark.read.table(CLEANED_TABLE)

# Total order count (denominator for popularity_weight)
total_orders = cleaned.count()
print(f"  Total orders in cleaned_mapped_dataset: {total_orders:,}")

# Explode ItemNames (comma-separated string) into individual items
items_sdf = (
    cleaned
    .withColumn(
        "item",
        explode(
            array_distinct(
                transform(split(col("ItemNames"), ","), lambda x: trim(x))
            )
        )
    )
    .filter(col("item") != "")
    .filter(col("item").isNotNull())
)

# Count orders containing each item (for popularity_weight = support)
item_order_counts = (
    items_sdf
    .groupBy("item")
    .agg(spark_count("*").alias("order_count"))
)

# Build final product_catalog as a pandas DataFrame (small enough)
item_counts_pd = item_order_counts.toPandas()

product_rows = []
for _, row in item_counts_pd.iterrows():
    name = row["item"]
    slug = slugify(name)
    category, base_price = categorize(name)
    popularity = round(row["order_count"] / total_orders, 4)
    product_rows.append((slug, name, slug, category, base_price, popularity))

# Deduplicate by product_id (slug) — keep the row with the highest popularity
# in the unlikely event two distinct names slug to the same value
seen = {}
for row in product_rows:
    pid = row[0]
    if pid not in seen or row[5] > seen[pid][5]:
        seen[pid] = row

product_rows_deduped = list(seen.values())
print(f"  {len(product_rows_deduped)} unique products built in {time.time()-t0:.1f}s")
# Quick preview
for r in sorted(product_rows_deduped, key=lambda x: x[4], reverse=True)[:5]:
    print(f"    {r[1]} -> slug={r[0]}, cat={r[3]}, price={r[4]}, pop={r[5]}")

# COMMAND ----------

# DBTITLE 1,Build user_profiles DataFrame
print("Building user_profiles...")
t0 = time.time()

from pyspark.sql.functions import countDistinct

user_profiles_sdf = (
    cleaned
    .groupBy("EmailAddress")
    .agg(countDistinct("CVOrderID").alias("total_orders"))
    .select(
        col("EmailAddress").alias("user_id"),
        lit(None).cast("string").alias("primary_store"),   # LCE data has no store info
        lit(None).cast("int").alias("store_visits"),       # LCE data has no store info
        col("total_orders"),
    )
)

user_profiles_pd = user_profiles_sdf.toPandas()
user_profile_rows = [
    (r.user_id, r.primary_store, r.store_visits, int(r.total_orders))
    for _, r in user_profiles_pd.iterrows()
]
print(f"  {len(user_profile_rows):,} user profiles built in {time.time()-t0:.1f}s")

# COMMAND ----------

# DBTITLE 1,Build als_recommendations DataFrame
import json

print("Building als_recommendations...")
t0 = time.time()

# Source schema: EmailAddress STRING, recommendations ARRAY<STRING>, scores ARRAY<DOUBLE>
als_src = spark.read.table(ALS_RECS_TABLE)
als_pd  = als_src.toPandas()

als_rows = []
for _, row in als_pd.iterrows():
    recs  = list(row["recommendations"]) if row["recommendations"] is not None else []
    scores = list(row["scores"])         if row["scores"]          is not None else []
    # Zip into [{product: ..., score: ...}, ...] preserving ranking order (no truncation)
    recs_json = json.dumps([
        {"product": p, "score": round(float(s), 6)}
        for p, s in zip(recs, scores)
    ])
    als_rows.append((str(row["EmailAddress"]), recs_json))

print(f"  {len(als_rows):,} ALS user recommendation sets built in {time.time()-t0:.1f}s")

# COMMAND ----------

# DBTITLE 1,Build mba_recommendations DataFrame (single-item antecedent strategy)
from pyspark.sql.functions import size as spark_size

print("Building mba_recommendations...")
t0 = time.time()

# Source schema: antecedent ARRAY<STRING>, consequent ARRAY<STRING>, lift DOUBLE, confidence DOUBLE
rules_src = spark.read.table(RULES_TABLE)

# Filter to single-item antecedents only (multi-item extension: see markdown cell below)
single_rules = (
    rules_src
    .filter(spark_size(col("antecedent")) == 1)
    .selectExpr(
        "antecedent[0] AS antecedent_item",
        "consequent[0] AS consequent_item",  # FPGrowth produces single-item consequents
        "confidence",
    )
    .filter(col("antecedent_item").isNotNull())
    .filter(col("consequent_item").isNotNull())
)

single_rules_pd = single_rules.toPandas()

# Group by antecedent, collect consequents ordered by confidence DESC, cap at 20
from collections import defaultdict

antecedent_groups = defaultdict(list)
for _, row in single_rules_pd.iterrows():
    antecedent_groups[row["antecedent_item"]].append(
        (row["consequent_item"], float(row["confidence"]))
    )

mba_rows = []
for antecedent_item, consequents in antecedent_groups.items():
    # Sort by confidence descending, cap at 20
    consequents_sorted = sorted(consequents, key=lambda x: x[1], reverse=True)[:20]
    recs_json = json.dumps([
        {"consequent": slugify(c), "rule_score": round(s, 6)}
        for c, s in consequents_sorted
    ])
    mba_rows.append((slugify(antecedent_item), recs_json))

print(f"  {len(mba_rows):,} MBA antecedent slugs built in {time.time()-t0:.1f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Future: multi-item antecedent recommendations
# MAGIC
# MAGIC The FPGrowth model in `01_lce_market_basket` generates rules for **all antecedent sizes**,
# MAGIC not just single-item ones. Those multi-item rules are stored in
# MAGIC `ioc_sandbox.ai_strategy.association_rules` with `size(antecedent) > 1` and are currently
# MAGIC unused by this sync notebook.
# MAGIC
# MAGIC **Why they were left out now:** the FastAPI app's `/api/recommend` endpoint (in
# MAGIC `recommend.py`) currently does a per-slug point-lookup:
# MAGIC ```python
# MAGIC WHERE product_slug IN ({placeholders})   -- one slug per cart item
# MAGIC ```
# MAGIC This maps cleanly to the single-antecedent strategy (one Postgres row per cart item).
# MAGIC Multi-item antecedents like `["Classic Pepperoni", "Crazy Bread®"]` don't have a single
# MAGIC slug key — they'd require a composite key or a server-side cross-join.
# MAGIC
# MAGIC **Extension path (two options):**
# MAGIC
# MAGIC 1. **Server-side cross-join (no schema change):** modify `_get_mba_recs()` in `recommend.py`
# MAGIC    to fetch *all* rules (or rules for the cart's items) and perform a Python-side cross-join
# MAGIC    against the full cart — the same logic used in `MBARecommenderModel.predict()` in
# MAGIC    `01_lce_market_basket.py`. This avoids any Postgres schema change but increases per-request
# MAGIC    data transfer as the rules set grows.
# MAGIC
# MAGIC 2. **Pre-computed cart-combo rollups (schema change):** add a second Postgres table
# MAGIC    `mba_recommendations_multi` keyed on a sorted, pipe-joined combo string
# MAGIC    (e.g. `"classic-pepperoni|crazy-bread-"`) and write one row per unique multi-item
# MAGIC    antecedent found in the rules. The app would need to generate the combo key from the
# MAGIC    cart and look it up. Scales well at serve time but requires a write-time cartesian
# MAGIC    explosion of antecedent combos.
# MAGIC
# MAGIC The full multi-item rules remain in `ioc_sandbox.ai_strategy.association_rules` and can be
# MAGIC queried at any time — no data is lost by the current single-item filter.

# COMMAND ----------

# DBTITLE 1,Bulk-load helper
from psycopg2.extras import execute_values


def sync_table(conn_params, table_name, columns, rows, batch_size=5000):
    """Truncate and bulk-insert rows into a Lakebase Postgres table.

    Idempotent: TRUNCATE before INSERT ensures running twice produces the same state.
    Uses psycopg2.extras.execute_values for efficient batch inserts.
    """
    conn = psycopg2.connect(**conn_params)
    cur  = conn.cursor()

    cur.execute(f"TRUNCATE TABLE {table_name}")

    cols_str = ", ".join(columns)
    template = "(" + ", ".join(["%s"] * len(columns)) + ")"

    total = len(rows)
    for i in range(0, total, batch_size):
        batch = rows[i : i + batch_size]
        execute_values(
            cur,
            f"INSERT INTO {table_name} ({cols_str}) VALUES %s",
            batch,
            template=template,
        )

    conn.commit()
    cur.close()
    conn.close()
    return total

# COMMAND ----------

# DBTITLE 1,Sync product_catalog
print("Syncing product_catalog...")
t0 = time.time()
n = sync_table(
    conn_params,
    "product_catalog",
    ["product_id", "product_name", "product_slug", "category", "base_price", "popularity_weight"],
    product_rows_deduped,
)
print(f"  Synced {n} products in {time.time()-t0:.1f}s")

# COMMAND ----------

# DBTITLE 1,Sync als_recommendations
print("Syncing als_recommendations...")
t0 = time.time()
n = sync_table(
    conn_params,
    "als_recommendations",
    ["user_id", "recommendations"],
    als_rows,
)
print(f"  Synced {n} ALS user recommendation sets in {time.time()-t0:.1f}s")

# COMMAND ----------

# DBTITLE 1,Sync mba_recommendations
print("Syncing mba_recommendations...")
t0 = time.time()
n = sync_table(
    conn_params,
    "mba_recommendations",
    ["product_slug", "recommendations"],
    mba_rows,
)
print(f"  Synced {n} MBA antecedent slugs in {time.time()-t0:.1f}s")

# COMMAND ----------

# DBTITLE 1,Sync user_profiles
print("Syncing user_profiles...")
t0 = time.time()
n = sync_table(
    conn_params,
    "user_profiles",
    ["user_id", "primary_store", "store_visits", "total_orders"],
    user_profile_rows,
)
print(f"  Synced {n} user profiles in {time.time()-t0:.1f}s")

# COMMAND ----------

# DBTITLE 1,Grant app service principal SELECT on all tables
app_name = cfg.get("app_name", "recommender-accelerator")

# Resolve DABS target (dev/staging/prod) from widget if set, else default to "dev"
try:
    target = dbutils.widgets.get("__bundle_target")
except Exception:
    target = "dev"

full_app_name = f"{app_name}-{target}"
print(f"Looking up service principal for app: {full_app_name}")

try:
    app_info = ws.apps.get(full_app_name)
    sp_id    = app_info.service_principal_id
    # Lakebase roles use the SP application_id (UUID), not the integer sp_id
    sp        = ws.service_principals.get(sp_id)
    sp_role   = sp.application_id
    print(f"  Found SP: {sp.display_name} (application_id={sp_role})")

    conn = psycopg2.connect(**conn_params)
    cur  = conn.cursor()
    tables = ["product_catalog", "als_recommendations", "mba_recommendations", "user_profiles"]
    for table in tables:
        cur.execute(f'GRANT SELECT ON {table} TO "{sp_role}"')
        print(f"  Granted SELECT on {table} to {sp_role}")
    conn.commit()
    cur.close()
    conn.close()
    print("All grants applied.")
except Exception as e:
    print(f"  Warning: could not grant SP access — {e}")
    print("  The app may not be deployed yet (first run). Re-run this cell after deploying the app,")
    print("  or grant SELECT manually: GRANT SELECT ON <table> TO \"<sp_application_id>\"")

# COMMAND ----------

# DBTITLE 1,Verify — row counts for all 4 tables
print("\n=== Lakebase Sync Complete ===")
conn = psycopg2.connect(**conn_params)
cur  = conn.cursor()

for table in ["product_catalog", "als_recommendations", "mba_recommendations", "user_profiles"]:
    cur.execute(f"SELECT COUNT(*) FROM {table}")
    cnt = cur.fetchone()[0]
    print(f"  {table}: {cnt:,} rows")

cur.close()
conn.close()
print("\nAll tables synced. App is ready to serve.")
