# Databricks notebook source
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # Data Preparation for Recommender System
# MAGIC This notebook prepares order transaction data for two downstream recommender models:
# MAGIC * **01_market_basket_analysis** — FPGrowth-based association rules
# MAGIC * **02_collaborative_filter** — ALS-based user-item recommendations
# MAGIC
# MAGIC ### Data Sources
# MAGIC | Section | Source | Description |
# MAGIC |---|---|---|
# MAGIC | Cells 3–6 | Synthetic | Generates 15K fake orders for local testing |
# MAGIC | Cells 8–9 | **Production** | Pulls 3 months of real order data from `ioc.lce_base_mobile` |
# MAGIC
# MAGIC ### Compute Requirements
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Runtime** | DBR 17.3 ML |
# MAGIC | **Compute** | Classic cluster |
# MAGIC
# MAGIC ### Output Tables
# MAGIC | Table | Description |
# MAGIC |---|---|
# MAGIC | `cleaned_mapped_dataset` | Full cleaned dataset (all orders) |
# MAGIC | `train_dataset` | Temporal training split (Jan 1 – Mar 15, 2026) |
# MAGIC | `test_dataset` | Temporal test split (Mar 16 – Mar 31, 2026) — shared across both downstream notebooks |
# MAGIC
# MAGIC ### Why Temporal Split?
# MAGIC The original random order-level split sent 98.3 % of single-order users entirely into one split,
# MAGIC leaving ALS with only ~1,300 evaluable test orders. A **temporal split** ensures that repeat
# MAGIC customers who ordered in both periods appear in both train and test, enabling meaningful Hit@k evaluation.

# COMMAND ----------

# DBTITLE 1,Configuration
catalog = 'ioc_sandbox'
schema = 'ai_strategy'

#catalog = 'users'
#schema = 'jon_cheung'

# Output table names
cleaned_table = f'{catalog}.{schema}.cleaned_mapped_dataset'
train_table   = f'{catalog}.{schema}.train_dataset'
test_table    = f'{catalog}.{schema}.test_dataset'

# Ensure the schema exists
spark.sql(f'CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}')

# COMMAND ----------

# DBTITLE 1,Generate synthetic order data
import random
from pyspark.sql.types import StructType, StructField, StringType

random.seed(42)

# ---------------------
# Product catalog
# ---------------------
pizzas = [
    'pepperoni-pizza', 'cheese-pizza', 'supreme-pizza', 'meat-lovers-pizza',
    'veggie-pizza', 'hawaiian-pizza', 'bbq-chicken-pizza', 'deep-dish-pepperoni',
]
sides = ['crazy-bread', 'italian-cheese-bread', 'caesar-wings', 'stuffed-crazy-bread']
drinks = ['pepsi', 'mountain-dew', 'diet-pepsi', 'orange-fanta']
desserts = ['cookie-dough-brownie', 'cinnamon-loaded-crazy-bites']

# Items that should NOT be recommended (to be cleaned out)
sauce_list = ['marinara-sauce', 'ranch-dressing']
utensil_list = ['plastic-fork', 'plastic-knife']

# ---------------------
# Generate transactions
# ---------------------
num_users = 2_000
num_orders = 15_000
user_ids = [str(random.randint(10_000_000, 99_999_999)) for _ in range(num_users)]

orders = []
for i in range(num_orders):
    mpid = random.choice(user_ids)
    order_id = f'ORD-{i+1:06d}'

    # Every order has at least 1 pizza
    items = random.sample(pizzas, random.randint(1, 3))

    # Sides (~60 %), drinks (~50 %), desserts (~20 %)
    if random.random() < 0.60:
        items.append(random.choice(sides))
    if random.random() < 0.50:
        items.append(random.choice(drinks))
    if random.random() < 0.20:
        items.append(random.choice(desserts))

    # Sprinkle in items we will later clean out
    if random.random() < 0.35:
        items.append(random.choice(sauce_list))
    if random.random() < 0.25:
        items.append(random.choice(utensil_list))

    orders.append((mpid, order_id, ', '.join(items)))

# Build Spark DataFrame (mimics a raw orders table)
raw_schema = StructType([
    StructField('mpid', StringType()),
    StructField('order_id', StringType()),
    StructField('order_products', StringType()),   # comma-separated product names
])
sdf_raw = spark.createDataFrame(orders, raw_schema)
print(f'Generated {sdf_raw.count():,} raw orders')
display(sdf_raw.limit(10))

# COMMAND ----------

# DBTITLE 1,Cleaning section
# MAGIC %md
# MAGIC ## Data Cleaning
# MAGIC Remove items we should never recommend — condiment add-ons (sauces) and disposable utensils — then drop any orders that become empty after filtering.

# COMMAND ----------

# DBTITLE 1,Parse and clean order products
from pyspark.sql.functions import col, expr

# 1. Split comma-separated string into an array of unique, trimmed product slugs
sdf_prepared = sdf_raw.withColumn(
    'order_product_list',
    expr("array_distinct(transform(split(order_products, ','), x -> trim(x)))")
)

# 2. Define items to remove
sauce_list  = ['marinara-sauce', 'ranch-dressing']
utensil_list = ['plastic-fork', 'plastic-knife']
items_to_remove = sauce_list + utensil_list

# 3. Filter out unwanted items from each order
remove_expr = ', '.join([f"'{item}'" for item in items_to_remove])
sdf_cleaned = sdf_prepared.withColumn(
    'order_product_list',
    expr(f"filter(order_product_list, x -> NOT array_contains(array({remove_expr}), x))")
)

# 4. Drop empty strings and orders with no remaining products
sdf_cleaned = sdf_cleaned.withColumn(
    'order_product_list',
    expr("filter(order_product_list, x -> x != '')")
)
sdf_cleaned_mapped = (
    sdf_cleaned
    .filter(expr('size(order_product_list) > 0'))
    .select('mpid', 'order_id', 'order_product_list')
)

print(f'Cleaned dataset: {sdf_cleaned_mapped.count():,} orders')
display(sdf_cleaned_mapped.limit(10))

# COMMAND ----------

# DBTITLE 1,Save cleaned dataset and test split to Unity Catalog
# Save the full cleaned dataset (used by both 01_MBA and 02_collab_filter)
sdf_cleaned_mapped.write.format('delta').mode('overwrite') \
    .option('overwriteSchema', 'true') \
    .saveAsTable(cleaned_table)

# 80/20 train-test split — both downstream notebooks load these directly
# so they evaluate against the exact same held-out set
train, test = sdf_cleaned_mapped.randomSplit([0.8, 0.2], seed=42)

train.write.format('delta').mode('overwrite') \
    .option('overwriteSchema', 'true') \
    .saveAsTable(train_table)

test.write.format('delta').mode('overwrite') \
    .option('overwriteSchema', 'true') \
    .saveAsTable(test_table)

print(f'Saved to {catalog}.{schema}:')
print(f'  {cleaned_table}  {sdf_cleaned_mapped.count():,} orders')
print(f'  {train_table}           {train.count():,} orders')
print(f'  {test_table}            {test.count():,} orders')

# COMMAND ----------

# DBTITLE 1,Production data pipeline
# MAGIC %md
# MAGIC ## Production Data — Multi-Month Order History
# MAGIC Pull 3 months of real order data from `ioc.lce_base_mobile` to give collaborative filtering
# MAGIC enough repeat-purchase signal. A single-day snapshot yields ~98 % one-time buyers, making
# MAGIC ALS evaluation nearly impossible.
# MAGIC
# MAGIC ### Temporal Train / Test Split
# MAGIC Instead of a random order-level split (which separates users rather than orders), we use a
# MAGIC **temporal split** that mirrors production reality — train on the past, predict the future:
# MAGIC
# MAGIC | Split | Date range | Purpose |
# MAGIC |---|---|---|
# MAGIC | **Train** | Jan 1 – Mar 15, 2026 | Model training (ALS + MBA) |
# MAGIC | **Test** | Mar 16 – Mar 31, 2026 | Held-out evaluation (~80/20) |
# MAGIC
# MAGIC Users who ordered in both periods are "warm" and evaluable by ALS.
# MAGIC The MBA notebook can still evaluate on any test order regardless of user overlap.

# COMMAND ----------

# DBTITLE 1,Pull 3-month order history from production
# MAGIC %sql
# MAGIC -- Read pre-staged 3-month production data (Jan–Mar 2026)
# MAGIC -- Staged from ioc.lce_base_mobile.orderdetails + orderitems via serverless compute
# MAGIC -- to bypass cluster permission restrictions on the source tables.
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW raw_orders AS
# MAGIC SELECT *
# MAGIC FROM ioc_sandbox.ai_strategy.raw_orders_staged

# COMMAND ----------

# DBTITLE 1,Clean production data, temporal split, and save
from pyspark.sql.functions import col, countDistinct, count

# ------------------------------------------------------------------
# 1. Load and clean production data
# ------------------------------------------------------------------
sdf_production = spark.table('raw_orders')

sdf_clean = (
    sdf_production
    .filter(col('EmailAddress').isNotNull())
    .filter(col('EmailAddress') != '')
    .filter(col('ItemNames').isNotNull())
    .filter(col('ItemNames') != '')
)

print(f'Production data: {sdf_clean.count():,} orders  '
      f'({sdf_clean.select("EmailAddress").distinct().count():,} unique users)')

# ------------------------------------------------------------------
# 2. Temporal train / test split
#    Train: Jan 1 – Mar 15   (~80 %)
#    Test:  Mar 16 – Mar 31  (~20 %)
# ------------------------------------------------------------------
cutoff_date = '2026-03-16'

output_cols = ['EmailAddress', 'CVOrderID', 'ItemNames']

train = sdf_clean.filter(col('BusinessDate') < cutoff_date).select(*output_cols)
test  = sdf_clean.filter(col('BusinessDate') >= cutoff_date).select(*output_cols)

# ------------------------------------------------------------------
# 3. Save to Unity Catalog (same tables as before)
# ------------------------------------------------------------------
sdf_clean.select(*output_cols).write.format('delta').mode('overwrite') \
    .option('overwriteSchema', 'true').saveAsTable(cleaned_table)

train.write.format('delta').mode('overwrite') \
    .option('overwriteSchema', 'true').saveAsTable(train_table)

test.write.format('delta').mode('overwrite') \
    .option('overwriteSchema', 'true').saveAsTable(test_table)

print(f'\nSaved to {catalog}.{schema}:')
print(f'  {cleaned_table}  {sdf_clean.count():,} orders')
print(f'  {train_table}           {train.count():,} orders')
print(f'  {test_table}            {test.count():,} orders')

# ------------------------------------------------------------------
# 4. Verify user overlap (critical for ALS evaluation)
# ------------------------------------------------------------------
train_users = train.select('EmailAddress').distinct()
test_users  = test.select('EmailAddress').distinct()
overlap     = train_users.join(test_users, 'EmailAddress', 'inner').count()

print(f'\nUser overlap: {overlap:,} users appear in both train and test')
print(f'  Train users: {train_users.count():,}')
print(f'  Test users:  {test_users.count():,}')
print(f'  Overlap %%:  {100 * overlap / test_users.count():.1f} %% of test users are warm')
