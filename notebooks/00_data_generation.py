# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Synthetic Data Generation
# MAGIC Generates realistic transaction data from the configured vertical template.
# MAGIC Outputs: `raw_orders` and `product_catalog` Delta tables.
# MAGIC
# MAGIC **Compute**: Serverless compatible.

# COMMAND ----------

# MAGIC %run ./config_loader

# COMMAND ----------

# DBTITLE 1,Load config and vertical template
cfg = load_config()

import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) if "__file__" in dir() else ".")
from verticals import get_vertical

vertical = get_vertical(cfg["vertical"])
catalog = cfg["catalog"]
schema = cfg["schema"]
seed = cfg.get("seed", 42)
order_count = cfg.get("order_count", 500000)
user_count = cfg.get("user_count", 10000)
store_count = cfg.get("store_count", 50)

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

print(f"Vertical: {cfg['vertical']} | Catalog: {catalog}.{schema}")
print(f"Generating {order_count:,} orders for {user_count:,} users across {store_count} stores")

# COMMAND ----------

# DBTITLE 1,Write product catalog table
from pyspark.sql import Row

product_rows = [
    Row(
        product_id=f"prod_{i:04d}",
        product_name=p["name"],
        product_slug=p["slug"],
        category=p["category"],
        base_price=float(p["base_price"]),
        popularity_weight=float(p["popularity_weight"]),
    )
    for i, p in enumerate(vertical["products"])
]

product_catalog_sdf = spark.createDataFrame(product_rows)
product_catalog_sdf.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{catalog}.{schema}.product_catalog"
)

print(f"Wrote {len(product_rows)} products to {catalog}.{schema}.product_catalog")
product_catalog_sdf.display()

# COMMAND ----------

# DBTITLE 1,Generate synthetic orders
import random
import uuid
from datetime import datetime, timedelta
from pyspark.sql.types import StructType, StructField, StringType, DateType, ArrayType, FloatType

random.seed(seed)

products = vertical["products"]
categories = vertical["categories"]
affinities = vertical["affinities"]

# Build popularity-weighted product list for initial item selection
weighted_products = []
for p in products:
    weighted_products.extend([p] * p["popularity_weight"])

# Pre-index products by category for affinity lookups
products_by_category = {}
for p in products:
    products_by_category.setdefault(p["category"], []).append(p)


def generate_basket() -> list:
    """Generate a single order basket using affinity-driven co-purchase logic."""
    # Pick first item weighted by popularity
    first_item = random.choice(weighted_products)
    basket = [first_item["slug"]]
    seen_slugs = {first_item["slug"]}

    # Determine basket size (1-8 items, weighted toward 2-4)
    basket_size = min(random.choices([1, 2, 3, 4, 5, 6, 7, 8], weights=[5, 20, 30, 25, 10, 5, 3, 2])[0], len(products))

    for _ in range(basket_size - 1):
        # For each existing item in basket, check affinity to other categories
        added = False
        random.shuffle(categories)
        for cat in categories:
            source_cat = first_item["category"]
            key = (source_cat, cat) if (source_cat, cat) in affinities else (cat, source_cat)
            affinity = affinities.get(key, 0.05)

            if random.random() < affinity:
                candidates = [p for p in products_by_category.get(cat, []) if p["slug"] not in seen_slugs]
                if candidates:
                    # Weight by popularity within category
                    cat_weighted = []
                    for c in candidates:
                        cat_weighted.extend([c] * c["popularity_weight"])
                    pick = random.choice(cat_weighted)
                    basket.append(pick["slug"])
                    seen_slugs.add(pick["slug"])
                    added = True
                    break

        if not added:
            # Fallback: pick any unseen item by popularity
            candidates = [p for p in products if p["slug"] not in seen_slugs]
            if not candidates:
                break
            fallback_weighted = []
            for c in candidates:
                fallback_weighted.extend([c] * c["popularity_weight"])
            pick = random.choice(fallback_weighted)
            basket.append(pick["slug"])
            seen_slugs.add(pick["slug"])

    return basket


# Generate all orders on the driver, then parallelize
user_ids = [f"user_{i:06d}" for i in range(user_count)]
store_ids = [f"store_{i:03d}" for i in range(store_count)]
start_date = datetime.now() - timedelta(days=365)

orders_data = []
for i in range(order_count):
    user_id = random.choice(user_ids)
    order_id = str(uuid.uuid4())[:12]
    order_date = start_date + timedelta(days=random.randint(0, 365))
    store_id = random.choice(store_ids)
    basket = generate_basket()
    orders_data.append((user_id, order_id, order_date.date(), store_id, basket))

    if (i + 1) % 100000 == 0:
        print(f"  Generated {i + 1:,} / {order_count:,} orders")

print(f"Generated {len(orders_data):,} orders. Writing to Delta...")

# COMMAND ----------

# DBTITLE 1,Write raw_orders table
order_schema = StructType([
    StructField("user_id", StringType(), False),
    StructField("order_id", StringType(), False),
    StructField("order_date", DateType(), False),
    StructField("store_id", StringType(), False),
    StructField("products", ArrayType(StringType()), False),
])

raw_orders_sdf = spark.createDataFrame(orders_data, schema=order_schema)
raw_orders_sdf.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{catalog}.{schema}.raw_orders"
)

print(f"Wrote {raw_orders_sdf.count():,} orders to {catalog}.{schema}.raw_orders")
raw_orders_sdf.display()
