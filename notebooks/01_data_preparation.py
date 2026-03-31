# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Data Preparation
# MAGIC Cleans transaction data and prepares features for MBA and ALS models.
# MAGIC Outputs: `cleaned_orders` and `user_item_ratings` Delta tables.
# MAGIC
# MAGIC **Compute**: Serverless compatible.

# COMMAND ----------

# DBTITLE 1,Load config
# MAGIC %run ./config_loader

cfg = load_config()
catalog = cfg["catalog"]
schema = cfg["schema"]
source_table = cfg.get("source_table")
exclude_items = cfg.get("exclude_items", [])

# COMMAND ----------

# DBTITLE 1,Load raw orders
if source_table:
    print(f"Using customer data from: {source_table}")
    sdf_raw = spark.read.table(source_table)
else:
    print(f"Using synthetic data from: {catalog}.{schema}.raw_orders")
    sdf_raw = spark.read.table(f"{catalog}.{schema}.raw_orders")

print(f"Raw orders: {sdf_raw.count():,}")
sdf_raw.display()

# COMMAND ----------

# DBTITLE 1,Filter and clean orders
from pyspark.sql.functions import col, size, array_except, array, lit, explode, array_distinct

# Filter nulls
sdf_filtered = sdf_raw.filter(col("products").isNotNull()).filter(size(col("products")) > 0)

# Remove excluded items (configurable via config.yaml)
if exclude_items:
    exclude_arr = array(*[lit(item) for item in exclude_items])
    sdf_filtered = sdf_filtered.withColumn("products", array_except(col("products"), exclude_arr))
    sdf_filtered = sdf_filtered.filter(size(col("products")) > 0)

# Deduplicate within each basket
sdf_filtered = sdf_filtered.withColumn("products", array_distinct(col("products")))

# For synthetic data, products are already slugs. For customer data with a product_catalog
# mapping table, join here to convert display names to slugs.
if source_table and spark.catalog.tableExists(f"{catalog}.{schema}.product_catalog"):
    # Explode, join to slug mapping, re-aggregate
    sdf_exploded = sdf_filtered.withColumn("product_name", explode("products"))
    product_catalog = spark.read.table(f"{catalog}.{schema}.product_catalog").select("product_name", "product_slug")
    sdf_mapped = sdf_exploded.join(product_catalog, on="product_name", how="inner")
    from pyspark.sql.functions import collect_list
    sdf_filtered = sdf_mapped.groupBy("user_id", "order_id", "order_date", "store_id").agg(
        array_distinct(collect_list("product_slug")).alias("products")
    ).filter(size(col("products")) > 0)

sdf_cleaned = sdf_filtered.withColumnRenamed("products", "order_product_list")
print(f"Cleaned orders: {sdf_cleaned.count():,}")

# COMMAND ----------

# DBTITLE 1,Write cleaned_orders
sdf_cleaned.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{catalog}.{schema}.cleaned_orders"
)
print(f"Wrote cleaned_orders to {catalog}.{schema}.cleaned_orders")
sdf_cleaned.display()

# COMMAND ----------

# DBTITLE 1,Generate user-item implicit ratings for ALS
from pyspark.sql.functions import explode as explode_fn, count
from pyspark.sql.functions import sum as spark_sum

# Explode basket to one row per (user, item)
sdf_user_items = sdf_cleaned.withColumn("product_slug", explode_fn("order_product_list")).select("user_id", "product_slug")

# Count (user, item) co-occurrences
sdf_counts = sdf_user_items.groupBy("user_id", "product_slug").agg(count("*").alias("item_order_count"))

# Total orders per user
sdf_user_totals = sdf_counts.groupBy("user_id").agg(spark_sum("item_order_count").alias("total_orders"))

# Join and compute proportion
sdf_ratings = sdf_counts.join(sdf_user_totals, on="user_id")
sdf_ratings = sdf_ratings.withColumn("proportion_of_orders", col("item_order_count") / col("total_orders"))
sdf_ratings = sdf_ratings.select("user_id", "product_slug", "item_order_count", "proportion_of_orders")

sdf_ratings.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{catalog}.{schema}.user_item_ratings"
)
print(f"Wrote user_item_ratings: {sdf_ratings.count():,} rows")
sdf_ratings.display()
