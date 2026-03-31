# Recommender Accelerator Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a configurable recommender accelerator that generates synthetic data, trains MBA + ALS models, serves pre-computed results from Lakebase, and presents an interactive checkout demo app.

**Architecture:** Config-driven pipeline of 4 Databricks notebooks (data gen, prep, MBA training, ALS training) writing pre-computed recommendations to Delta tables synced to Lakebase. APX app (React + FastAPI) reads from Lakebase via psycopg2 to power an interactive shopping/checkout experience with real-time recommendation updates.

**Tech Stack:** Python, PySpark, PySpark ML (FPGrowth, ALS), MLflow, Optuna, Faker, Lakebase (PostgreSQL), FastAPI, React, TypeScript, shadcn/ui, Databricks Asset Bundles

**Spec:** `docs/superpowers/specs/2026-03-31-recommender-accelerator-design.md`

**Original notebooks (reference):** `/tmp/recommender_model/Recommender Model/`

---

## File Structure

```
recommender-accelerator/
├── config.yaml                           # Central config: vertical, catalog, schema, model params
├── databricks.yml                        # Asset bundle: job, cluster, app, Lakebase resources
├── notebooks/
│   ├── verticals/
│   │   ├── __init__.py                   # Vertical loader: get_vertical(name) -> dict
│   │   ├── qsr.py                        # QSR product catalog + affinity matrix
│   │   ├── retail.py                     # Retail product catalog + affinity matrix
│   │   └── grocery.py                    # Grocery product catalog + affinity matrix
│   ├── config_loader.py                  # Shared config reader (reads config.yaml via Spark/dbutils)
│   ├── 00_data_generation.py             # Databricks notebook: synthetic data gen
│   ├── 01_data_preparation.py            # Databricks notebook: clean + feature engineering
│   ├── 02_market_basket.py               # Databricks notebook: FPGrowth + pre-compute MBA recs
│   └── 03_collaborative_filter.py        # Databricks notebook: ALS + HPO + pre-compute user recs
├── app/                                  # APX scaffold (created via apx init)
│   ├── pyproject.toml
│   ├── src/recommender_app/
│   │   ├── app.py                        # FastAPI entrypoint
│   │   ├── db.py                         # Lakebase connection pool
│   │   └── routers/
│   │       ├── products.py               # GET /api/products, GET /api/users
│   │       └── recommend.py              # POST /api/recommend
│   └── ui/
│       └── src/
│           ├── routes/
│           │   └── index.tsx             # Main checkout page
│           ├── components/
│           │   ├── mode-toggle.tsx        # Known User / Anonymous toggle
│           │   ├── user-search.tsx        # User ID search/select
│           │   ├── product-grid.tsx       # Product catalog browsable grid
│           │   ├── cart-panel.tsx         # Shopping cart sidebar
│           │   └── recommendations.tsx    # "You might also like" panel
│           └── lib/
│               └── api.ts                # Generated API client (Orval)
├── docs/
│   ├── README.md                         # Setup guide, config reference, architecture
│   └── superpowers/
│       ├── specs/
│       │   └── 2026-03-31-recommender-accelerator-design.md
│       └── plans/
│           └── 2026-03-31-recommender-accelerator.md  (this file)
```

---

## Task 1: Project Scaffold and Config

**Files:**
- Create: `config.yaml`
- Create: `notebooks/config_loader.py`
- Create: `.gitignore`

- [ ] **Step 1: Initialize git repo**

```bash
cd ~/Desktop/Projects/recommender-accelerator
git init
```

- [ ] **Step 2: Create .gitignore**

Create `~/Desktop/Projects/recommender-accelerator/.gitignore`:

```
__pycache__/
*.pyc
.venv/
.env
node_modules/
dist/
.databricks/
*.egg-info/
```

- [ ] **Step 3: Create config.yaml**

Create `~/Desktop/Projects/recommender-accelerator/config.yaml`:

```yaml
# Recommender Accelerator Configuration
# Edit this file to configure the pipeline for your environment.

# Industry vertical: qsr | retail | grocery
# Determines synthetic product catalog and co-purchase patterns.
# Ignored when source_table is set.
vertical: qsr

# Databricks catalog and schema for all tables
catalog: jdub_demo
schema: recommender

# Optional: point to your own transaction table instead of generating synthetic data.
# When set, skip notebook 00 and run from notebook 01.
# Table must have columns: user_id STRING, order_id STRING, order_date DATE, products ARRAY<STRING>
# source_table: my_catalog.my_schema.my_orders

# Items to exclude from recommendations (sauces, utensils, etc.)
# Only used when source_table is set. Synthetic data has no items to exclude.
exclude_items: []

# Data generation parameters (only used by notebook 00)
product_count: 50
order_count: 500000
user_count: 10000
store_count: 50
seed: 42

# Model parameters
mba_min_transactions: 1000
mba_min_confidence: 0.0
als_hpo_trials: 20
recommendation_k: 5

# MLflow experiment paths
mlflow_experiment_root: /Shared/recommender-accelerator
```

- [ ] **Step 4: Create config_loader.py**

Create `~/Desktop/Projects/recommender-accelerator/notebooks/config_loader.py`:

```python
# Databricks notebook source
# DBTITLE 1,Config Loader
"""
Shared config loader for all notebooks.
Reads config.yaml from the repo root and returns a dict.
Usage in other notebooks:
    %run ./config_loader
    cfg = load_config()
"""

import yaml
import os


def load_config() -> dict:
    """Load config.yaml from the repo root directory."""
    # When run as a Databricks notebook via %run, the working directory
    # is the notebook's location. config.yaml is one level up.
    config_paths = [
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "config.yaml"),
        "/Workspace/Repos/config.yaml",  # fallback for Repos
        "../config.yaml",  # relative fallback
    ]
    for path in config_paths:
        resolved = os.path.realpath(path)
        if os.path.exists(resolved):
            with open(resolved) as f:
                return yaml.safe_load(f)
    raise FileNotFoundError(
        "config.yaml not found. Searched: " + ", ".join(config_paths)
    )
```

- [ ] **Step 5: Commit**

```bash
git add config.yaml notebooks/config_loader.py .gitignore
git commit -m "feat: project scaffold with config.yaml and config loader"
```

---

## Task 2: Vertical Templates

**Files:**
- Create: `notebooks/verticals/__init__.py`
- Create: `notebooks/verticals/qsr.py`
- Create: `notebooks/verticals/retail.py`
- Create: `notebooks/verticals/grocery.py`

Each vertical module exports a dict with:
- `products`: list of `{name, slug, category, base_price, popularity_weight}` dicts
- `categories`: list of category names
- `affinities`: dict mapping `(category, category)` tuples to probability floats (0.0-1.0)

- [ ] **Step 1: Create verticals/__init__.py**

Create `~/Desktop/Projects/recommender-accelerator/notebooks/verticals/__init__.py`:

```python
"""
Vertical templates for synthetic data generation.
Each vertical defines a product catalog and co-purchase affinity matrix.
"""

from importlib import import_module

VERTICALS = ["qsr", "retail", "grocery"]


def get_vertical(name: str) -> dict:
    """Load a vertical template by name. Returns dict with keys: products, categories, affinities."""
    if name not in VERTICALS:
        raise ValueError(f"Unknown vertical '{name}'. Choose from: {VERTICALS}")
    mod = import_module(f"verticals.{name}")
    return {
        "products": mod.PRODUCTS,
        "categories": mod.CATEGORIES,
        "affinities": mod.AFFINITIES,
    }
```

- [ ] **Step 2: Create qsr.py**

Create `~/Desktop/Projects/recommender-accelerator/notebooks/verticals/qsr.py`:

```python
"""QSR (Quick Service Restaurant) vertical template."""

CATEGORIES = ["Entrees", "Sides", "Drinks", "Desserts"]

PRODUCTS = [
    # Entrees
    {"name": "Classic Burger", "slug": "classic-burger", "category": "Entrees", "base_price": 7.99, "popularity_weight": 10},
    {"name": "Double Cheeseburger", "slug": "double-cheeseburger", "category": "Entrees", "base_price": 9.49, "popularity_weight": 8},
    {"name": "Crispy Chicken Sandwich", "slug": "crispy-chicken-sandwich", "category": "Entrees", "base_price": 8.49, "popularity_weight": 9},
    {"name": "Spicy Chicken Sandwich", "slug": "spicy-chicken-sandwich", "category": "Entrees", "base_price": 8.99, "popularity_weight": 7},
    {"name": "Grilled Chicken Wrap", "slug": "grilled-chicken-wrap", "category": "Entrees", "base_price": 7.49, "popularity_weight": 5},
    {"name": "Fish Sandwich", "slug": "fish-sandwich", "category": "Entrees", "base_price": 6.99, "popularity_weight": 3},
    {"name": "Veggie Burger", "slug": "veggie-burger", "category": "Entrees", "base_price": 7.99, "popularity_weight": 3},
    {"name": "BBQ Bacon Burger", "slug": "bbq-bacon-burger", "category": "Entrees", "base_price": 10.49, "popularity_weight": 6},
    {"name": "Chicken Tenders 3pc", "slug": "chicken-tenders-3pc", "category": "Entrees", "base_price": 6.99, "popularity_weight": 8},
    {"name": "Chicken Tenders 5pc", "slug": "chicken-tenders-5pc", "category": "Entrees", "base_price": 9.49, "popularity_weight": 7},
    {"name": "Nuggets 6pc", "slug": "nuggets-6pc", "category": "Entrees", "base_price": 5.49, "popularity_weight": 9},
    {"name": "Nuggets 10pc", "slug": "nuggets-10pc", "category": "Entrees", "base_price": 7.99, "popularity_weight": 6},
    # Sides
    {"name": "Classic Fries", "slug": "classic-fries", "category": "Sides", "base_price": 3.49, "popularity_weight": 10},
    {"name": "Curly Fries", "slug": "curly-fries", "category": "Sides", "base_price": 3.99, "popularity_weight": 6},
    {"name": "Onion Rings", "slug": "onion-rings", "category": "Sides", "base_price": 3.99, "popularity_weight": 5},
    {"name": "Mashed Potatoes", "slug": "mashed-potatoes", "category": "Sides", "base_price": 2.99, "popularity_weight": 4},
    {"name": "Coleslaw", "slug": "coleslaw", "category": "Sides", "base_price": 2.49, "popularity_weight": 3},
    {"name": "Mac & Cheese", "slug": "mac-and-cheese", "category": "Sides", "base_price": 3.49, "popularity_weight": 5},
    {"name": "Side Salad", "slug": "side-salad", "category": "Sides", "base_price": 3.99, "popularity_weight": 2},
    {"name": "Corn on the Cob", "slug": "corn-on-the-cob", "category": "Sides", "base_price": 2.49, "popularity_weight": 3},
    {"name": "Biscuit", "slug": "biscuit", "category": "Sides", "base_price": 1.49, "popularity_weight": 7},
    {"name": "Loaded Fries", "slug": "loaded-fries", "category": "Sides", "base_price": 4.99, "popularity_weight": 4},
    # Drinks
    {"name": "Fountain Soda", "slug": "fountain-soda", "category": "Drinks", "base_price": 2.29, "popularity_weight": 10},
    {"name": "Iced Tea", "slug": "iced-tea", "category": "Drinks", "base_price": 2.29, "popularity_weight": 6},
    {"name": "Lemonade", "slug": "lemonade", "category": "Drinks", "base_price": 2.49, "popularity_weight": 5},
    {"name": "Vanilla Shake", "slug": "vanilla-shake", "category": "Drinks", "base_price": 4.99, "popularity_weight": 5},
    {"name": "Chocolate Shake", "slug": "chocolate-shake", "category": "Drinks", "base_price": 4.99, "popularity_weight": 5},
    {"name": "Strawberry Shake", "slug": "strawberry-shake", "category": "Drinks", "base_price": 4.99, "popularity_weight": 4},
    {"name": "Coffee", "slug": "coffee", "category": "Drinks", "base_price": 1.99, "popularity_weight": 4},
    {"name": "Bottled Water", "slug": "bottled-water", "category": "Drinks", "base_price": 1.49, "popularity_weight": 3},
    # Desserts
    {"name": "Chocolate Chip Cookie", "slug": "chocolate-chip-cookie", "category": "Desserts", "base_price": 1.99, "popularity_weight": 6},
    {"name": "Apple Pie", "slug": "apple-pie", "category": "Desserts", "base_price": 2.49, "popularity_weight": 5},
    {"name": "Brownie", "slug": "brownie", "category": "Desserts", "base_price": 2.49, "popularity_weight": 4},
    {"name": "Soft Serve Cone", "slug": "soft-serve-cone", "category": "Desserts", "base_price": 1.49, "popularity_weight": 5},
    {"name": "Sundae", "slug": "sundae", "category": "Desserts", "base_price": 3.49, "popularity_weight": 3},
    {"name": "Cinnamon Sticks", "slug": "cinnamon-sticks", "category": "Desserts", "base_price": 2.99, "popularity_weight": 3},
]

# Co-purchase affinity: probability that a second item from category B is added
# given an item from category A is already in the cart.
# Higher = more likely to appear together.
AFFINITIES = {
    ("Entrees", "Sides"): 0.75,
    ("Entrees", "Drinks"): 0.65,
    ("Entrees", "Desserts"): 0.20,
    ("Sides", "Drinks"): 0.40,
    ("Sides", "Desserts"): 0.15,
    ("Drinks", "Desserts"): 0.25,
}
```

- [ ] **Step 3: Create retail.py**

Create `~/Desktop/Projects/recommender-accelerator/notebooks/verticals/retail.py`:

```python
"""Retail / E-commerce vertical template."""

CATEGORIES = ["Electronics", "Accessories", "Clothing", "Home"]

PRODUCTS = [
    # Electronics
    {"name": "Smartphone Pro", "slug": "smartphone-pro", "category": "Electronics", "base_price": 999.99, "popularity_weight": 10},
    {"name": "Wireless Earbuds", "slug": "wireless-earbuds", "category": "Electronics", "base_price": 129.99, "popularity_weight": 9},
    {"name": "Tablet 10in", "slug": "tablet-10in", "category": "Electronics", "base_price": 449.99, "popularity_weight": 7},
    {"name": "Bluetooth Speaker", "slug": "bluetooth-speaker", "category": "Electronics", "base_price": 59.99, "popularity_weight": 6},
    {"name": "Laptop Stand", "slug": "laptop-stand", "category": "Electronics", "base_price": 49.99, "popularity_weight": 5},
    {"name": "Webcam HD", "slug": "webcam-hd", "category": "Electronics", "base_price": 79.99, "popularity_weight": 4},
    {"name": "Portable Charger", "slug": "portable-charger", "category": "Electronics", "base_price": 39.99, "popularity_weight": 8},
    {"name": "Smart Watch", "slug": "smart-watch", "category": "Electronics", "base_price": 249.99, "popularity_weight": 7},
    {"name": "Noise-Canceling Headphones", "slug": "noise-canceling-headphones", "category": "Electronics", "base_price": 299.99, "popularity_weight": 6},
    # Accessories
    {"name": "Phone Case", "slug": "phone-case", "category": "Accessories", "base_price": 29.99, "popularity_weight": 9},
    {"name": "Screen Protector", "slug": "screen-protector", "category": "Accessories", "base_price": 12.99, "popularity_weight": 8},
    {"name": "USB-C Cable", "slug": "usb-c-cable", "category": "Accessories", "base_price": 14.99, "popularity_weight": 8},
    {"name": "Tablet Case", "slug": "tablet-case", "category": "Accessories", "base_price": 39.99, "popularity_weight": 6},
    {"name": "Watch Band", "slug": "watch-band", "category": "Accessories", "base_price": 24.99, "popularity_weight": 5},
    {"name": "Charging Dock", "slug": "charging-dock", "category": "Accessories", "base_price": 44.99, "popularity_weight": 5},
    {"name": "Laptop Sleeve", "slug": "laptop-sleeve", "category": "Accessories", "base_price": 34.99, "popularity_weight": 4},
    # Clothing
    {"name": "Graphic Tee", "slug": "graphic-tee", "category": "Clothing", "base_price": 24.99, "popularity_weight": 7},
    {"name": "Hoodie", "slug": "hoodie", "category": "Clothing", "base_price": 49.99, "popularity_weight": 6},
    {"name": "Running Shoes", "slug": "running-shoes", "category": "Clothing", "base_price": 89.99, "popularity_weight": 5},
    {"name": "Baseball Cap", "slug": "baseball-cap", "category": "Clothing", "base_price": 19.99, "popularity_weight": 5},
    {"name": "Athletic Socks 3pk", "slug": "athletic-socks-3pk", "category": "Clothing", "base_price": 14.99, "popularity_weight": 6},
    {"name": "Joggers", "slug": "joggers", "category": "Clothing", "base_price": 39.99, "popularity_weight": 4},
    # Home
    {"name": "Scented Candle", "slug": "scented-candle", "category": "Home", "base_price": 19.99, "popularity_weight": 5},
    {"name": "Photo Frame", "slug": "photo-frame", "category": "Home", "base_price": 14.99, "popularity_weight": 3},
    {"name": "Throw Pillow", "slug": "throw-pillow", "category": "Home", "base_price": 24.99, "popularity_weight": 4},
    {"name": "Desk Organizer", "slug": "desk-organizer", "category": "Home", "base_price": 29.99, "popularity_weight": 4},
    {"name": "LED Desk Lamp", "slug": "led-desk-lamp", "category": "Home", "base_price": 34.99, "popularity_weight": 5},
    {"name": "Mug Set", "slug": "mug-set", "category": "Home", "base_price": 18.99, "popularity_weight": 4},
]

AFFINITIES = {
    ("Electronics", "Accessories"): 0.80,
    ("Electronics", "Clothing"): 0.10,
    ("Electronics", "Home"): 0.15,
    ("Accessories", "Clothing"): 0.10,
    ("Accessories", "Home"): 0.10,
    ("Clothing", "Home"): 0.15,
}
```

- [ ] **Step 4: Create grocery.py**

Create `~/Desktop/Projects/recommender-accelerator/notebooks/verticals/grocery.py`:

```python
"""Grocery vertical template."""

CATEGORIES = ["Produce", "Dairy", "Bakery", "Pantry"]

PRODUCTS = [
    # Produce
    {"name": "Bananas", "slug": "bananas", "category": "Produce", "base_price": 0.79, "popularity_weight": 10},
    {"name": "Avocados 2pk", "slug": "avocados-2pk", "category": "Produce", "base_price": 3.49, "popularity_weight": 7},
    {"name": "Baby Spinach", "slug": "baby-spinach", "category": "Produce", "base_price": 3.99, "popularity_weight": 6},
    {"name": "Roma Tomatoes", "slug": "roma-tomatoes", "category": "Produce", "base_price": 2.49, "popularity_weight": 7},
    {"name": "Yellow Onions 3lb", "slug": "yellow-onions-3lb", "category": "Produce", "base_price": 2.99, "popularity_weight": 8},
    {"name": "Red Bell Peppers", "slug": "red-bell-peppers", "category": "Produce", "base_price": 1.49, "popularity_weight": 5},
    {"name": "Lemons", "slug": "lemons", "category": "Produce", "base_price": 0.69, "popularity_weight": 6},
    {"name": "Garlic Head", "slug": "garlic-head", "category": "Produce", "base_price": 0.79, "popularity_weight": 7},
    {"name": "Cilantro Bunch", "slug": "cilantro-bunch", "category": "Produce", "base_price": 0.99, "popularity_weight": 5},
    # Dairy
    {"name": "Whole Milk Gallon", "slug": "whole-milk-gallon", "category": "Dairy", "base_price": 4.29, "popularity_weight": 10},
    {"name": "Large Eggs 12ct", "slug": "large-eggs-12ct", "category": "Dairy", "base_price": 3.99, "popularity_weight": 9},
    {"name": "Shredded Cheddar", "slug": "shredded-cheddar", "category": "Dairy", "base_price": 3.49, "popularity_weight": 7},
    {"name": "Greek Yogurt", "slug": "greek-yogurt", "category": "Dairy", "base_price": 1.29, "popularity_weight": 6},
    {"name": "Butter", "slug": "butter", "category": "Dairy", "base_price": 4.49, "popularity_weight": 8},
    {"name": "Cream Cheese", "slug": "cream-cheese", "category": "Dairy", "base_price": 2.99, "popularity_weight": 5},
    {"name": "Sour Cream", "slug": "sour-cream", "category": "Dairy", "base_price": 2.49, "popularity_weight": 5},
    # Bakery
    {"name": "Sliced White Bread", "slug": "sliced-white-bread", "category": "Bakery", "base_price": 3.49, "popularity_weight": 9},
    {"name": "Wheat Bread", "slug": "wheat-bread", "category": "Bakery", "base_price": 3.99, "popularity_weight": 6},
    {"name": "Bagels 6pk", "slug": "bagels-6pk", "category": "Bakery", "base_price": 3.49, "popularity_weight": 5},
    {"name": "Flour Tortillas", "slug": "flour-tortillas", "category": "Bakery", "base_price": 2.99, "popularity_weight": 7},
    {"name": "Hamburger Buns 8pk", "slug": "hamburger-buns-8pk", "category": "Bakery", "base_price": 2.99, "popularity_weight": 5},
    {"name": "Croissants 4pk", "slug": "croissants-4pk", "category": "Bakery", "base_price": 4.99, "popularity_weight": 4},
    # Pantry
    {"name": "Spaghetti", "slug": "spaghetti", "category": "Pantry", "base_price": 1.49, "popularity_weight": 8},
    {"name": "Marinara Sauce", "slug": "marinara-sauce", "category": "Pantry", "base_price": 3.49, "popularity_weight": 7},
    {"name": "Olive Oil", "slug": "olive-oil", "category": "Pantry", "base_price": 6.99, "popularity_weight": 7},
    {"name": "Rice 2lb", "slug": "rice-2lb", "category": "Pantry", "base_price": 2.99, "popularity_weight": 8},
    {"name": "Black Beans Can", "slug": "black-beans-can", "category": "Pantry", "base_price": 1.29, "popularity_weight": 6},
    {"name": "Chicken Broth", "slug": "chicken-broth", "category": "Pantry", "base_price": 2.49, "popularity_weight": 6},
    {"name": "Cereal", "slug": "cereal", "category": "Pantry", "base_price": 4.49, "popularity_weight": 7},
    {"name": "Peanut Butter", "slug": "peanut-butter", "category": "Pantry", "base_price": 3.99, "popularity_weight": 6},
    {"name": "Salsa Jar", "slug": "salsa-jar", "category": "Pantry", "base_price": 3.49, "popularity_weight": 5},
]

AFFINITIES = {
    ("Produce", "Dairy"): 0.45,
    ("Produce", "Bakery"): 0.30,
    ("Produce", "Pantry"): 0.50,
    ("Dairy", "Bakery"): 0.55,
    ("Dairy", "Pantry"): 0.40,
    ("Bakery", "Pantry"): 0.45,
}
```

- [ ] **Step 5: Commit**

```bash
git add notebooks/verticals/
git commit -m "feat: add vertical templates for QSR, retail, and grocery"
```

---

## Task 3: Notebook 00 - Data Generation

**Files:**
- Create: `notebooks/00_data_generation.py`

This notebook generates synthetic transaction data using the configured vertical template. It uses PySpark (serverless-compatible) and Python random module (no Faker needed; the vertical templates already define realistic products).

- [ ] **Step 1: Create 00_data_generation.py**

Create `~/Desktop/Projects/recommender-accelerator/notebooks/00_data_generation.py`:

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Synthetic Data Generation
# MAGIC Generates realistic transaction data from the configured vertical template.
# MAGIC Outputs: `raw_orders` and `product_catalog` Delta tables.
# MAGIC
# MAGIC **Compute**: Serverless compatible.

# COMMAND ----------

# DBTITLE 1,Load config and vertical template
# MAGIC %run ./config_loader

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
```

- [ ] **Step 2: Commit**

```bash
git add notebooks/00_data_generation.py
git commit -m "feat: add notebook 00 - synthetic data generation from vertical config"
```

---

## Task 4: Notebook 01 - Data Preparation

**Files:**
- Create: `notebooks/01_data_preparation.py`

Cleans orders, maps to slugs, generates user-item ratings for ALS. Serverless compatible (no UDFs, only joins and aggregations).

- [ ] **Step 1: Create 01_data_preparation.py**

Create `~/Desktop/Projects/recommender-accelerator/notebooks/01_data_preparation.py`:

```python
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
```

- [ ] **Step 2: Commit**

```bash
git add notebooks/01_data_preparation.py
git commit -m "feat: add notebook 01 - data preparation and feature engineering"
```

---

## Task 5: Notebook 02 - Market Basket Analysis

**Files:**
- Create: `notebooks/02_market_basket.py`

FPGrowth training, evaluation, MLflow logging, and pre-computed MBA lookup table generation. Requires single-user ML cluster.

- [ ] **Step 1: Create 02_market_basket.py**

Create `~/Desktop/Projects/recommender-accelerator/notebooks/02_market_basket.py`:

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Market Basket Analysis
# MAGIC Trains FPGrowth model, evaluates with Hit@k, logs to MLflow, and generates
# MAGIC pre-computed per-product recommendation lookup table for Lakebase serving.
# MAGIC
# MAGIC **Compute**: Requires single-user ML cluster (PySpark ML).

# COMMAND ----------

# DBTITLE 1,Load config
# MAGIC %run ./config_loader

cfg = load_config()
catalog = cfg["catalog"]
schema = cfg["schema"]
min_transactions = cfg.get("mba_min_transactions", 1000)
min_confidence = cfg.get("mba_min_confidence", 0.0)
k = cfg.get("recommendation_k", 5)
experiment_root = cfg.get("mlflow_experiment_root", "/Shared/recommender-accelerator")

# COMMAND ----------

# DBTITLE 1,Load cleaned orders
sdf_cleaned = spark.read.table(f"{catalog}.{schema}.cleaned_orders")
num_transactions = sdf_cleaned.count()
print(f"Loaded {num_transactions:,} cleaned orders")

# COMMAND ----------

# DBTITLE 1,EDA - Product support
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

# COMMAND ----------

# DBTITLE 1,Train FPGrowth
from pyspark.ml.fpm import FPGrowth

train, test = sdf_cleaned.randomSplit([0.8, 0.2], seed=42)

fpGrowth = FPGrowth(
    itemsCol="order_product_list",
    minSupport=min_transactions / num_transactions,
    minConfidence=min_confidence,
    numPartitions=sc.defaultParallelism * 100,
)

model = fpGrowth.fit(train)
rules_count = model.associationRules.count()
print(f"Generated {rules_count:,} association rules")
model.associationRules.sort("antecedent", "consequent").display()

# COMMAND ----------

# DBTITLE 1,Save association rules
model.associationRules.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{catalog}.{schema}.mba_rules"
)

# Also save train/test for reproducibility
train.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{catalog}.{schema}.mba_train_dataset"
)
test.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{catalog}.{schema}.mba_test_dataset"
)

# COMMAND ----------

# DBTITLE 1,Recommendation scoring function
from pyspark.sql.functions import (
    broadcast, array_intersect, array, expr, size, power,
    row_number, collect_list, struct, col
)
from pyspark.sql.window import Window


def generate_recommendations(rules_sdf, cart_sdf, k=5, metric="confidence", testing=False):
    """
    Generate top-k product recommendations based on association rules.

    Parameters:
        rules_sdf: DataFrame with columns antecedent, consequent, lift, confidence
        cart_sdf: DataFrame with columns order_id, cart, and optionally 'added' if testing
        k: number of top recommendations
        metric: scoring metric (confidence or lift)
        testing: if True, include 'added' column for evaluation

    Returns:
        DataFrame with order_id, cart, recommendations_with_scores
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
        .withColumn("rank", row_number().over(Window.partitionBy("order_id", "consequent").orderBy(col("rule_score").desc())))
        .filter(col("rank") == 1)
        .withColumn("rank", row_number().over(Window.partitionBy("order_id").orderBy(col("rule_score").desc())))
        .filter(col("rank") <= k)
        .groupBy(return_columns)
        .agg(collect_list(struct("consequent", "rule_score")).alias("recommendations_with_scores"))
    )

    return top_k

# COMMAND ----------

# DBTITLE 1,Evaluate Hit@k on test set
from pyspark.sql.functions import expr, array_contains

rules = (
    spark.read.table(f"{catalog}.{schema}.mba_rules")
    .selectExpr("antecedent", "consequent[0] as consequent", "lift", "confidence")
)

test_data = spark.read.table(f"{catalog}.{schema}.mba_test_dataset").filter(size(col("order_product_list")) > 1)
test_transformed = (
    test_data
    .withColumn("cart", expr("slice(order_product_list, 1, size(order_product_list) - 1)"))
    .withColumn("added", expr("order_product_list[size(order_product_list) - 1]"))
    .withColumn("order_id", col("order_id"))
)

top_k_recs = generate_recommendations(rules, test_transformed, k=k, metric="confidence", testing=True)

top_k_recs = top_k_recs.withColumn(
    "hit_at_k",
    expr("cast(array_contains(transform(recommendations_with_scores, x -> x.consequent), added) as int)"),
)

hit_rate = top_k_recs.agg({"hit_at_k": "avg"}).collect()[0][0]
print(f"Hit@{k}: {hit_rate:.4f}")

# COMMAND ----------

# DBTITLE 1,Log model to MLflow
import mlflow
import mlflow.pyfunc
import pandas as pd
from mlflow.models.signature import ModelSignature
from mlflow.types import ColSpec, Schema, DataType
from mlflow.types.schema import Array

mlflow.set_experiment(f"{experiment_root}/market_basket_analysis")


class MBARecommenderModel(mlflow.pyfunc.PythonModel):
    def __init__(self, k=5, metric="confidence"):
        self.k = k
        self.metric = metric

    def load_context(self, context):
        self.rules_df = pd.read_parquet(context.artifacts["rules_file"])

    def _calculate_match_score(self, row):
        intersection = set(row["cart"]).intersection(row["antecedent"])
        return (len(intersection) ** 2) / (len(row["antecedent"]) * len(row["cart"]))

    def predict(self, context, model_input):
        merged = model_input.merge(self.rules_df, how="cross")
        if merged.empty:
            return pd.DataFrame(columns=["order_id", "cart", "recommendations", "rule_score"])
        merged = merged[merged.apply(lambda x: bool(set(x["cart"]) & set(x["antecedent"])), axis=1)]
        merged["match_score"] = merged.apply(self._calculate_match_score, axis=1)
        merged["rule_score"] = merged[self.metric] * merged["match_score"]
        merged = merged[~merged.apply(lambda x: x["consequent"] in x["cart"], axis=1)]
        grouped = merged.loc[merged.groupby(["order_id", "consequent"])["rule_score"].idxmax()]
        top_k = grouped.groupby("order_id").apply(lambda x: x.nlargest(self.k, "rule_score")).reset_index(drop=True)
        result = top_k.groupby("order_id").agg({"cart": "first", "consequent": list, "rule_score": list}).reset_index()
        result.rename(columns={"consequent": "recommendations"}, inplace=True)
        return result


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

# COMMAND ----------

# DBTITLE 1,Generate pre-computed MBA recommendations lookup table
import json
from pyspark.sql import Row

# For each unique product, generate top-k recommendations
product_catalog = spark.read.table(f"{catalog}.{schema}.product_catalog")
all_slugs = [row.product_slug for row in product_catalog.select("product_slug").collect()]

# Build single-item carts for each product
cart_rows = [Row(order_id=slug, cart=[slug]) for slug in all_slugs]
cart_sdf = spark.createDataFrame(cart_rows)

# Generate recommendations for every product
mba_recs = generate_recommendations(rules, cart_sdf, k=k, metric="confidence", testing=False)

# Reshape: order_id is product_slug, extract recs as JSON string
from pyspark.sql.functions import to_json

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
print(f"Wrote {recs_count} product recommendation entries to {catalog}.{schema}.mba_recommendations")
mba_lookup.display()

# COMMAND ----------

# DBTITLE 1,Sync mba_recommendations to Lakebase
# MAGIC %md
# MAGIC **Manual step**: Configure Lakebase sync for the `mba_recommendations` table.
# MAGIC Use the Databricks UI or CLI to create a Lakebase managed sync:
# MAGIC ```sql
# MAGIC CREATE OR REPLACE ONLINE TABLE {catalog}.{schema}.mba_recommendations_online
# MAGIC AS SELECT * FROM {catalog}.{schema}.mba_recommendations;
# MAGIC ```
# MAGIC Or configure via `databricks.yml` Lakebase resources (see Task 8).
```

- [ ] **Step 2: Commit**

```bash
git add notebooks/02_market_basket.py
git commit -m "feat: add notebook 02 - FPGrowth MBA with pre-computed lookup table"
```

---

## Task 6: Notebook 03 - Collaborative Filter

**Files:**
- Create: `notebooks/03_collaborative_filter.py`

ALS training with Optuna HPO, MLflow logging, and pre-computed per-user recommendation table. Requires single-user ML cluster.

- [ ] **Step 1: Create 03_collaborative_filter.py**

Create `~/Desktop/Projects/recommender-accelerator/notebooks/03_collaborative_filter.py`:

```python
# Databricks notebook source
# MAGIC %pip install optuna

# COMMAND ----------

# MAGIC %md
# MAGIC # 03 - Collaborative Filtering (ALS)
# MAGIC Trains ALS model with Optuna hyperparameter optimization, evaluates with Hit@k,
# MAGIC logs to MLflow, and generates pre-computed per-user recommendation lookup table.
# MAGIC
# MAGIC **Compute**: Requires single-user ML cluster (PySpark ML).

# COMMAND ----------

# DBTITLE 1,Load config
# MAGIC %run ./config_loader

cfg = load_config()
catalog = cfg["catalog"]
schema = cfg["schema"]
k = cfg.get("recommendation_k", 5)
n_trials = cfg.get("als_hpo_trials", 20)
experiment_root = cfg.get("mlflow_experiment_root", "/Shared/recommender-accelerator")

# COMMAND ----------

# DBTITLE 1,Load data
from pyspark.sql.functions import col

sdf_cleaned = spark.read.table(f"{catalog}.{schema}.cleaned_orders")
sdf_ratings = spark.read.table(f"{catalog}.{schema}.user_item_ratings")

train, test = sdf_cleaned.randomSplit([0.8, 0.2], seed=42)
print(f"Train: {train.count():,} | Test: {test.count():,}")

# COMMAND ----------

# DBTITLE 1,Preprocess for ALS
from pyspark.sql.functions import explode, count, sum as spark_sum, row_number
from pyspark.sql.window import Window


def preprocess_pipeline(train_dataset):
    """
    Preprocess training dataset for ALS.
    Creates integer user/item mappings and computes implicit ratings
    (proportion of user's orders containing each item).

    Returns: (als_train_dataset, item_mapping, user_mapping)
    """
    sdf_exploded = train_dataset.withColumn("product", explode("order_product_list"))
    als_input = sdf_exploded.select(col("user_id").alias("user"), col("product").alias("item"))

    item_mapping = als_input.select("item").distinct().withColumn("item_id", row_number().over(Window.orderBy("item")))
    user_mapping = als_input.select("user").distinct().withColumn("user_id_int", row_number().over(Window.orderBy("user")))

    als_input_mapped = (
        als_input
        .join(item_mapping, on="item")
        .join(user_mapping, on="user")
        .select("user_id_int", "item_id", "item")
    )

    als_grouped = als_input_mapped.groupBy("user_id_int", "item_id").agg(count("*").alias("item_count"))
    user_totals = als_grouped.groupBy("user_id_int").agg(spark_sum("item_count").alias("total_orders"))
    als_with_totals = als_grouped.join(user_totals, on="user_id_int")
    als_train = als_with_totals.withColumn("proportion_of_orders", col("item_count") / col("total_orders"))

    return als_train, item_mapping, user_mapping


als_train, item_mapping, user_mapping = preprocess_pipeline(train)
print(f"ALS train: {als_train.count():,} user-item pairs")
print(f"Users: {user_mapping.count():,} | Items: {item_mapping.count():,}")

# COMMAND ----------

# DBTITLE 1,Prepare test set
from pyspark.sql.functions import expr, size, array_contains, avg

test_filtered = test.filter(size(col("order_product_list")) > 1)
test_transformed = (
    test_filtered
    .withColumn("cart", expr("slice(order_product_list, 1, size(order_product_list) - 1)"))
    .withColumn("added", expr("order_product_list[size(order_product_list) - 1]"))
)

test_linked = test_transformed.join(user_mapping, test_transformed.user_id == user_mapping.user, "inner")
test_linked = (
    test_linked
    .join(item_mapping, test_linked.added == item_mapping.item, "inner")
    .withColumnRenamed("item_id", "added_item_id")
)
print(f"Test set: {test_linked.count():,} evaluable rows")

# COMMAND ----------

# DBTITLE 1,Define ALS training function
from pyspark.ml.recommendation import ALS


def train_als(train_dataset, rank, maxIter):
    als = ALS(
        rank=rank,
        maxIter=maxIter,
        userCol="user_id_int",
        itemCol="item_id",
        ratingCol="proportion_of_orders",
        implicitPrefs=True,
    )
    return als.fit(train_dataset)

# COMMAND ----------

# DBTITLE 1,Optuna HPO
import optuna
import mlflow
from functools import partial

mlflow.set_experiment(f"{experiment_root}/als_collaborative_filter")


def objective(trial, parent_run_id, k=5):
    with mlflow.start_run(parent_run_id=parent_run_id, nested=True):
        params = {
            "rank": trial.suggest_int("rank", 1, 100),
            "maxIter": trial.suggest_int("maxIter", 1, 10),
        }
        model = train_als(als_train, rank=params["rank"], maxIter=params["maxIter"])

        test_recs = model.recommendForUserSubset(test_linked.select("user_id_int").distinct(), k)
        test_recs = (
            test_recs.join(test_linked, "user_id_int", "inner")
            .select("user_id_int", "cart", "added", "added_item_id", "recommendations")
            .withColumn("hit_at_k", array_contains(col("recommendations.item_id"), col("added_item_id")).cast("int"))
        )
        avg_hit = test_recs.agg(avg(col("hit_at_k"))).first()[0]

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
import os

with mlflow.start_run(run_name="als_final", nested=True, parent_run_id=parent_run.info.run_id):
    als_full, item_map_full, user_map_full = preprocess_pipeline(sdf_cleaned)
    final_model = train_als(als_full, rank=best.params["rank"], maxIter=best.params["maxIter"])

    # Save mappings as parquet artifacts
    user_map_full.write.parquet("/tmp/als_user_mapping.parquet", mode="overwrite")
    item_map_full.write.parquet("/tmp/als_item_mapping.parquet", mode="overwrite")

    mlflow.log_artifact("/tmp/als_user_mapping.parquet", "user_mapping")
    mlflow.log_artifact("/tmp/als_item_mapping.parquet", "item_mapping")
    mlflow.spark.log_model(final_model, artifact_path="model",
                           registered_model_name=f"{catalog}.{schema}.als_recommender")
    mlflow.log_params({"rank": best.params["rank"], "maxIter": best.params["maxIter"]})
    mlflow.log_metric("hit_at_k", best.value)

print("Final model logged to MLflow")

# COMMAND ----------

# DBTITLE 1,Save mappings to Delta
user_map_full.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{catalog}.{schema}.als_user_mapping"
)
item_map_full.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{catalog}.{schema}.als_item_mapping"
)

# COMMAND ----------

# DBTITLE 1,Generate pre-computed per-user recommendations
from pyspark.sql.functions import to_json, collect_list, struct, explode as explode_fn

recs_per_user = final_model.recommendForAllUsers(k)

# Explode recommendations array, join to item_mapping to get product slugs
recs_exploded = (
    recs_per_user
    .withColumn("rec", explode_fn("recommendations"))
    .select("user_id_int", col("rec.item_id").alias("item_id"), col("rec.rating").alias("score"))
)

recs_with_slugs = recs_exploded.join(item_map_full, on="item_id")
recs_with_users = recs_with_slugs.join(user_map_full, on="user_id_int")

# Aggregate back to one row per user with JSON recommendations
als_lookup = (
    recs_with_users
    .groupBy(col("user").alias("user_id"))
    .agg(to_json(collect_list(struct(col("item").alias("product"), "score"))).alias("recommendations"))
)

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
```

- [ ] **Step 2: Commit**

```bash
git add notebooks/03_collaborative_filter.py
git commit -m "feat: add notebook 03 - ALS collaborative filter with Optuna HPO and pre-computed recs"
```

---

## Task 7: APX Demo App - Backend

**Files:**
- Create: `app/` (via `apx init`)
- Create: `app/src/recommender_app/db.py`
- Create: `app/src/recommender_app/routers/products.py`
- Create: `app/src/recommender_app/routers/recommend.py`
- Modify: `app/src/recommender_app/app.py`

- [ ] **Step 1: Initialize APX project**

Use the APX toolkit to scaffold the app:

```bash
cd ~/Desktop/Projects/recommender-accelerator
# Use apx MCP tool: mcp__plugin_apx_apx__start with project initialization
# Or manually create the structure following APX conventions
```

If APX scaffold is not available, create the directory structure manually:

```bash
mkdir -p app/src/recommender_app/routers
touch app/src/recommender_app/__init__.py
touch app/src/recommender_app/routers/__init__.py
```

- [ ] **Step 2: Create db.py - Lakebase connection pool**

Create `~/Desktop/Projects/recommender-accelerator/app/src/recommender_app/db.py`:

```python
"""Lakebase (PostgreSQL) connection pool for the recommender app."""

import os
from contextlib import contextmanager
from typing import Generator

import psycopg2
from psycopg2 import pool

_pool: pool.SimpleConnectionPool | None = None


def get_pool() -> pool.SimpleConnectionPool:
    """Get or create the connection pool. Reads config from environment variables."""
    global _pool
    if _pool is None:
        _pool = pool.SimpleConnectionPool(
            minconn=1,
            maxconn=5,
            host=os.environ["LAKEBASE_HOST"],
            port=int(os.environ.get("LAKEBASE_PORT", "5432")),
            dbname=os.environ["LAKEBASE_DATABASE"],
            user=os.environ["LAKEBASE_USER"],
            password=os.environ["LAKEBASE_PASSWORD"],
            sslmode=os.environ.get("LAKEBASE_SSLMODE", "require"),
        )
    return _pool


@contextmanager
def get_connection() -> Generator:
    """Context manager that gets a connection from the pool and returns it when done."""
    p = get_pool()
    conn = p.getconn()
    try:
        yield conn
    finally:
        p.putconn(conn)
```

- [ ] **Step 3: Create products.py router**

Create `~/Desktop/Projects/recommender-accelerator/app/src/recommender_app/routers/products.py`:

```python
"""Product catalog and user listing endpoints."""

from fastapi import APIRouter
from pydantic import BaseModel

from recommender_app.db import get_connection

router = APIRouter(prefix="/api", tags=["products"])


class Product(BaseModel):
    product_id: str
    product_name: str
    product_slug: str
    category: str
    base_price: float


class UserInfo(BaseModel):
    user_id: str


@router.get("/products", response_model=list[Product])
def list_products():
    """Return full product catalog with categories and prices."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT product_id, product_name, product_slug, category, base_price "
                "FROM product_catalog ORDER BY category, product_name"
            )
            rows = cur.fetchall()
    return [
        Product(
            product_id=r[0],
            product_name=r[1],
            product_slug=r[2],
            category=r[3],
            base_price=r[4],
        )
        for r in rows
    ]


@router.get("/users", response_model=list[UserInfo])
def list_users(limit: int = 20):
    """Return sample user IDs for the known-user dropdown."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT user_id FROM als_recommendations LIMIT %s",
                (limit,),
            )
            rows = cur.fetchall()
    return [UserInfo(user_id=r[0]) for r in rows]
```

- [ ] **Step 4: Create recommend.py router**

Create `~/Desktop/Projects/recommender-accelerator/app/src/recommender_app/routers/recommend.py`:

```python
"""Recommendation endpoint: serves pre-computed results from Lakebase."""

import json
from typing import Literal

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from recommender_app.db import get_connection

router = APIRouter(prefix="/api", tags=["recommend"])


class RecommendRequest(BaseModel):
    mode: Literal["known", "anonymous"]
    user_id: str | None = None
    cart: list[str] = []


class Recommendation(BaseModel):
    product: str
    score: float
    rank: int


class RecommendResponse(BaseModel):
    recommendations: list[Recommendation]
    mode: str
    source: str


def _get_als_recs(user_id: str, cart: list[str], k: int = 5) -> list[dict]:
    """Fetch ALS recommendations for a known user, excluding cart items."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT recommendations FROM als_recommendations WHERE user_id = %s",
                (user_id,),
            )
            row = cur.fetchone()
    if not row:
        return []
    recs = json.loads(row[0]) if isinstance(row[0], str) else row[0]
    cart_set = set(cart)
    filtered = [r for r in recs if r["product"] not in cart_set]
    return filtered[:k]


def _get_mba_recs(cart: list[str], k: int = 5) -> list[dict]:
    """Fetch MBA recommendations for cart items, merge and re-rank."""
    if not cart:
        return []
    with get_connection() as conn:
        with conn.cursor() as cur:
            placeholders = ",".join(["%s"] * len(cart))
            cur.execute(
                f"SELECT product_slug, recommendations FROM mba_recommendations "
                f"WHERE product_slug IN ({placeholders})",
                cart,
            )
            rows = cur.fetchall()

    # Merge recommendations from all cart items
    cart_set = set(cart)
    score_map: dict[str, float] = {}
    for _, recs_json in rows:
        recs = json.loads(recs_json) if isinstance(recs_json, str) else recs_json
        for r in recs:
            product = r.get("consequent", r.get("product", ""))
            score = r.get("rule_score", r.get("score", 0.0))
            if product not in cart_set:
                score_map[product] = score_map.get(product, 0.0) + score

    sorted_recs = sorted(score_map.items(), key=lambda x: x[1], reverse=True)
    return [{"product": p, "score": s} for p, s in sorted_recs[:k]]


@router.post("/recommend", response_model=RecommendResponse)
def recommend(req: RecommendRequest):
    """Generate top-5 recommendations based on mode and cart contents."""
    k = 5

    if req.mode == "known":
        if not req.user_id:
            raise HTTPException(status_code=400, detail="user_id required for known mode")
        recs = _get_als_recs(req.user_id, req.cart, k)
        source = "als"
    else:
        if not req.cart:
            raise HTTPException(status_code=400, detail="cart required for anonymous mode")
        recs = _get_mba_recs(req.cart, k)
        source = "mba"

    recommendations = [
        Recommendation(product=r["product"], score=round(r["score"], 4), rank=i + 1)
        for i, r in enumerate(recs)
    ]

    return RecommendResponse(recommendations=recommendations, mode=req.mode, source=source)
```

- [ ] **Step 5: Create app.py - FastAPI entrypoint**

Create `~/Desktop/Projects/recommender-accelerator/app/src/recommender_app/app.py`:

```python
"""FastAPI entrypoint for the Recommender Accelerator demo app."""

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from recommender_app.routers import products, recommend

app = FastAPI(title="Recommender Accelerator", version="1.0.0")

app.include_router(products.router)
app.include_router(recommend.router)


@app.get("/api/health")
def health():
    return {"status": "ok"}
```

- [ ] **Step 6: Create pyproject.toml**

Create `~/Desktop/Projects/recommender-accelerator/app/pyproject.toml`:

```toml
[project]
name = "recommender-app"
version = "1.0.0"
description = "Recommender Accelerator demo app"
requires-python = ">=3.11"
dependencies = [
    "fastapi>=0.110.0",
    "uvicorn>=0.27.0",
    "psycopg2-binary>=2.9.0",
    "pydantic>=2.0.0",
]
```

- [ ] **Step 7: Commit**

```bash
git add app/
git commit -m "feat: add APX backend - FastAPI with Lakebase-backed recommendation endpoints"
```

---

## Task 8: APX Demo App - Frontend

**Files:**
- Create: `app/ui/src/routes/index.tsx`
- Create: `app/ui/src/components/mode-toggle.tsx`
- Create: `app/ui/src/components/user-search.tsx`
- Create: `app/ui/src/components/product-grid.tsx`
- Create: `app/ui/src/components/cart-panel.tsx`
- Create: `app/ui/src/components/recommendations.tsx`

This task creates the interactive checkout UI. The API client (`app/ui/src/lib/api.ts`) is generated by Orval from the FastAPI OpenAPI schema after the backend is running.

- [ ] **Step 1: Generate API client**

After the backend is running locally (`apx start`), generate the Orval API client:

```bash
cd ~/Desktop/Projects/recommender-accelerator/app/ui
bunx orval
```

This generates `src/lib/api.ts` with typed hooks for all endpoints.

- [ ] **Step 2: Create mode-toggle.tsx**

Create `~/Desktop/Projects/recommender-accelerator/app/ui/src/components/mode-toggle.tsx`:

```tsx
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs";

interface ModeToggleProps {
  mode: "known" | "anonymous";
  onModeChange: (mode: "known" | "anonymous") => void;
}

export function ModeToggle({ mode, onModeChange }: ModeToggleProps) {
  return (
    <Tabs value={mode} onValueChange={(v) => onModeChange(v as "known" | "anonymous")}>
      <TabsList>
        <TabsTrigger value="known">Known User</TabsTrigger>
        <TabsTrigger value="anonymous">Anonymous</TabsTrigger>
      </TabsList>
    </Tabs>
  );
}
```

- [ ] **Step 3: Create user-search.tsx**

Create `~/Desktop/Projects/recommender-accelerator/app/ui/src/components/user-search.tsx`:

```tsx
import { useState } from "react";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";

interface UserSearchProps {
  users: { user_id: string }[];
  selectedUser: string | null;
  onUserSelect: (userId: string) => void;
}

export function UserSearch({ users, selectedUser, onUserSelect }: UserSearchProps) {
  return (
    <Select value={selectedUser ?? ""} onValueChange={onUserSelect}>
      <SelectTrigger className="w-64">
        <SelectValue placeholder="Select a user ID..." />
      </SelectTrigger>
      <SelectContent>
        {users.map((u) => (
          <SelectItem key={u.user_id} value={u.user_id}>
            {u.user_id}
          </SelectItem>
        ))}
      </SelectContent>
    </Select>
  );
}
```

- [ ] **Step 4: Create product-grid.tsx**

Create `~/Desktop/Projects/recommender-accelerator/app/ui/src/components/product-grid.tsx`:

```tsx
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardFooter } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { useState } from "react";

interface Product {
  product_id: string;
  product_name: string;
  product_slug: string;
  category: string;
  base_price: number;
}

interface ProductGridProps {
  products: Product[];
  onAddToCart: (slug: string) => void;
  cartSlugs: Set<string>;
}

const CATEGORY_COLORS: Record<string, string> = {
  Entrees: "bg-red-100 text-red-800",
  Sides: "bg-amber-100 text-amber-800",
  Drinks: "bg-blue-100 text-blue-800",
  Desserts: "bg-purple-100 text-purple-800",
  Electronics: "bg-blue-100 text-blue-800",
  Accessories: "bg-slate-100 text-slate-800",
  Clothing: "bg-emerald-100 text-emerald-800",
  Home: "bg-amber-100 text-amber-800",
  Produce: "bg-green-100 text-green-800",
  Dairy: "bg-sky-100 text-sky-800",
  Bakery: "bg-amber-100 text-amber-800",
  Pantry: "bg-orange-100 text-orange-800",
};

export function ProductGrid({ products, onAddToCart, cartSlugs }: ProductGridProps) {
  const [search, setSearch] = useState("");
  const [activeCategory, setActiveCategory] = useState<string | null>(null);

  const categories = [...new Set(products.map((p) => p.category))];
  const filtered = products.filter((p) => {
    const matchesSearch = p.product_name.toLowerCase().includes(search.toLowerCase());
    const matchesCategory = !activeCategory || p.category === activeCategory;
    return matchesSearch && matchesCategory;
  });

  return (
    <div className="space-y-4">
      <div className="flex gap-2 items-center flex-wrap">
        <Input
          placeholder="Search products..."
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          className="w-64"
        />
        <div className="flex gap-1 flex-wrap">
          <Badge
            variant={activeCategory === null ? "default" : "outline"}
            className="cursor-pointer"
            onClick={() => setActiveCategory(null)}
          >
            All
          </Badge>
          {categories.map((cat) => (
            <Badge
              key={cat}
              variant={activeCategory === cat ? "default" : "outline"}
              className="cursor-pointer"
              onClick={() => setActiveCategory(cat)}
            >
              {cat}
            </Badge>
          ))}
        </div>
      </div>

      <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-3">
        {filtered.map((p) => (
          <Card key={p.product_slug} className="flex flex-col">
            <CardContent className="pt-4 flex-1">
              <Badge className={CATEGORY_COLORS[p.category] ?? "bg-gray-100 text-gray-800"} variant="secondary">
                {p.category}
              </Badge>
              <h3 className="font-medium mt-2 text-sm">{p.product_name}</h3>
              <p className="text-muted-foreground text-sm">${p.base_price.toFixed(2)}</p>
            </CardContent>
            <CardFooter className="pt-0">
              <Button
                size="sm"
                variant={cartSlugs.has(p.product_slug) ? "secondary" : "default"}
                onClick={() => onAddToCart(p.product_slug)}
                className="w-full"
              >
                {cartSlugs.has(p.product_slug) ? "In Cart" : "Add to Cart"}
              </Button>
            </CardFooter>
          </Card>
        ))}
      </div>
    </div>
  );
}
```

- [ ] **Step 5: Create cart-panel.tsx**

Create `~/Desktop/Projects/recommender-accelerator/app/ui/src/components/cart-panel.tsx`:

```tsx
import { Button } from "@/components/ui/button";
import { X } from "lucide-react";

interface CartItem {
  slug: string;
  name: string;
  price: number;
  quantity: number;
}

interface CartPanelProps {
  items: CartItem[];
  onRemove: (slug: string) => void;
  onClear: () => void;
}

export function CartPanel({ items, onRemove, onClear }: CartPanelProps) {
  const total = items.reduce((sum, item) => sum + item.price * item.quantity, 0);

  return (
    <div className="border rounded-lg p-4 space-y-3">
      <div className="flex justify-between items-center">
        <h2 className="font-semibold text-lg">Cart ({items.length})</h2>
        {items.length > 0 && (
          <Button variant="ghost" size="sm" onClick={onClear}>
            Clear
          </Button>
        )}
      </div>

      {items.length === 0 ? (
        <p className="text-muted-foreground text-sm">Add items to get recommendations</p>
      ) : (
        <div className="space-y-2">
          {items.map((item) => (
            <div key={item.slug} className="flex justify-between items-center text-sm">
              <div>
                <span className="font-medium">{item.name}</span>
                <span className="text-muted-foreground ml-2">${item.price.toFixed(2)}</span>
              </div>
              <Button variant="ghost" size="icon" className="h-6 w-6" onClick={() => onRemove(item.slug)}>
                <X className="h-3 w-3" />
              </Button>
            </div>
          ))}
          <div className="border-t pt-2 flex justify-between font-semibold">
            <span>Total</span>
            <span>${total.toFixed(2)}</span>
          </div>
        </div>
      )}
    </div>
  );
}
```

- [ ] **Step 6: Create recommendations.tsx**

Create `~/Desktop/Projects/recommender-accelerator/app/ui/src/components/recommendations.tsx`:

```tsx
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Progress } from "@/components/ui/progress";
import { Loader2 } from "lucide-react";

interface Recommendation {
  product: string;
  score: number;
  rank: number;
}

interface RecommendationsProps {
  recommendations: Recommendation[];
  source: string;
  loading: boolean;
  productNames: Record<string, string>;
  productPrices: Record<string, number>;
  onAddToCart: (slug: string) => void;
}

export function Recommendations({
  recommendations,
  source,
  loading,
  productNames,
  productPrices,
  onAddToCart,
}: RecommendationsProps) {
  if (loading) {
    return (
      <div className="border rounded-lg p-6 flex items-center justify-center">
        <Loader2 className="h-5 w-5 animate-spin mr-2" />
        <span className="text-muted-foreground">Getting recommendations...</span>
      </div>
    );
  }

  if (recommendations.length === 0) {
    return null;
  }

  const maxScore = Math.max(...recommendations.map((r) => r.score));

  return (
    <div className="border rounded-lg p-4 space-y-3">
      <div className="flex justify-between items-center">
        <h2 className="font-semibold text-lg">You might also like</h2>
        <span className="text-xs text-muted-foreground uppercase tracking-wide">
          {source === "als" ? "Personalized" : "Based on cart"}
        </span>
      </div>

      <div className="space-y-2">
        {recommendations.map((rec) => (
          <Card key={rec.product} className="overflow-hidden">
            <CardContent className="p-3 flex items-center gap-3">
              <div className="flex-1 min-w-0">
                <div className="flex items-center gap-2">
                  <span className="font-medium text-sm truncate">
                    {productNames[rec.product] ?? rec.product}
                  </span>
                  <span className="text-muted-foreground text-xs">
                    ${(productPrices[rec.product] ?? 0).toFixed(2)}
                  </span>
                </div>
                <div className="flex items-center gap-2 mt-1">
                  <Progress value={(rec.score / maxScore) * 100} className="h-1.5 flex-1" />
                  <span className="text-xs font-mono text-muted-foreground w-12 text-right">
                    {(rec.score * 100).toFixed(0)}%
                  </span>
                </div>
              </div>
              <Button size="sm" variant="outline" onClick={() => onAddToCart(rec.product)}>
                Add
              </Button>
            </CardContent>
          </Card>
        ))}
      </div>
    </div>
  );
}
```

- [ ] **Step 7: Create index.tsx - Main checkout page**

Create `~/Desktop/Projects/recommender-accelerator/app/ui/src/routes/index.tsx`:

```tsx
import { useState, useCallback, useEffect } from "react";
import { createFileRoute } from "@tanstack/react-router";
import { ModeToggle } from "@/components/mode-toggle";
import { UserSearch } from "@/components/user-search";
import { ProductGrid } from "@/components/product-grid";
import { CartPanel } from "@/components/cart-panel";
import { Recommendations } from "@/components/recommendations";

export const Route = createFileRoute("/")({ component: HomePage });

interface Product {
  product_id: string;
  product_name: string;
  product_slug: string;
  category: string;
  base_price: number;
}

interface CartItem {
  slug: string;
  name: string;
  price: number;
  quantity: number;
}

interface Rec {
  product: string;
  score: number;
  rank: number;
}

function HomePage() {
  const [mode, setMode] = useState<"known" | "anonymous">("anonymous");
  const [selectedUser, setSelectedUser] = useState<string | null>(null);
  const [products, setProducts] = useState<Product[]>([]);
  const [users, setUsers] = useState<{ user_id: string }[]>([]);
  const [cart, setCart] = useState<CartItem[]>([]);
  const [recommendations, setRecommendations] = useState<Rec[]>([]);
  const [source, setSource] = useState("");
  const [loading, setLoading] = useState(false);

  const productMap = Object.fromEntries(products.map((p) => [p.product_slug, p]));
  const productNames = Object.fromEntries(products.map((p) => [p.product_slug, p.product_name]));
  const productPrices = Object.fromEntries(products.map((p) => [p.product_slug, p.base_price]));
  const cartSlugs = new Set(cart.map((c) => c.slug));

  useEffect(() => {
    fetch("/api/products").then((r) => r.json()).then(setProducts);
    fetch("/api/users?limit=50").then((r) => r.json()).then(setUsers);
  }, []);

  const fetchRecs = useCallback(
    async (currentCart: CartItem[], currentMode: "known" | "anonymous", userId: string | null) => {
      const slugs = currentCart.map((c) => c.slug);
      if (currentMode === "anonymous" && slugs.length === 0) {
        setRecommendations([]);
        return;
      }
      if (currentMode === "known" && !userId) {
        setRecommendations([]);
        return;
      }

      setLoading(true);
      try {
        const res = await fetch("/api/recommend", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ mode: currentMode, user_id: userId, cart: slugs }),
        });
        const data = await res.json();
        setRecommendations(data.recommendations);
        setSource(data.source);
      } finally {
        setLoading(false);
      }
    },
    [],
  );

  const addToCart = useCallback(
    (slug: string) => {
      if (cartSlugs.has(slug)) return;
      const p = productMap[slug];
      if (!p) return;
      const newCart = [...cart, { slug, name: p.product_name, price: p.base_price, quantity: 1 }];
      setCart(newCart);
      fetchRecs(newCart, mode, selectedUser);
    },
    [cart, cartSlugs, productMap, mode, selectedUser, fetchRecs],
  );

  const removeFromCart = useCallback(
    (slug: string) => {
      const newCart = cart.filter((c) => c.slug !== slug);
      setCart(newCart);
      fetchRecs(newCart, mode, selectedUser);
    },
    [cart, mode, selectedUser, fetchRecs],
  );

  const clearCart = useCallback(() => {
    setCart([]);
    setRecommendations([]);
  }, []);

  const handleModeChange = useCallback(
    (newMode: "known" | "anonymous") => {
      setMode(newMode);
      setRecommendations([]);
      if (newMode === "known" && selectedUser) {
        fetchRecs(cart, newMode, selectedUser);
      } else if (newMode === "anonymous" && cart.length > 0) {
        fetchRecs(cart, newMode, null);
      }
    },
    [cart, selectedUser, fetchRecs],
  );

  const handleUserSelect = useCallback(
    (userId: string) => {
      setSelectedUser(userId);
      fetchRecs(cart, "known", userId);
    },
    [cart, fetchRecs],
  );

  return (
    <div className="min-h-screen bg-background">
      <header className="border-b px-6 py-4">
        <div className="max-w-7xl mx-auto flex items-center justify-between">
          <h1 className="text-xl font-bold">Recommender Accelerator</h1>
          <div className="flex items-center gap-4">
            <ModeToggle mode={mode} onModeChange={handleModeChange} />
            {mode === "known" && (
              <UserSearch users={users} selectedUser={selectedUser} onUserSelect={handleUserSelect} />
            )}
          </div>
        </div>
      </header>

      <main className="max-w-7xl mx-auto px-6 py-6">
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          <div className="lg:col-span-2">
            <ProductGrid products={products} onAddToCart={addToCart} cartSlugs={cartSlugs} />
          </div>
          <div className="space-y-4">
            <CartPanel items={cart} onRemove={removeFromCart} onClear={clearCart} />
            <Recommendations
              recommendations={recommendations}
              source={source}
              loading={loading}
              productNames={productNames}
              productPrices={productPrices}
              onAddToCart={addToCart}
            />
          </div>
        </div>
      </main>
    </div>
  );
}
```

- [ ] **Step 8: Install shadcn/ui components**

```bash
cd ~/Desktop/Projects/recommender-accelerator/app/ui
bunx shadcn@latest add tabs select badge card progress input button
bun add lucide-react
```

- [ ] **Step 9: Commit**

```bash
git add app/ui/
git commit -m "feat: add APX frontend - interactive checkout UI with real-time recommendations"
```

---

## Task 9: Databricks Asset Bundle

**Files:**
- Create: `databricks.yml`

- [ ] **Step 1: Create databricks.yml**

Create `~/Desktop/Projects/recommender-accelerator/databricks.yml`:

```yaml
bundle:
  name: recommender-accelerator

variables:
  catalog:
    default: jdub_demo
  schema:
    default: recommender

resources:
  jobs:
    recommender_training_pipeline:
      name: "Recommender Training Pipeline"
      tasks:
        - task_key: data_generation
          notebook_task:
            notebook_path: ./notebooks/00_data_generation.py
          # Serverless - no cluster config needed

        - task_key: data_preparation
          notebook_task:
            notebook_path: ./notebooks/01_data_preparation.py
          depends_on:
            - task_key: data_generation

        - task_key: market_basket
          notebook_task:
            notebook_path: ./notebooks/02_market_basket.py
          depends_on:
            - task_key: data_preparation
          new_cluster:
            spark_version: "15.4.x-ml-scala2.12"
            node_type_id: "i3.xlarge"
            num_workers: 2
            data_security_mode: SINGLE_USER

        - task_key: collaborative_filter
          notebook_task:
            notebook_path: ./notebooks/03_collaborative_filter.py
          depends_on:
            - task_key: data_preparation
          new_cluster:
            spark_version: "15.4.x-ml-scala2.12"
            node_type_id: "i3.xlarge"
            num_workers: 2
            data_security_mode: SINGLE_USER

      # Schedule OFF by default. Customer enables when ready.
      # schedule:
      #   quartz_cron_expression: "0 0 6 ? * MON"
      #   timezone_id: "America/Denver"
```

- [ ] **Step 2: Commit**

```bash
git add databricks.yml
git commit -m "feat: add Databricks Asset Bundle with training pipeline job (schedule off)"
```

---

## Task 10: README

**Files:**
- Create: `docs/README.md`

- [ ] **Step 1: Create README.md**

Create `~/Desktop/Projects/recommender-accelerator/docs/README.md`:

```markdown
# Recommender Accelerator

A configurable, demo-ready product recommendation system built on Databricks. Generates synthetic data for any industry vertical (QSR, retail, grocery), trains two complementary models (Market Basket Analysis + Collaborative Filtering), and serves pre-computed results through an interactive checkout-style demo app.

## Quick Start

1. **Clone and configure:**
   ```bash
   git clone <repo-url>
   cd recommender-accelerator
   # Edit config.yaml: set your catalog, schema, and vertical
   ```

2. **Deploy:**
   ```bash
   databricks bundle deploy
   ```

3. **Run the training pipeline:**
   ```bash
   databricks bundle run recommender_training_pipeline
   ```

4. **Open the demo app** from the Databricks Apps UI.

## Configuration

Edit `config.yaml` to customize:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `vertical` | `qsr` | Industry vertical: `qsr`, `retail`, `grocery` |
| `catalog` | `jdub_demo` | Unity Catalog catalog name |
| `schema` | `recommender` | Schema for all tables |
| `order_count` | `500000` | Number of synthetic orders |
| `user_count` | `10000` | Number of synthetic users |
| `recommendation_k` | `5` | Top-k recommendations to generate |
| `als_hpo_trials` | `20` | Optuna hyperparameter search trials |

## Using Your Own Data

1. Set `source_table` in `config.yaml` to your transaction table
2. Your table must have: `user_id`, `order_id`, `order_date`, `products` (array of strings)
3. Optionally set `exclude_items` to filter non-recommendable items
4. Skip notebook 00, run from notebook 01

## Architecture

```
config.yaml -> [00 Data Gen] -> [01 Data Prep] -> [02 MBA] -> Lakebase -> App
                                                -> [03 ALS] -> Lakebase -> App
```

- **Notebooks 00-01**: Serverless compute
- **Notebooks 02-03**: Single-user ML cluster (PySpark ML requirement)
- **Serving**: Pre-computed lookup tables in Lakebase (low-latency PostgreSQL)
- **App**: React + FastAPI via Databricks Apps (APX)

## Models

**Market Basket Analysis (FPGrowth)**: Finds product co-purchase patterns. Powers anonymous/cart-based recommendations.

**Collaborative Filtering (ALS)**: Learns user preferences from order history. Powers personalized recommendations for known users.

Both models log to MLflow for experiment tracking and model registry.
```

- [ ] **Step 2: Commit**

```bash
git add docs/README.md
git commit -m "docs: add project README with quick start and configuration guide"
```

---

**Self-review complete.** Checked all spec requirements against tasks:
- Config-driven verticals: Task 1 + 2
- Notebook 00 (data gen): Task 3
- Notebook 01 (prep): Task 4
- Notebook 02 (MBA + pre-compute + Lakebase): Task 5
- Notebook 03 (ALS + HPO + pre-compute + Lakebase): Task 6
- App backend (FastAPI + Lakebase): Task 7
- App frontend (React checkout UI): Task 8
- Asset bundle (job, schedule off): Task 9
- README: Task 10
- MLflow integration: covered in Tasks 5 and 6
- Customer adoption path: covered in README (Task 10)

No placeholders, TBDs, or incomplete sections. Type/function names are consistent across tasks.
