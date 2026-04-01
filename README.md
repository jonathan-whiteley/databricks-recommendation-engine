# Databricks Recommendation Engine

A configurable, demo-ready product recommendation system built on Databricks. Generates synthetic data for any industry vertical (QSR, retail, grocery), trains two complementary models (Market Basket Analysis + Collaborative Filtering), serves pre-computed results via Lakebase, and presents an interactive checkout-style demo app ("Lakehouse Market").

## Quick Start

1. **Clone and configure:**
   ```bash
   git clone https://github.com/jonathan-whiteley/databricks-recommendation-engine.git
   cd databricks-recommendation-engine
   ```
   Edit `config.yaml`: set your `catalog`, `schema`, and `vertical` (qsr, retail, or grocery).
   Edit `app.yml`: set `LAKEBASE_INSTANCE_NAME` to your Lakebase instance.

2. **Deploy everything** (pipeline + app):
   ```bash
   apx build                      # Build the frontend + Python wheel
   databricks bundle deploy       # Deploy notebooks, job, and app to workspace
   ```

3. **Run the training pipeline:**
   ```bash
   databricks bundle run recommender_training_pipeline
   ```
   Runs all 4 notebooks (~14 min): generates synthetic data, cleans it, trains MBA and ALS models, writes recommendation lookup tables to Delta.

4. **Set up Lakebase:**

   Create a Lakebase instance (or use an existing one):
   ```bash
   databricks database create-database-instance <instance-name> --capacity CU_1
   ```

   Connect and create the tables:
   ```sql
   CREATE TABLE IF NOT EXISTS product_catalog (
       product_id TEXT PRIMARY KEY, product_name TEXT, product_slug TEXT,
       category TEXT, base_price DOUBLE PRECISION, popularity_weight DOUBLE PRECISION
   );
   CREATE TABLE IF NOT EXISTS mba_recommendations (
       product_slug TEXT PRIMARY KEY, recommendations JSONB
   );
   CREATE TABLE IF NOT EXISTS als_recommendations (
       user_id TEXT PRIMARY KEY, recommendations JSONB
   );
   CREATE TABLE IF NOT EXISTS user_profiles (
       user_id TEXT PRIMARY KEY, primary_store TEXT,
       store_visits INTEGER, total_orders INTEGER
   );
   ```

   Load data from Delta into Lakebase using the Databricks SDK and SQL warehouse, or set up Lakebase synced tables if your workspace supports UC catalog registration.

5. **Start the app:**
   ```bash
   databricks bundle run recommender_app    # Deploy and start the Databricks App
   ```
   The app URL will be shown in the output.

### Local Development

For local development and testing:
```bash
uv sync                    # Python dependencies
bun install                # JavaScript dependencies
apx components add tabs select badge card progress input button
LAKEBASE_INSTANCE_NAME=<your-instance> apx dev start   # http://localhost:9000
```

## Configuration

Edit `config.yaml` to customize:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `vertical` | `qsr` | Industry vertical: `qsr`, `retail`, `grocery` |
| `catalog` | `jdub_demo` | Unity Catalog catalog name |
| `schema` | `recommender` | Schema for all tables |
| `order_count` | `50000` | Number of synthetic orders |
| `user_count` | `2000` | Number of synthetic users |
| `recommendation_k` | `5` | Top-k recommendations to generate |
| `als_hpo_trials` | `5` | Optuna hyperparameter search trials |
| `mba_min_transactions` | `100` | Minimum transaction count for FPGrowth support threshold |

## Using Your Own Data

1. Set `source_table` in `config.yaml` to your transaction table
2. Your table must have: `user_id`, `order_id`, `order_date`, `products` (array of strings)
3. Optionally set `exclude_items` to filter non-recommendable items (sauces, utensils, etc.)
4. Skip notebook 00, run from notebook 01

## Architecture

```
config.yaml -> [00 Data Gen] -> [01 Data Prep] -> [02 MBA] -----> Lakebase -> App
                                                -> [03 ALS] ----> Lakebase -> App
```

- **All notebooks run on serverless compute** (no ML clusters required)
- **Notebook 02** uses mlxtend (single-node FPGrowth) for market basket analysis
- **Notebook 03** uses the implicit library (single-node ALS) with Optuna HPO
- **Serving**: Pre-computed lookup tables in Lakebase (low-latency PostgreSQL)
- **App**: React + FastAPI via Databricks Apps (APX), branded as "Lakehouse Market"

## Models

**Market Basket Analysis (mlxtend FPGrowth):** Finds product co-purchase patterns from transaction data. Generates association rules used to recommend items based on what's currently in the cart. Powers the anonymous/guest experience.

**Collaborative Filtering (implicit ALS):** Learns latent user preferences from order history using Alternating Least Squares with implicit feedback. Hyperparameter-tuned via Optuna. Powers personalized recommendations for known users.

Both models log experiments and metrics to MLflow.

## Demo App

The app ("Lakehouse Market") is an interactive checkout experience:

- **Browse products** in a grid with category filters
- **Known User / Guest toggle** to switch between personalized (ALS) and cart-based (MBA) recommendations
- **Add to cart** and watch recommendations update in real time
- **Match scores** displayed as inline progress bars with percentage badges
- **Settings panel** (gear icon) to customize brand colors, store name, and upload a logo
- **User profiles** showing primary store and order history for known users

## Vertical Templates

Three built-in industry templates with realistic product catalogs and co-purchase affinity patterns:

- **QSR**: 36 products (Entrees, Sides, Drinks, Desserts)
- **Retail**: 28 products (Electronics, Accessories, Clothing, Home)
- **Grocery**: 31 products (Produce, Dairy, Bakery, Pantry)

Add your own by creating a new Python file in `notebooks/verticals/` following the existing template structure.

## Pipeline Runtime

With default settings (50K orders, 2K users, 5 HPO trials):

| Task | Duration |
|------|----------|
| Data Generation | ~1 min |
| Data Preparation | ~1 min |
| Market Basket Analysis | ~9 min |
| Collaborative Filtering | ~3 min |
| **Total** | **~14 min** |
