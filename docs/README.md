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
