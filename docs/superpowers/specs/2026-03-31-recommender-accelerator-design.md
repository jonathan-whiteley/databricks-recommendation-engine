# Recommender Accelerator - Design Spec

**Date**: 2026-03-31
**Status**: Draft
**Author**: Jonathan Whiteley

## Overview

A configurable, demo-ready recommender system accelerator built on Databricks. Generates synthetic data for any industry vertical, trains two complementary recommendation models (Market Basket Analysis and Collaborative Filtering), serves pre-computed results via Lakebase, and presents an interactive checkout-style demo app via APX (React + FastAPI).

Designed as a public-facing repo that customers can clone, point at their own transaction data, and see end-to-end results.

## Goals

1. Replace hardcoded KFC-specific notebooks with a configurable, vertical-agnostic pipeline
2. Run data prep on serverless compute; training on ML clusters
3. Pre-compute recommendations into Lakebase for low-latency app serving
4. Ship an interactive demo app that feels like a real checkout experience
5. Package as a Databricks Asset Bundle for one-command deployment

## Non-Goals

- Real-time model inference at request time (we use pre-computed lookup tables)
- Supporting non-Databricks environments
- Building a production-grade e-commerce frontend

## Architecture

```
config.yaml
    |
    v
[00_data_generation.py]  ──serverless──>  raw_orders (Delta)
    |
    v
[01_data_preparation.py] ──serverless──>  cleaned_orders (Delta)
    |                                     user_item_ratings (Delta)
    |
    ├──────────────────────┐
    v                      v
[02_market_basket.py]    [03_collaborative_filter.py]
    ML cluster               ML cluster
    |                        |
    v                        v
mba_recommendations       als_recommendations
    (Delta)                  (Delta)
    |                        |
    └──── Lakebase Sync ─────┘
              |
              v
         [APX Demo App]
         FastAPI + React
```

## Configuration

All notebooks read from a shared `config.yaml` at the repo root.

```yaml
# Industry vertical: qsr | retail | grocery
vertical: qsr

# Databricks catalog and schema
catalog: jdub_demo
schema: recommender

# Data generation parameters
product_count: 50
order_count: 500000
user_count: 10000
seed: 42

# Model parameters
mba_min_transactions: 1000
mba_min_confidence: 0.0
als_hpo_trials: 20
recommendation_k: 5
```

### Vertical Templates

Each vertical defines a product catalog and co-purchase affinity matrix. Stored as Python dicts in a `verticals/` module within the notebooks folder.

**QSR template**: Entrees (burgers, chicken, sandwiches), Sides (fries, coleslaw, mashed potatoes), Drinks (sodas, shakes, iced tea), Desserts (cookies, pies, cakes). High affinity: entree+side, entree+drink. Low affinity: dessert+salad.

**Retail template**: Electronics (phones, tablets, headphones), Accessories (cases, chargers, screen protectors), Clothing (shirts, pants, shoes), Home (candles, frames, pillows). High affinity: phone+case, tablet+charger.

**Grocery template**: Produce (fruits, vegetables), Dairy (milk, cheese, yogurt), Bakery (bread, bagels, muffins), Pantry (pasta, sauce, cereal). High affinity: pasta+sauce, bread+butter.

Customers can add their own vertical by creating a new template file following the same structure.

### Customer Data Swap

To use real data instead of synthetic:
1. Skip notebook 00 (data generation)
2. Point notebook 01 at their source table by updating `config.yaml` with a `source_table` field
3. Define their exclusion list in config (items not to recommend: sauces, utensils, etc.)
4. Map their product names to slugs via a mapping table or config entry

## Notebooks

### 00_data_generation.py (Serverless)

**Purpose**: Generate realistic synthetic transaction data from config.

**Inputs**: `config.yaml`

**Process**:
- Read vertical template for the configured industry
- Generate product catalog table with columns: `product_id`, `product_name`, `product_slug`, `category`
- Generate user table: `user_id`, `created_date`
- Generate orders using Faker + Spark:
  - Power-law distribution for product popularity (some items ordered frequently, long tail of rare items)
  - Co-purchase affinity matrix influences basket composition (if burger is in cart, 70% chance fries are added)
  - Variable basket sizes (1-8 items, weighted toward 2-4)
  - Date range: trailing 12 months
  - Store IDs: 50 synthetic stores

**Outputs** (Delta tables in `{catalog}.{schema}`):
- `raw_orders`: `user_id STRING, order_id STRING, order_date DATE, store_id STRING, products ARRAY<STRING>`
- `product_catalog`: `product_id STRING, product_name STRING, product_slug STRING, category STRING`

### 01_data_preparation.py (Serverless)

**Purpose**: Clean transactions and prepare features for both models.

**Inputs**: `raw_orders`, `product_catalog`, `config.yaml`

**Process**:
- Filter null/empty orders
- Apply configurable exclusion list (read from config, not hardcoded)
- Map product names to slugs via product_catalog table join (no UDF needed; join is serverless-friendly)
- Drop empty baskets post-filtering
- Generate user-item implicit ratings for ALS: for each (user, item) pair, calculate `proportion_of_orders = count(user, item) / count(user)`

**Outputs** (Delta):
- `cleaned_orders`: `user_id, order_id, order_date, store_id, order_product_list ARRAY<STRING>`
- `user_item_ratings`: `user_id, item_id, product_slug, proportion_of_orders DOUBLE`

### 02_market_basket.py (ML Cluster - Single User)

**Purpose**: Train FPGrowth, generate association rules, write pre-computed MBA lookup table.

**Inputs**: `cleaned_orders`, config params

**Process**:
1. EDA: calculate support for all products, visualize top 25
2. Train FPGrowth on 80/20 split with config-driven `min_support` and `min_confidence`
3. Save raw association rules to `mba_rules` table
4. Evaluate: Hit@k on test set using the `generate_recommendations()` scoring function (confidence * match_score)
5. Log model and metrics to MLflow
6. **Generate pre-computed lookup table**: For every product in the catalog, find top-k recommendations with scores by running the scoring function against all rules. Write to `mba_recommendations`.
7. Sync `mba_recommendations` to Lakebase

**Outputs** (Delta + Lakebase):
- `mba_rules`: `antecedent ARRAY<STRING>, consequent STRING, lift DOUBLE, confidence DOUBLE`
- `mba_recommendations`: `product_slug STRING (PK), recommendations JSONB` where recommendations is `[{product: STRING, score: DOUBLE}, ...]`

### 03_collaborative_filter.py (ML Cluster - Single User)

**Purpose**: Train ALS with HPO, generate per-user recommendations, write pre-computed ALS lookup table.

**Inputs**: `cleaned_orders`, `user_item_ratings`, config params

**Process**:
1. Build user/item integer mappings for ALS
2. Train initial ALS model, evaluate Hit@k
3. Optuna HPO: search over rank (1-100) and maxIter (1-10), n_trials from config. Nested MLflow runs.
4. Retrain final model on full dataset with best params
5. Log final model + mappings to MLflow
6. **Generate pre-computed lookup table**: `model.recommendForAllUsers(k)`, map item IDs back to product slugs with scores. Write to `als_recommendations`.
7. Sync `als_recommendations` to Lakebase

**Outputs** (Delta + Lakebase):
- `als_user_mapping`: `user DOUBLE, user_id INT`
- `als_item_mapping`: `item STRING, item_id INT`
- `als_recommendations`: `user_id STRING (PK), recommendations JSONB` where recommendations is `[{product: STRING, score: DOUBLE}, ...]`

## Lakebase Serving Layer

Two tables synced from Delta to Lakebase for low-latency reads:

| Table | Primary Key | Queried By | Use Case |
|---|---|---|---|
| `mba_recommendations` | `product_slug` | Cart items | Anonymous user: look up recs for each cart item, merge and re-rank |
| `als_recommendations` | `user_id` | User ID | Known user: direct lookup of personalized recs |

**Sync mechanism**: Lakebase managed sync from Delta tables. Each training notebook triggers a sync after writing the Delta table. When the scheduled job reruns, tables refresh automatically.

**App queries Lakebase** via PostgreSQL wire protocol (psycopg2 from FastAPI). Connection string from Databricks app resources config.

## Scheduled Retraining

A Databricks Job defined in `databricks.yml`:

```
00_data_generation (serverless)
    └──> 01_data_preparation (serverless)
              ├──> 02_market_basket (ML cluster)
              └──> 03_collaborative_filter (ML cluster)
```

- 02 and 03 run in parallel after 01 completes
- **Schedule: off by default** (no cron). Customer enables when ready.
- For demo purposes: run manually or trigger via CLI
- For production: customer sets their own cadence (daily, weekly)

## Demo App (APX: React + FastAPI)

### Backend (FastAPI)

**Endpoints**:
- `GET /api/products` — full product catalog with categories for the cart builder
- `GET /api/users?limit=20` — sample user IDs for the known-user dropdown
- `POST /api/recommend` — main recommendation endpoint

**Recommend request/response**:
```json
// Request
{
  "mode": "known",        // "known" | "anonymous"
  "user_id": "user_123",  // required if mode=known
  "cart": ["burger-deluxe", "classic-fries"]  // optional for known, required for anonymous
}

// Response
{
  "recommendations": [
    {"product": "vanilla-shake", "score": 0.87, "rank": 1},
    {"product": "chocolate-cookie", "score": 0.72, "rank": 2},
    {"product": "onion-rings", "score": 0.65, "rank": 3},
    {"product": "iced-tea", "score": 0.58, "rank": 4},
    {"product": "apple-pie", "score": 0.41, "rank": 5}
  ],
  "mode": "known",
  "source": "als"  // "als" | "mba"
}
```

**Recommendation logic**:
- Known user with empty cart: return ALS recs directly
- Known user with cart items: return ALS recs, filtered to exclude items already in cart
- Anonymous with cart: query `mba_recommendations` for each cart item, merge all candidates, sum scores for items recommended by multiple cart items, re-rank, return top 5 excluding items in cart

### Frontend (React + shadcn/ui)

**Layout**: Single-page checkout experience.

**Top bar**: Mode toggle (Known User / Anonymous). Known User mode shows a user ID search/select dropdown.

**Main area - Product Catalog**: Grid of product cards organized by category. Each card shows product name, category tag, and an "Add to Cart" button. Searchable/filterable.

**Side panel - Cart**: Running list of cart items with quantities and a running total (synthetic prices). Remove button per item. "Clear Cart" action.

**Below cart - Recommendations**: "You might also like" section. Shows top 5 recommended products as cards, each displaying:
- Product name
- Relevance score (as a percentage bar or badge, e.g., "87% match")
- "Add to Cart" button

**Interactive behavior**: Clicking "Add to Cart" on a recommendation immediately adds it to the cart and fires a new `/api/recommend` call. Recommendations refresh in real time as the cart changes. This creates the demo "wow" moment: add a burger, see fries at 87%; add fries, watch the recommendations shift to drinks and desserts.

**Design**: Clean, professional checkout aesthetic. Not a toy; something that looks plausible as a real product. Card-based layout, subtle shadows, category color coding.

## Databricks Asset Bundle (databricks.yml)

Defines:
- Job with 4 notebook tasks (dependency graph, schedule off)
- ML cluster config for notebooks 02 and 03
- App deployment config
- Lakebase database and sync resources
- Variables for catalog, schema, cluster overrides

Supports `databricks bundle deploy` for one-command setup.

## MLflow Integration

- Notebook 02 logs FPGrowth rules as a PyFunc model with packaged rules parquet artifact
- Notebook 03 logs ALS model + user/item mappings, nested HPO runs under a parent
- Experiment path: configurable, defaults to `/Shared/recommender-accelerator/{model_name}`
- Registered model names: `{catalog}.{schema}.mba_recommender` and `{catalog}.{schema}.als_recommender`

## Customer Adoption Path

1. Clone repo
2. Edit `config.yaml`: set catalog/schema, optionally change vertical or point to real source table
3. `databricks bundle deploy`
4. Run the job manually (or enable schedule)
5. Open the app, demo recommendations
6. To use real data: skip notebook 00, update `source_table` in config, define exclusion list, run from notebook 01

## Open Questions

None. All decisions resolved during brainstorming.
