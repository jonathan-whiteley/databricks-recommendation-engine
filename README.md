# Databricks Recommendation Engine

[![Deploy with DABS](https://img.shields.io/badge/Deploy%20with-Databricks%20Asset%20Bundles-FF3621?logo=databricks&logoColor=white)](https://docs.databricks.com/aws/en/dev-tools/bundles/)
[![Built with apx](https://img.shields.io/badge/Built%20with-apx-1F2937)](https://github.com/databricks/apx)
[![Lakebase](https://img.shields.io/badge/Serving-Lakebase%20Postgres-336791?logo=postgresql&logoColor=white)](https://docs.databricks.com/aws/en/oltp/)
[![Python 3.11+](https://img.shields.io/badge/Python-3.11%2B-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![React 19](https://img.shields.io/badge/React-19-61DAFB?logo=react&logoColor=black)](https://react.dev/)

> A configurable, demo-ready product recommendation system on Databricks. One bundle deploys the training job, the Databricks App, and the Lakebase binding. One command runs the pipeline. Open the app and you have a checkout experience powered by MBA and ALS.

## What's Inside

| Layer | Tech | Purpose |
|---|---|---|
| Data | Synthetic generator (QSR, retail, grocery) or your own UC table | Drive the pipeline with realistic transactions |
| Models | PySpark FPGrowth (MBA), PySpark ALS with Optuna HPO | Cart-based and personalized recommendations |
| Serving | Lakebase (managed Postgres) | Low-latency lookup tables for the app |
| App | React 19, FastAPI, shadcn/ui via apx | "Lakehouse Market" interactive demo |
| Orchestration | Databricks Asset Bundle (`databricks.yml`) | Deploys job, app, and Lakebase resource binding |

## Why Databricks

| Capability | What you get |
|---|---|
| Asset Bundles | Single `databricks bundle deploy` provisions notebooks, job, app, and resource bindings |
| Databricks Apps | Hosted React + FastAPI app with OAuth and a service principal, no separate infra |
| Lakebase | Managed Postgres bound to the app via DABS resources; sub-100ms lookups |
| Unity Catalog | Catalog and schema scoping for every table the pipeline writes |
| MLflow | Experiment tracking and metrics for both MBA and ALS |
| Serverless + Classic | Serverless for prep and sync, single-user ML clusters for distributed training |

## Quick Start

You need: a Databricks workspace, the [Databricks CLI](https://docs.databricks.com/aws/en/dev-tools/cli/install) (v0.220+), [uv](https://docs.astral.sh/uv/getting-started/installation/), and [bun](https://bun.com/docs/installation).

### 1. Clone

```bash
git clone https://github.com/jonathan-whiteley/databricks-recommendation-engine.git
cd databricks-recommendation-engine
```

### 2. Create a Lakebase instance (one-time)

```bash
databricks database create-database-instance jdub-lakebase-db-instance --capacity CU_1
```

Pick any name; just keep it consistent in the next step.

### 3. Configure

Edit `config.yaml`:

```yaml
catalog: <your_catalog>
schema: <your_schema>
vertical: qsr           # qsr | retail | grocery
lakebase_instance: <your_instance_name>
```

Edit `databricks.yml` and set the same `lakebase_instance` in the variables block.

### 4. Deploy the bundle

```bash
apx build                 # bundle frontend + backend wheel into .build/
databricks bundle deploy  # push notebooks, job, and app to the workspace
```

This single deploy creates the training job, the Databricks App, and the Lakebase resource binding for the app's service principal.

### 5. Train the models

```bash
databricks bundle run recommender_training_pipeline
```

Runs the five-notebook pipeline (around 20 minutes). It generates synthetic data, prepares it, trains MBA and ALS in parallel on single-user ML clusters, and syncs the results to Lakebase.

### 6. Grant the app access to its tables (one-time)

After the first pipeline run, give the app's service principal `SELECT` on the four lookup tables. You can find the SP id with `databricks apps get recommender-accelerator-dev` (it's the `service_principal_client_id`):

```sql
GRANT SELECT ON product_catalog       TO `<sp-client-id>`;
GRANT SELECT ON mba_recommendations   TO `<sp-client-id>`;
GRANT SELECT ON als_recommendations   TO `<sp-client-id>`;
GRANT SELECT ON user_profiles         TO `<sp-client-id>`;
```

### 7. Launch the app

```bash
databricks bundle run recommender_app
```

The app URL is printed in the output. Open it in your browser; you'll be prompted to log in with your Databricks account.

## Local Development

```bash
uv sync                                  # Python dependencies
bun install                              # JavaScript dependencies
apx components add tabs select badge card progress input button
LAKEBASE_INSTANCE_NAME=<your-instance> apx dev start   # http://localhost:9000
```

`apx dev start` runs the FastAPI backend and the Vite dev server with HMR. The backend authenticates to Lakebase using your local Databricks profile credentials.

## Architecture

```
                                config.yaml
                                     |
                                     v
            +----------------------------------------------+
            |         Bundle: recommender-accelerator      |
            +----------------------------------------------+
                |                                       |
                v                                       v
     +-------------------+                     +------------------+
     |   Training Job    |                     |  Databricks App  |
     | (5 notebooks)     |                     | "Lakehouse Mkt." |
     +-------------------+                     +------------------+
        |                                              |
        | 00 data gen   (serverless)                   |
        | 01 data prep  (serverless)                   |
        | 02 MBA        (single-user ML cluster) -+    |
        | 03 ALS        (single-user ML cluster) -+    |
        | 04 lakebase sync (serverless)           |    |
        +-----------------------------------------+    |
                                |                     |
                                v                     v
                         +-----------------------------+
                         |   Lakebase (Postgres)       |
                         |   product_catalog           |
                         |   mba_recommendations       |
                         |   als_recommendations       |
                         |   user_profiles             |
                         +-----------------------------+
```

Notebooks 02 and 03 run in parallel; notebook 04 fans them back in for the Lakebase sync.

## What the Bundle Creates

| Resource | Type | Notes |
|---|---|---|
| `recommender_training_pipeline` | Job | 5 notebook tasks; classic ML clusters auto-provisioned for 02 and 03 |
| `recommender_app` | Databricks App | React + FastAPI, served from `./.build/` |
| `lakebase` (app resource) | Database binding | `CAN_CONNECT_AND_CREATE` on `databricks_postgres` |

The default target is `dev` using the `azure` workspace profile. Add a `prod` target in `databricks.yml` to deploy to a separate workspace.

## Configuration

`config.yaml` is the single source of truth for the pipeline.

| Parameter | Default | Description |
|---|---|---|
| `vertical` | `qsr` | Industry template: `qsr`, `retail`, `grocery` |
| `catalog` | `jdub_demo` | Unity Catalog catalog |
| `schema` | `recommender` | Schema for all pipeline tables |
| `source_table` | unset | Optional: point at your own UC table to skip notebook 00 |
| `exclude_items` | `[]` | Items filtered from recommendations (sauces, utensils, etc.) |
| `order_count` | `500000` | Synthetic order count |
| `user_count` | `10000` | Synthetic user count |
| `recommendation_k` | `5` | Top-k recommendations to serve |
| `mba_min_transactions` | `1000` | FPGrowth support threshold |
| `als_hpo_trials` | `20` | Optuna HPO trials |
| `lakebase_instance` | `jdub-lakebase-db-instance` | Must exist before deploy |
| `app_name` | `recommender-accelerator` | App name (used to look up the SP) |

## Using Your Own Data

1. Set `source_table` in `config.yaml` to a UC table you can read.
2. Required columns: `user_id STRING`, `order_id STRING`, `order_date DATE`, `products ARRAY<STRING>`.
3. Optionally fill `exclude_items` to drop non-recommendable products.
4. Skip notebook 00 and run from notebook 01.

The job wiring is unchanged; only the data source flips.

<details>
<summary><strong>Models</strong></summary>

**Market Basket Analysis (PySpark FPGrowth).** Mines product co-purchase rules from transactions. Powers the guest experience: given the cart, suggest the next item.

**Collaborative Filtering (PySpark ALS).** Implicit-feedback ALS over the user-product matrix, hyperparameter-tuned via Optuna. Powers personalized recommendations for known users.

Both models log experiments to MLflow under `mlflow_experiment_root` (configurable in `config.yaml`).

A serverless fallback exists for workspaces without classic compute: `02_market_basket_serverless.py` (mlxtend) and `03_collaborative_filter_serverless.py` (implicit).
</details>

<details>
<summary><strong>Demo App: Lakehouse Market</strong></summary>

- Browse the product grid with category filters
- Toggle Known User vs Guest to swap between ALS and MBA
- Add to cart and watch recommendations refresh in real time
- Match scores rendered as inline progress bars
- Settings panel for brand color, store name, and logo upload
- User profile view shows primary store and order history

Backend: FastAPI + psycopg2 against Lakebase. Frontend: React 19, TanStack Router and Query, shadcn/ui, Tailwind 4.
</details>

<details>
<summary><strong>Vertical Templates</strong></summary>

| Vertical | Categories | Products |
|---|---|---|
| `qsr` | Entrees, Sides, Drinks, Desserts | 36 |
| `retail` | Electronics, Accessories, Clothing, Home | 28 |
| `grocery` | Produce, Dairy, Bakery, Pantry | 31 |

Add a new vertical by dropping a Python file into `notebooks/verticals/` that follows the existing template structure.
</details>

<details>
<summary><strong>Pipeline Runtime</strong></summary>

Defaults: 500K orders, 10K users, 20 HPO trials, two `Standard_E4ds_v4` workers per ML cluster.

| Task | Compute | Duration |
|---|---|---|
| 00 Data Generation | Serverless | ~2 min |
| 01 Data Preparation | Serverless | ~2 min |
| 02 Market Basket | ML Cluster | ~5 min plus cluster startup |
| 03 Collaborative Filter | ML Cluster | ~10 min plus cluster startup |
| 04 Lakebase Sync | Serverless | ~1 min |
| **Total** | | **~20 min** |

Cold start adds 5 to 10 minutes on the first run. Tasks 02 and 03 share a cluster spec, so subsequent runs reuse a warm cluster.
</details>

<details>
<summary><strong>Project Layout</strong></summary>

```
.
├── databricks.yml              # Bundle: job, app, Lakebase binding
├── config.yaml                 # Pipeline configuration
├── app.yml                     # App runtime command + env
├── pyproject.toml              # Backend deps + apx metadata
├── package.json                # Frontend deps
├── notebooks/
│   ├── 00_data_generation.py
│   ├── 01_data_preparation.py
│   ├── 02_market_basket.py            (and *_serverless.py)
│   ├── 03_collaborative_filter.py     (and *_serverless.py)
│   ├── 04_lakebase_sync.py
│   ├── config_loader.py
│   └── verticals/{qsr,retail,grocery}.py
└── src/recommender_app/
    ├── backend/                # FastAPI app
    └── ui/                     # React + shadcn UI
```

</details>

## Troubleshooting

| Symptom | Fix |
|---|---|
| `bundle deploy` fails on Lakebase resource | Confirm the Lakebase instance exists and the name matches in both `config.yaml` and `databricks.yml` |
| App returns 403 reading tables | Re-run the GRANT block in step 6 with the SP from `databricks apps get` |
| Job fails on notebook 02 or 03 | Workspace lacks classic compute access; switch to the `*_serverless.py` variants |
| Local `apx dev start` cannot reach Lakebase | Ensure `LAKEBASE_INSTANCE_NAME` is exported and your Databricks CLI profile is authenticated |

## License

Internal accelerator. Use within your Databricks engagement.
