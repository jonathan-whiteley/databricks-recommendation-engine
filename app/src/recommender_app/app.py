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
