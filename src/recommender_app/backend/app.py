from fastapi import FastAPI
from recommender_app.backend.routers import products, recommend

app = FastAPI(title="Recommender Accelerator", version="1.0.0")
app.include_router(products.router)
app.include_router(recommend.router)

@app.get("/api/health")
def health():
    return {"status": "ok"}
