import os
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from recommender_app.backend.routers import products, recommend

app = FastAPI(title="Recommender Accelerator", version="1.0.0")
app.include_router(products.router)
app.include_router(recommend.router)


@app.get("/api/health")
def health():
    return {"status": "ok"}


# Serve built frontend in production.
# APX builds the frontend into __dist__/ inside the package.
_dist_dir = Path(__file__).parent.parent / "__dist__"
if _dist_dir.is_dir():
    # Serve static assets (JS, CSS, images)
    app.mount("/assets", StaticFiles(directory=str(_dist_dir / "assets")), name="assets")

    # Serve index.html for all non-API routes (SPA fallback)
    @app.get("/{path:path}")
    def spa_fallback(path: str):
        file_path = _dist_dir / path
        if file_path.is_file():
            return FileResponse(str(file_path))
        return FileResponse(str(_dist_dir / "index.html"))
