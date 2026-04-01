from pathlib import Path

app_name = "recommender-app"
app_entrypoint = "recommender_app.backend.app:app"
app_slug = "recommender_app"
api_prefix = "/api"
dist_dir = Path(__file__).parent / "__dist__"