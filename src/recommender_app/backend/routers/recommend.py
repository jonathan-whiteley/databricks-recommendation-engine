"""Recommendation endpoint: serves pre-computed results from Lakebase."""

import json
from typing import Literal

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from recommender_app.backend.db import get_connection

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
