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
