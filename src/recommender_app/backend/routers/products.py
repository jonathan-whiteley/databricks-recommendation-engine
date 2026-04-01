"""Product catalog and user listing endpoints."""

from fastapi import APIRouter
from pydantic import BaseModel

from recommender_app.backend.db import get_connection

router = APIRouter(prefix="/api", tags=["products"])


class Product(BaseModel):
    product_id: str
    product_name: str
    product_slug: str
    category: str
    base_price: float


class UserInfo(BaseModel):
    user_id: str
    primary_store: str | None = None
    total_orders: int | None = None


class UserProfile(BaseModel):
    user_id: str
    primary_store: str | None = None
    store_visits: int | None = None
    total_orders: int | None = None


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
    """Return sample user IDs with store info for the known-user dropdown."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT a.user_id, p.primary_store, p.total_orders "
                "FROM als_recommendations a "
                "LEFT JOIN user_profiles p ON a.user_id = p.user_id "
                "ORDER BY a.user_id LIMIT %s",
                (limit,),
            )
            rows = cur.fetchall()
    return [UserInfo(user_id=r[0], primary_store=r[1], total_orders=r[2]) for r in rows]


@router.get("/users/{user_id}", response_model=UserProfile)
def get_user(user_id: str):
    """Get profile for a specific user."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT user_id, primary_store, store_visits, total_orders "
                "FROM user_profiles WHERE user_id = %s",
                (user_id,),
            )
            row = cur.fetchone()
    if not row:
        return UserProfile(user_id=user_id)
    return UserProfile(user_id=row[0], primary_store=row[1], store_visits=row[2], total_orders=row[3])
