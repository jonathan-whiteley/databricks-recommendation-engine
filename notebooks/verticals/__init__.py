"""
Vertical templates for synthetic data generation.
Each vertical defines a product catalog and co-purchase affinity matrix.
"""

from importlib import import_module

VERTICALS = ["qsr", "retail", "grocery"]


def get_vertical(name: str) -> dict:
    """Load a vertical template by name. Returns dict with keys: products, categories, affinities."""
    if name not in VERTICALS:
        raise ValueError(f"Unknown vertical '{name}'. Choose from: {VERTICALS}")
    mod = import_module(f"verticals.{name}")
    return {
        "products": mod.PRODUCTS,
        "categories": mod.CATEGORIES,
        "affinities": mod.AFFINITIES,
    }
