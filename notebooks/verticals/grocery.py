"""Grocery vertical template."""

CATEGORIES = ["Produce", "Dairy", "Bakery", "Pantry"]

PRODUCTS = [
    # Produce
    {"name": "Bananas", "slug": "bananas", "category": "Produce", "base_price": 0.79, "popularity_weight": 10},
    {"name": "Avocados 2pk", "slug": "avocados-2pk", "category": "Produce", "base_price": 3.49, "popularity_weight": 7},
    {"name": "Baby Spinach", "slug": "baby-spinach", "category": "Produce", "base_price": 3.99, "popularity_weight": 6},
    {"name": "Roma Tomatoes", "slug": "roma-tomatoes", "category": "Produce", "base_price": 2.49, "popularity_weight": 7},
    {"name": "Yellow Onions 3lb", "slug": "yellow-onions-3lb", "category": "Produce", "base_price": 2.99, "popularity_weight": 8},
    {"name": "Red Bell Peppers", "slug": "red-bell-peppers", "category": "Produce", "base_price": 1.49, "popularity_weight": 5},
    {"name": "Lemons", "slug": "lemons", "category": "Produce", "base_price": 0.69, "popularity_weight": 6},
    {"name": "Garlic Head", "slug": "garlic-head", "category": "Produce", "base_price": 0.79, "popularity_weight": 7},
    {"name": "Cilantro Bunch", "slug": "cilantro-bunch", "category": "Produce", "base_price": 0.99, "popularity_weight": 5},
    # Dairy
    {"name": "Whole Milk Gallon", "slug": "whole-milk-gallon", "category": "Dairy", "base_price": 4.29, "popularity_weight": 10},
    {"name": "Large Eggs 12ct", "slug": "large-eggs-12ct", "category": "Dairy", "base_price": 3.99, "popularity_weight": 9},
    {"name": "Shredded Cheddar", "slug": "shredded-cheddar", "category": "Dairy", "base_price": 3.49, "popularity_weight": 7},
    {"name": "Greek Yogurt", "slug": "greek-yogurt", "category": "Dairy", "base_price": 1.29, "popularity_weight": 6},
    {"name": "Butter", "slug": "butter", "category": "Dairy", "base_price": 4.49, "popularity_weight": 8},
    {"name": "Cream Cheese", "slug": "cream-cheese", "category": "Dairy", "base_price": 2.99, "popularity_weight": 5},
    {"name": "Sour Cream", "slug": "sour-cream", "category": "Dairy", "base_price": 2.49, "popularity_weight": 5},
    # Bakery
    {"name": "Sliced White Bread", "slug": "sliced-white-bread", "category": "Bakery", "base_price": 3.49, "popularity_weight": 9},
    {"name": "Wheat Bread", "slug": "wheat-bread", "category": "Bakery", "base_price": 3.99, "popularity_weight": 6},
    {"name": "Bagels 6pk", "slug": "bagels-6pk", "category": "Bakery", "base_price": 3.49, "popularity_weight": 5},
    {"name": "Flour Tortillas", "slug": "flour-tortillas", "category": "Bakery", "base_price": 2.99, "popularity_weight": 7},
    {"name": "Hamburger Buns 8pk", "slug": "hamburger-buns-8pk", "category": "Bakery", "base_price": 2.99, "popularity_weight": 5},
    {"name": "Croissants 4pk", "slug": "croissants-4pk", "category": "Bakery", "base_price": 4.99, "popularity_weight": 4},
    # Pantry
    {"name": "Spaghetti", "slug": "spaghetti", "category": "Pantry", "base_price": 1.49, "popularity_weight": 8},
    {"name": "Marinara Sauce", "slug": "marinara-sauce", "category": "Pantry", "base_price": 3.49, "popularity_weight": 7},
    {"name": "Olive Oil", "slug": "olive-oil", "category": "Pantry", "base_price": 6.99, "popularity_weight": 7},
    {"name": "Rice 2lb", "slug": "rice-2lb", "category": "Pantry", "base_price": 2.99, "popularity_weight": 8},
    {"name": "Black Beans Can", "slug": "black-beans-can", "category": "Pantry", "base_price": 1.29, "popularity_weight": 6},
    {"name": "Chicken Broth", "slug": "chicken-broth", "category": "Pantry", "base_price": 2.49, "popularity_weight": 6},
    {"name": "Cereal", "slug": "cereal", "category": "Pantry", "base_price": 4.49, "popularity_weight": 7},
    {"name": "Peanut Butter", "slug": "peanut-butter", "category": "Pantry", "base_price": 3.99, "popularity_weight": 6},
    {"name": "Salsa Jar", "slug": "salsa-jar", "category": "Pantry", "base_price": 3.49, "popularity_weight": 5},
]

AFFINITIES = {
    ("Produce", "Dairy"): 0.45,
    ("Produce", "Bakery"): 0.30,
    ("Produce", "Pantry"): 0.50,
    ("Dairy", "Bakery"): 0.55,
    ("Dairy", "Pantry"): 0.40,
    ("Bakery", "Pantry"): 0.45,
}
