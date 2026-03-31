"""Retail / E-commerce vertical template."""

CATEGORIES = ["Electronics", "Accessories", "Clothing", "Home"]

PRODUCTS = [
    # Electronics
    {"name": "Smartphone Pro", "slug": "smartphone-pro", "category": "Electronics", "base_price": 999.99, "popularity_weight": 10},
    {"name": "Wireless Earbuds", "slug": "wireless-earbuds", "category": "Electronics", "base_price": 129.99, "popularity_weight": 9},
    {"name": "Tablet 10in", "slug": "tablet-10in", "category": "Electronics", "base_price": 449.99, "popularity_weight": 7},
    {"name": "Bluetooth Speaker", "slug": "bluetooth-speaker", "category": "Electronics", "base_price": 59.99, "popularity_weight": 6},
    {"name": "Laptop Stand", "slug": "laptop-stand", "category": "Electronics", "base_price": 49.99, "popularity_weight": 5},
    {"name": "Webcam HD", "slug": "webcam-hd", "category": "Electronics", "base_price": 79.99, "popularity_weight": 4},
    {"name": "Portable Charger", "slug": "portable-charger", "category": "Electronics", "base_price": 39.99, "popularity_weight": 8},
    {"name": "Smart Watch", "slug": "smart-watch", "category": "Electronics", "base_price": 249.99, "popularity_weight": 7},
    {"name": "Noise-Canceling Headphones", "slug": "noise-canceling-headphones", "category": "Electronics", "base_price": 299.99, "popularity_weight": 6},
    # Accessories
    {"name": "Phone Case", "slug": "phone-case", "category": "Accessories", "base_price": 29.99, "popularity_weight": 9},
    {"name": "Screen Protector", "slug": "screen-protector", "category": "Accessories", "base_price": 12.99, "popularity_weight": 8},
    {"name": "USB-C Cable", "slug": "usb-c-cable", "category": "Accessories", "base_price": 14.99, "popularity_weight": 8},
    {"name": "Tablet Case", "slug": "tablet-case", "category": "Accessories", "base_price": 39.99, "popularity_weight": 6},
    {"name": "Watch Band", "slug": "watch-band", "category": "Accessories", "base_price": 24.99, "popularity_weight": 5},
    {"name": "Charging Dock", "slug": "charging-dock", "category": "Accessories", "base_price": 44.99, "popularity_weight": 5},
    {"name": "Laptop Sleeve", "slug": "laptop-sleeve", "category": "Accessories", "base_price": 34.99, "popularity_weight": 4},
    # Clothing
    {"name": "Graphic Tee", "slug": "graphic-tee", "category": "Clothing", "base_price": 24.99, "popularity_weight": 7},
    {"name": "Hoodie", "slug": "hoodie", "category": "Clothing", "base_price": 49.99, "popularity_weight": 6},
    {"name": "Running Shoes", "slug": "running-shoes", "category": "Clothing", "base_price": 89.99, "popularity_weight": 5},
    {"name": "Baseball Cap", "slug": "baseball-cap", "category": "Clothing", "base_price": 19.99, "popularity_weight": 5},
    {"name": "Athletic Socks 3pk", "slug": "athletic-socks-3pk", "category": "Clothing", "base_price": 14.99, "popularity_weight": 6},
    {"name": "Joggers", "slug": "joggers", "category": "Clothing", "base_price": 39.99, "popularity_weight": 4},
    # Home
    {"name": "Scented Candle", "slug": "scented-candle", "category": "Home", "base_price": 19.99, "popularity_weight": 5},
    {"name": "Photo Frame", "slug": "photo-frame", "category": "Home", "base_price": 14.99, "popularity_weight": 3},
    {"name": "Throw Pillow", "slug": "throw-pillow", "category": "Home", "base_price": 24.99, "popularity_weight": 4},
    {"name": "Desk Organizer", "slug": "desk-organizer", "category": "Home", "base_price": 29.99, "popularity_weight": 4},
    {"name": "LED Desk Lamp", "slug": "led-desk-lamp", "category": "Home", "base_price": 34.99, "popularity_weight": 5},
    {"name": "Mug Set", "slug": "mug-set", "category": "Home", "base_price": 18.99, "popularity_weight": 4},
]

AFFINITIES = {
    ("Electronics", "Accessories"): 0.80,
    ("Electronics", "Clothing"): 0.10,
    ("Electronics", "Home"): 0.15,
    ("Accessories", "Clothing"): 0.10,
    ("Accessories", "Home"): 0.10,
    ("Clothing", "Home"): 0.15,
}
