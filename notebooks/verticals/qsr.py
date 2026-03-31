"""QSR (Quick Service Restaurant) vertical template."""

CATEGORIES = ["Entrees", "Sides", "Drinks", "Desserts"]

PRODUCTS = [
    # Entrees
    {"name": "Classic Burger", "slug": "classic-burger", "category": "Entrees", "base_price": 7.99, "popularity_weight": 10},
    {"name": "Double Cheeseburger", "slug": "double-cheeseburger", "category": "Entrees", "base_price": 9.49, "popularity_weight": 8},
    {"name": "Crispy Chicken Sandwich", "slug": "crispy-chicken-sandwich", "category": "Entrees", "base_price": 8.49, "popularity_weight": 9},
    {"name": "Spicy Chicken Sandwich", "slug": "spicy-chicken-sandwich", "category": "Entrees", "base_price": 8.99, "popularity_weight": 7},
    {"name": "Grilled Chicken Wrap", "slug": "grilled-chicken-wrap", "category": "Entrees", "base_price": 7.49, "popularity_weight": 5},
    {"name": "Fish Sandwich", "slug": "fish-sandwich", "category": "Entrees", "base_price": 6.99, "popularity_weight": 3},
    {"name": "Veggie Burger", "slug": "veggie-burger", "category": "Entrees", "base_price": 7.99, "popularity_weight": 3},
    {"name": "BBQ Bacon Burger", "slug": "bbq-bacon-burger", "category": "Entrees", "base_price": 10.49, "popularity_weight": 6},
    {"name": "Chicken Tenders 3pc", "slug": "chicken-tenders-3pc", "category": "Entrees", "base_price": 6.99, "popularity_weight": 8},
    {"name": "Chicken Tenders 5pc", "slug": "chicken-tenders-5pc", "category": "Entrees", "base_price": 9.49, "popularity_weight": 7},
    {"name": "Nuggets 6pc", "slug": "nuggets-6pc", "category": "Entrees", "base_price": 5.49, "popularity_weight": 9},
    {"name": "Nuggets 10pc", "slug": "nuggets-10pc", "category": "Entrees", "base_price": 7.99, "popularity_weight": 6},
    # Sides
    {"name": "Classic Fries", "slug": "classic-fries", "category": "Sides", "base_price": 3.49, "popularity_weight": 10},
    {"name": "Curly Fries", "slug": "curly-fries", "category": "Sides", "base_price": 3.99, "popularity_weight": 6},
    {"name": "Onion Rings", "slug": "onion-rings", "category": "Sides", "base_price": 3.99, "popularity_weight": 5},
    {"name": "Mashed Potatoes", "slug": "mashed-potatoes", "category": "Sides", "base_price": 2.99, "popularity_weight": 4},
    {"name": "Coleslaw", "slug": "coleslaw", "category": "Sides", "base_price": 2.49, "popularity_weight": 3},
    {"name": "Mac & Cheese", "slug": "mac-and-cheese", "category": "Sides", "base_price": 3.49, "popularity_weight": 5},
    {"name": "Side Salad", "slug": "side-salad", "category": "Sides", "base_price": 3.99, "popularity_weight": 2},
    {"name": "Corn on the Cob", "slug": "corn-on-the-cob", "category": "Sides", "base_price": 2.49, "popularity_weight": 3},
    {"name": "Biscuit", "slug": "biscuit", "category": "Sides", "base_price": 1.49, "popularity_weight": 7},
    {"name": "Loaded Fries", "slug": "loaded-fries", "category": "Sides", "base_price": 4.99, "popularity_weight": 4},
    # Drinks
    {"name": "Fountain Soda", "slug": "fountain-soda", "category": "Drinks", "base_price": 2.29, "popularity_weight": 10},
    {"name": "Iced Tea", "slug": "iced-tea", "category": "Drinks", "base_price": 2.29, "popularity_weight": 6},
    {"name": "Lemonade", "slug": "lemonade", "category": "Drinks", "base_price": 2.49, "popularity_weight": 5},
    {"name": "Vanilla Shake", "slug": "vanilla-shake", "category": "Drinks", "base_price": 4.99, "popularity_weight": 5},
    {"name": "Chocolate Shake", "slug": "chocolate-shake", "category": "Drinks", "base_price": 4.99, "popularity_weight": 5},
    {"name": "Strawberry Shake", "slug": "strawberry-shake", "category": "Drinks", "base_price": 4.99, "popularity_weight": 4},
    {"name": "Coffee", "slug": "coffee", "category": "Drinks", "base_price": 1.99, "popularity_weight": 4},
    {"name": "Bottled Water", "slug": "bottled-water", "category": "Drinks", "base_price": 1.49, "popularity_weight": 3},
    # Desserts
    {"name": "Chocolate Chip Cookie", "slug": "chocolate-chip-cookie", "category": "Desserts", "base_price": 1.99, "popularity_weight": 6},
    {"name": "Apple Pie", "slug": "apple-pie", "category": "Desserts", "base_price": 2.49, "popularity_weight": 5},
    {"name": "Brownie", "slug": "brownie", "category": "Desserts", "base_price": 2.49, "popularity_weight": 4},
    {"name": "Soft Serve Cone", "slug": "soft-serve-cone", "category": "Desserts", "base_price": 1.49, "popularity_weight": 5},
    {"name": "Sundae", "slug": "sundae", "category": "Desserts", "base_price": 3.49, "popularity_weight": 3},
    {"name": "Cinnamon Sticks", "slug": "cinnamon-sticks", "category": "Desserts", "base_price": 2.99, "popularity_weight": 3},
]

# Co-purchase affinity: probability that a second item from category B is added
# given an item from category A is already in the cart.
# Higher = more likely to appear together.
AFFINITIES = {
    ("Entrees", "Sides"): 0.75,
    ("Entrees", "Drinks"): 0.65,
    ("Entrees", "Desserts"): 0.20,
    ("Sides", "Drinks"): 0.40,
    ("Sides", "Desserts"): 0.15,
    ("Drinks", "Desserts"): 0.25,
}
