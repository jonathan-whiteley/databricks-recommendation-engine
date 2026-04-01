import { useState, useCallback, useEffect } from "react";
import { createFileRoute } from "@tanstack/react-router";
import { ModeToggle } from "@/components/mode-toggle";
import { UserSearch } from "@/components/user-search";
import { ProductGrid } from "@/components/product-grid";
import { CartPanel } from "@/components/cart-panel";
import { Recommendations } from "@/components/recommendations";

export const Route = createFileRoute("/")({ component: HomePage });

interface Product {
  product_id: string;
  product_name: string;
  product_slug: string;
  category: string;
  base_price: number;
}

interface CartItem {
  slug: string;
  name: string;
  price: number;
  quantity: number;
}

interface Rec {
  product: string;
  score: number;
  rank: number;
}

interface UserProfile {
  user_id: string;
  primary_store: string | null;
  store_visits: number | null;
  total_orders: number | null;
}

function HomePage() {
  const [mode, setMode] = useState<"known" | "anonymous">("anonymous");
  const [selectedUser, setSelectedUser] = useState<string | null>(null);
  const [products, setProducts] = useState<Product[]>([]);
  const [users, setUsers] = useState<{ user_id: string }[]>([]);
  const [cart, setCart] = useState<CartItem[]>([]);
  const [recommendations, setRecommendations] = useState<Rec[]>([]);
  const [source, setSource] = useState("");
  const [loading, setLoading] = useState(false);
  const [userProfile, setUserProfile] = useState<UserProfile | null>(null);

  const productMap = Object.fromEntries(products.map((p) => [p.product_slug, p]));
  const productNames = Object.fromEntries(products.map((p) => [p.product_slug, p.product_name]));
  const productPrices = Object.fromEntries(products.map((p) => [p.product_slug, p.base_price]));
  const cartSlugs = new Set(cart.map((c) => c.slug));

  useEffect(() => {
    fetch("/api/products").then((r) => r.json()).then(setProducts);
    fetch("/api/users?limit=50").then((r) => r.json()).then(setUsers);
  }, []);

  const fetchRecs = useCallback(
    async (currentCart: CartItem[], currentMode: "known" | "anonymous", userId: string | null) => {
      const slugs = currentCart.map((c) => c.slug);
      // Require at least one cart item before showing recommendations
      if (slugs.length === 0) {
        setRecommendations([]);
        return;
      }
      if (currentMode === "known" && !userId) {
        setRecommendations([]);
        return;
      }

      setLoading(true);
      try {
        const res = await fetch("/api/recommend", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ mode: currentMode, user_id: userId, cart: slugs }),
        });
        const data = await res.json();
        setRecommendations(data.recommendations);
        setSource(data.source);
      } finally {
        setLoading(false);
      }
    },
    [],
  );

  const addToCart = useCallback(
    (slug: string) => {
      if (cartSlugs.has(slug)) return;
      const p = productMap[slug];
      if (!p) return;
      const newCart = [...cart, { slug, name: p.product_name, price: p.base_price, quantity: 1 }];
      setCart(newCart);
      fetchRecs(newCart, mode, selectedUser);
    },
    [cart, cartSlugs, productMap, mode, selectedUser, fetchRecs],
  );

  const removeFromCart = useCallback(
    (slug: string) => {
      const newCart = cart.filter((c) => c.slug !== slug);
      setCart(newCart);
      fetchRecs(newCart, mode, selectedUser);
    },
    [cart, mode, selectedUser, fetchRecs],
  );

  const clearCart = useCallback(() => {
    setCart([]);
    setRecommendations([]);
  }, []);

  const handleModeChange = useCallback(
    (newMode: "known" | "anonymous") => {
      setMode(newMode);
      setRecommendations([]);
      if (newMode === "anonymous") setUserProfile(null);
      if (cart.length > 0) {
        if (newMode === "known" && selectedUser) {
          fetchRecs(cart, newMode, selectedUser);
        } else if (newMode === "anonymous") {
          fetchRecs(cart, newMode, null);
        }
      }
    },
    [cart, selectedUser, fetchRecs],
  );

  const handleUserSelect = useCallback(
    (userId: string) => {
      setSelectedUser(userId);
      fetch(`/api/users/${userId}`).then((r) => r.json()).then(setUserProfile);
      if (cart.length > 0) {
        fetchRecs(cart, "known", userId);
      }
    },
    [cart, fetchRecs],
  );

  return (
    <div className="min-h-screen bg-background">
      <header className="border-b px-6 py-4">
        <div className="max-w-7xl mx-auto flex items-center justify-between">
          <h1 className="text-xl font-bold">Recommender Accelerator</h1>
          <div className="flex items-center gap-4">
            <ModeToggle mode={mode} onModeChange={handleModeChange} />
            {mode === "known" && (
              <UserSearch users={users} selectedUser={selectedUser} onUserSelect={handleUserSelect} />
            )}
          </div>
        </div>
      </header>

      <main className="max-w-7xl mx-auto px-6 py-6">
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          <div className="lg:col-span-2">
            <ProductGrid products={products} onAddToCart={addToCart} cartSlugs={cartSlugs} />
          </div>
          <div className="space-y-4">
            {mode === "known" && userProfile && (
              <div className="border rounded-lg p-4 bg-muted/30">
                <div className="text-sm font-medium">{userProfile.user_id}</div>
                <div className="text-xs text-muted-foreground mt-1 space-y-0.5">
                  {userProfile.primary_store && <div>Store: {userProfile.primary_store}</div>}
                  {userProfile.total_orders && <div>Total orders: {userProfile.total_orders}</div>}
                  {userProfile.store_visits && <div>Visits to this store: {userProfile.store_visits}</div>}
                </div>
              </div>
            )}
            <CartPanel items={cart} onRemove={removeFromCart} onClear={clearCart} />
            <Recommendations
              recommendations={recommendations}
              source={source}
              loading={loading}
              productNames={productNames}
              productPrices={productPrices}
              onAddToCart={addToCart}
            />
          </div>
        </div>
      </main>
    </div>
  );
}
