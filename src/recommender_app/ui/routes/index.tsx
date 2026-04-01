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
    <div className="min-h-screen bg-[#fcf9f8] text-[#1c1b1b]">
      {/* Top NavBar */}
      <header className="w-full top-0 sticky z-50 bg-[#f6f3f2] flex justify-between items-center px-8 py-6">
        <div className="flex items-center gap-12">
          <h1 className="text-2xl font-black text-[#ad2c00] font-headline tracking-tighter uppercase">
            Lakehouse Market
          </h1>
          <nav className="hidden md:flex items-center gap-8 font-headline tracking-tighter font-bold uppercase text-sm">
            <a className="text-[#1c1b1b] hover:bg-stone-200 transition-colors px-2 py-1" href="#">
              Menu
            </a>
            <a className="text-[#ad2c00] px-2 py-1" href="#">
              Checkout
            </a>
          </nav>
        </div>
        <div className="flex items-center gap-4">
          <ModeToggle mode={mode} onModeChange={handleModeChange} />
          {mode === "known" && (
            <UserSearch users={users} selectedUser={selectedUser} onUserSelect={handleUserSelect} />
          )}
        </div>
      </header>

      {/* Main Layout: Catalog + Sidebar */}
      <main className="flex flex-col md:flex-row min-h-screen relative">
        {/* Catalog Section */}
        <div className="flex-1 p-8 lg:p-12 mb-24 md:mb-0">
          {/* User Profile Info (if known mode) */}
          {mode === "known" && userProfile && (
            <div className="mb-8 bg-white rounded-3xl p-6 shadow-sm">
              <div className="flex items-center gap-4">
                <div className="w-12 h-12 rounded-full bg-[#f6f3f2] flex items-center justify-center">
                  <span className="material-symbols-outlined text-[#ad2c00]">person</span>
                </div>
                <div>
                  <div className="font-headline font-black text-lg">{userProfile.user_id}</div>
                  <div className="text-xs text-stone-400 font-medium flex gap-4 mt-0.5">
                    {userProfile.primary_store && <span>Store: {userProfile.primary_store}</span>}
                    {userProfile.total_orders && <span>Orders: {userProfile.total_orders}</span>}
                    {userProfile.store_visits && <span>Visits: {userProfile.store_visits}</span>}
                  </div>
                </div>
              </div>
            </div>
          )}

          <ProductGrid products={products} onAddToCart={addToCart} cartSlugs={cartSlugs} />
        </div>

        {/* Sidebar */}
        <aside className="w-full md:w-[460px] bg-[#fdfcfc] flex flex-col md:sticky md:top-0 md:h-screen md:overflow-y-auto">
          {/* Cart Section (white bg) */}
          <CartPanel items={cart} onRemove={removeFromCart} onClear={clearCart} />

          {/* Recommendations Section (#f5f2f0 bg) */}
          <Recommendations
            recommendations={recommendations}
            source={source}
            loading={loading}
            productNames={productNames}
            productPrices={productPrices}
            onAddToCart={addToCart}
            userName={userProfile?.user_id}
          />
        </aside>
      </main>
    </div>
  );
}
