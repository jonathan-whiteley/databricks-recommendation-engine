import { useState, useCallback, useEffect } from "react";
import { createFileRoute } from "@tanstack/react-router";
import { ModeToggle } from "@/components/mode-toggle";
import { UserSearch } from "@/components/user-search";
import { ProductGrid } from "@/components/product-grid";
import { CartPanel } from "@/components/cart-panel";
import { Recommendations } from "@/components/recommendations";
import { SettingsPanel } from "@/components/settings-panel";

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
  const [brandName, setBrandName] = useState(() => {
    try { return JSON.parse(localStorage.getItem("lakehouse-market-brand") || "{}").storeName || "Lakehouse Market"; } catch { return "Lakehouse Market"; }
  });
  const [logoUrl, setLogoUrl] = useState<string | null>(() => {
    try { return JSON.parse(localStorage.getItem("lakehouse-market-brand") || "{}").logoUrl || null; } catch { return null; }
  });

  // Apply saved brand color on mount
  useEffect(() => {
    try {
      const saved = JSON.parse(localStorage.getItem("lakehouse-market-brand") || "{}");
      if (saved.primaryColor) {
        document.documentElement.style.setProperty("--brand-primary", saved.primaryColor);
        const r = parseInt(saved.primaryColor.slice(1, 3), 16);
        const g = parseInt(saved.primaryColor.slice(3, 5), 16);
        const b = parseInt(saved.primaryColor.slice(5, 7), 16);
        document.documentElement.style.setProperty("--brand-primary-light", `rgba(${r},${g},${b},0.12)`);
        document.documentElement.style.setProperty("--brand-primary-shadow", `rgba(${r},${g},${b},0.4)`);
      }
    } catch {}
  }, []);

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
      <header className="w-full top-0 sticky z-50 bg-[#f6f3f2] flex justify-between items-center px-8 py-5">
        <div className="flex items-center gap-8">
          <div className="flex items-center gap-3">
            {logoUrl ? (
              <img src={logoUrl} alt={brandName} className="h-8 w-auto max-w-[160px] object-contain" />
            ) : (
              <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 300 331" className="h-7 w-auto shrink-0">
                <path fill="var(--brand-primary, #FF3621)" d="M283.923 136.449L150.144 213.624L6.88995 131.168L0 134.982V194.844L150.144 281.115L283.923 204.234V235.926L150.144 313.1L6.88995 230.644L0 234.458V244.729L150.144 331L300 244.729V184.867L293.11 181.052L150.144 263.215L16.0766 186.334V154.643L150.144 231.524L300 145.253V86.2713L292.536 81.8697L150.144 163.739L22.9665 90.9663L150.144 17.8998L254.641 78.055L263.828 72.773V65.4371L150.144 0L0 86.2713V95.6613L150.144 181.933L283.923 104.758V136.449Z"/>
              </svg>
            )}
            <h1 className="text-2xl font-black text-brand font-headline tracking-tighter uppercase">
              {brandName}
            </h1>
          </div>
          <nav className="hidden md:flex items-center gap-8 font-headline tracking-tighter font-bold uppercase text-sm">
            <a className="text-brand px-2 py-1" href="#">
              Checkout
            </a>
          </nav>
        </div>
        <div className="flex items-center gap-4">
          <ModeToggle mode={mode} onModeChange={handleModeChange} />
          {mode === "known" && (
            <UserSearch users={users} selectedUser={selectedUser} onUserSelect={handleUserSelect} />
          )}
          <SettingsPanel
            onBrandChange={(s) => {
              setBrandName(s.storeName);
              setLogoUrl(s.logoUrl);
            }}
          />
        </div>
      </header>

      {/* Main Layout: Catalog (2/3) + Sidebar (1/3) */}
      <main className="flex flex-col md:flex-row min-h-screen relative">
        {/* Catalog Section */}
        <div className="w-full md:w-2/3 p-8 lg:p-10 mb-24 md:mb-0">
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
        <aside className="w-full md:w-1/3 bg-[#fdfcfc] border-l border-[#eae7e7] flex flex-col md:sticky md:top-0 md:h-screen md:overflow-y-auto">
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
