import { useState } from "react";

interface Product {
  product_id: string;
  product_name: string;
  product_slug: string;
  category: string;
  base_price: number;
}

interface ProductGridProps {
  products: Product[];
  onAddToCart: (slug: string) => void;
  cartSlugs: Set<string>;
}

const CATEGORY_ICONS: Record<string, string> = {
  Entrees: "lunch_dining",
  Sides: "nutrition",
  Drinks: "local_bar",
  Desserts: "cake",
  Electronics: "devices",
  Accessories: "watch",
  Clothing: "checkroom",
  Home: "home",
  Produce: "eco",
  Dairy: "egg",
  Bakery: "bakery_dining",
  Pantry: "kitchen",
};

export function ProductGrid({ products, onAddToCart, cartSlugs }: ProductGridProps) {
  const [search, setSearch] = useState("");
  const [activeCategory, setActiveCategory] = useState<string | null>(null);

  // Sort categories: put Entrees-type categories first, then alphabetical
  const CATEGORY_ORDER: Record<string, number> = {
    Entrees: 0, Sides: 1, Drinks: 2, Desserts: 3,
    Electronics: 0, Accessories: 1, Clothing: 2, Home: 3,
    Produce: 0, Dairy: 1, Bakery: 2, Pantry: 3,
  };
  const categories = [...new Set(products.map((p) => p.category))].sort(
    (a, b) => (CATEGORY_ORDER[a] ?? 99) - (CATEGORY_ORDER[b] ?? 99)
  );
  const filtered = products
    .filter((p) => {
      const matchesSearch = p.product_name.toLowerCase().includes(search.toLowerCase());
      const matchesCategory = !activeCategory || p.category === activeCategory;
      return matchesSearch && matchesCategory;
    })
    .sort((a, b) => (CATEGORY_ORDER[a.category] ?? 99) - (CATEGORY_ORDER[b.category] ?? 99));

  return (
    <div>
      <header className="mb-10">
        <h2 className="font-headline text-4xl font-black tracking-tighter leading-none mb-4">
          Browse Our Selection
        </h2>

        {/* Search as subtle inline input */}
        <div className="mb-6">
          <input
            type="text"
            placeholder="Search menu..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            className="bg-[#f6f3f2] px-6 py-3 rounded-full text-sm font-[Plus_Jakarta_Sans] w-64 focus:outline-none focus:bg-[#eae7e7] transition-colors placeholder:text-stone-400"
          />
        </div>

        {/* Category Pills */}
        <div className="flex gap-3 overflow-x-auto no-scrollbar pb-2">
          <span
            className={`px-6 py-2 rounded-full text-sm font-bold whitespace-nowrap cursor-pointer transition-colors ${
              activeCategory === null
                ? "bg-brand text-white"
                : "bg-[#c8c6c5] text-[#1c1b1b] hover:bg-[#e5e2e1]"
            }`}
            onClick={() => setActiveCategory(null)}
          >
            All Items
          </span>
          {categories.map((cat) => (
            <span
              key={cat}
              className={`px-6 py-2 rounded-full text-sm font-medium whitespace-nowrap cursor-pointer transition-colors ${
                activeCategory === cat
                  ? "bg-brand text-white"
                  : "bg-[#c8c6c5] text-[#1c1b1b] hover:bg-[#e5e2e1]"
              }`}
              onClick={() => setActiveCategory(cat)}
            >
              {cat}
            </span>
          ))}
        </div>
      </header>

      {/* Product Grid */}
      <div className="grid grid-cols-2 sm:grid-cols-3 xl:grid-cols-3 gap-4">
        {filtered.map((p) => {
          const inCart = cartSlugs.has(p.product_slug);
          const icon = CATEGORY_ICONS[p.category] ?? "restaurant";

          return (
            <div
              key={p.product_slug}
              className="bg-white rounded-2xl p-4 shadow-sm hover:shadow-md transition-all cursor-pointer group"
            >
              {/* Image Placeholder */}
              <div className="aspect-[4/3] bg-[#f0f0f0] rounded-xl flex flex-col items-center justify-center mb-3 relative">
                <span className="material-symbols-outlined text-[#bdbdbd] text-5xl mb-1">
                  {icon}
                </span>
                <span className="font-headline font-black text-sm tracking-tight text-[#bdbdbd] uppercase">
                  {p.category}
                </span>
              </div>

              {/* Product Info */}
              <div className="flex justify-between items-start mb-1">
                <h3 className="font-headline font-black text-sm tracking-tight leading-tight">
                  {p.product_name}
                </h3>
                <span className="font-headline font-black text-sm text-brand shrink-0 ml-2">
                  ${p.base_price.toFixed(2)}
                </span>
              </div>

              {/* Add to Cart Button */}
              <button
                className={`w-full font-headline font-black py-2.5 rounded-full text-xs uppercase tracking-widest transition-all mt-3 ${
                  inCart
                    ? "bg-brand text-white"
                    : "bg-[#f6f3f2] text-brand hover:bg-brand hover:text-white"
                }`}
                onClick={() => onAddToCart(p.product_slug)}
              >
                {inCart ? "In Cart" : "Add to Cart"}
              </button>
            </div>
          );
        })}
      </div>
    </div>
  );
}
