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

  const categories = [...new Set(products.map((p) => p.category))];
  const filtered = products.filter((p) => {
    const matchesSearch = p.product_name.toLowerCase().includes(search.toLowerCase());
    const matchesCategory = !activeCategory || p.category === activeCategory;
    return matchesSearch && matchesCategory;
  });

  return (
    <div>
      {/* Editorial Header */}
      <header className="mb-12">
        <p className="font-headline font-extrabold text-[#ad2c00] text-sm uppercase tracking-widest mb-2">
          Editor's Pick
        </p>
        <h2 className="font-headline text-5xl font-black tracking-tighter leading-none mb-4">
          Sizzle &amp; Slate<br />Essentials
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
                ? "bg-[#ad2c00] text-white"
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
                  ? "bg-[#ad2c00] text-white"
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
      <div className="grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-2 gap-8">
        {filtered.map((p) => {
          const inCart = cartSlugs.has(p.product_slug);
          const icon = CATEGORY_ICONS[p.category] ?? "restaurant";

          return (
            <div
              key={p.product_slug}
              className="bg-white rounded-3xl p-6 shadow-sm hover:shadow-md transition-all cursor-pointer group"
            >
              {/* Image Placeholder */}
              <div className="aspect-square bg-[#f0f0f0] rounded-2xl flex flex-col items-center justify-center mb-6 relative">
                <span className="material-symbols-outlined text-[#bdbdbd] text-8xl mb-2">
                  {icon}
                </span>
                <span className="font-headline font-black text-4xl tracking-tight text-[#bdbdbd] uppercase">
                  {p.category}
                </span>
              </div>

              {/* Product Info */}
              <div className="flex justify-between items-start mb-2">
                <h3 className="font-headline font-black text-2xl tracking-tight">
                  {p.product_name}
                </h3>
                <span className="font-headline font-black text-xl text-[#ad2c00]">
                  ${p.base_price.toFixed(2)}
                </span>
              </div>

              <p className="text-stone-500 text-sm leading-relaxed max-w-[80%] mb-6">
                {p.category}
              </p>

              {/* Add to Cart Button */}
              <button
                className={`w-full font-headline font-black py-4 rounded-full text-sm uppercase tracking-widest transition-all ${
                  inCart
                    ? "bg-[#ad2c00] text-white"
                    : "bg-[#f6f3f2] text-[#ad2c00] hover:bg-[#ad2c00] hover:text-white"
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
