import { Button } from "@/components/ui/button";
import { Card, CardContent, CardFooter } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
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

const CATEGORY_COLORS: Record<string, string> = {
  Entrees: "bg-red-100 text-red-800",
  Sides: "bg-amber-100 text-amber-800",
  Drinks: "bg-blue-100 text-blue-800",
  Desserts: "bg-purple-100 text-purple-800",
  Electronics: "bg-blue-100 text-blue-800",
  Accessories: "bg-slate-100 text-slate-800",
  Clothing: "bg-emerald-100 text-emerald-800",
  Home: "bg-amber-100 text-amber-800",
  Produce: "bg-green-100 text-green-800",
  Dairy: "bg-sky-100 text-sky-800",
  Bakery: "bg-amber-100 text-amber-800",
  Pantry: "bg-orange-100 text-orange-800",
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
    <div className="space-y-4">
      <div className="flex gap-2 items-center flex-wrap">
        <Input
          placeholder="Search products..."
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          className="w-64"
        />
        <div className="flex gap-1 flex-wrap">
          <Badge
            variant={activeCategory === null ? "default" : "outline"}
            className="cursor-pointer"
            onClick={() => setActiveCategory(null)}
          >
            All
          </Badge>
          {categories.map((cat) => (
            <Badge
              key={cat}
              variant={activeCategory === cat ? "default" : "outline"}
              className="cursor-pointer"
              onClick={() => setActiveCategory(cat)}
            >
              {cat}
            </Badge>
          ))}
        </div>
      </div>

      <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-3">
        {filtered.map((p) => (
          <Card key={p.product_slug} className="flex flex-col">
            <CardContent className="pt-4 flex-1">
              <Badge className={CATEGORY_COLORS[p.category] ?? "bg-gray-100 text-gray-800"} variant="secondary">
                {p.category}
              </Badge>
              <h3 className="font-medium mt-2 text-sm">{p.product_name}</h3>
              <p className="text-muted-foreground text-sm">${p.base_price.toFixed(2)}</p>
            </CardContent>
            <CardFooter className="pt-0">
              <Button
                size="sm"
                variant={cartSlugs.has(p.product_slug) ? "secondary" : "default"}
                onClick={() => onAddToCart(p.product_slug)}
                className="w-full"
              >
                {cartSlugs.has(p.product_slug) ? "In Cart" : "Add to Cart"}
              </Button>
            </CardFooter>
          </Card>
        ))}
      </div>
    </div>
  );
}
