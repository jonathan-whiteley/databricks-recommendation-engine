interface CartItem {
  slug: string;
  name: string;
  price: number;
  quantity: number;
}

interface CartPanelProps {
  items: CartItem[];
  onRemove: (slug: string) => void;
  onClear: () => void;
}

const SLUG_ICONS: Record<string, string> = {
  entree: "lunch_dining",
  burger: "lunch_dining",
  side: "nutrition",
  fries: "nutrition",
  drink: "local_bar",
  shake: "local_bar",
  dessert: "cake",
};

function getIconForItem(slug: string): string {
  const lower = slug.toLowerCase();
  for (const [key, icon] of Object.entries(SLUG_ICONS)) {
    if (lower.includes(key)) return icon;
  }
  return "restaurant";
}

export function CartPanel({ items, onRemove, onClear }: CartPanelProps) {
  const total = items.reduce((sum, item) => sum + item.price * item.quantity, 0);

  return (
    <div className="p-10 pb-4 bg-white flex flex-col">
      {/* Header */}
      <div className="flex justify-between items-center mb-8">
        <h2 className="text-[32px] font-headline font-black tracking-tight">Your Order</h2>
        {items.length > 0 && (
          <button
            className="text-xs font-bold uppercase tracking-widest text-[#ad2c00] hover:opacity-70 transition-opacity"
            onClick={onClear}
          >
            Clear Cart
          </button>
        )}
      </div>

      {/* Cart Items - fixed min-height for 4 items so subtotal/button stays put */}
      <div className="min-h-[280px]">
        {items.length === 0 ? (
          <p className="text-stone-400 text-sm">Add items to get started</p>
        ) : (
          <div className="flex flex-col gap-5">
            {items.map((item) => (
              <div key={item.slug} className="flex items-center gap-4">
                {/* Icon thumbnail */}
                <div className="w-12 h-12 rounded-full bg-stone-100 flex items-center justify-center overflow-hidden shrink-0">
                  <span className="material-symbols-outlined text-2xl text-stone-800">
                    {getIconForItem(item.slug)}
                  </span>
                </div>
                {/* Name and price */}
                <div className="flex-grow min-w-0">
                  <h4 className="font-headline font-extrabold text-[14px] leading-tight mb-0.5 truncate">
                    {item.name}
                  </h4>
                  <p className="text-xs text-stone-400 font-medium">
                    ${item.price.toFixed(2)}
                  </p>
                </div>
                {/* Remove */}
                <button
                  className="w-7 h-7 flex items-center justify-center text-stone-400 hover:text-[#ad2c00] transition-colors shrink-0"
                  onClick={() => onRemove(item.slug)}
                >
                  <span className="material-symbols-outlined text-base">close</span>
                </button>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Subtotal + Button - always at the bottom */}
      <div className="mt-auto pt-6 border-t border-[#f0eded]">
        <div className="flex justify-between items-center mb-6">
          <span className="text-stone-400 font-medium">Subtotal</span>
          <span className="text-[28px] font-headline font-black">
            ${items.length > 0 ? total.toFixed(2) : "0.00"}
          </span>
        </div>
        <button
          className={`w-full font-headline font-black py-5 rounded-full text-lg uppercase tracking-wider transition-all mb-4 ${
            items.length > 0
              ? "bg-[#ad2c00] text-white btn-shadow hover:brightness-110 active:scale-[0.98]"
              : "bg-[#eae7e7] text-stone-400 cursor-not-allowed"
          }`}
          disabled={items.length === 0}
        >
          Complete Order
        </button>
      </div>
    </div>
  );
}
