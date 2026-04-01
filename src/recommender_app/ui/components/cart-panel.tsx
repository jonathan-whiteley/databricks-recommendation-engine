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
    <div className="p-10 pb-4 bg-white">
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

      {items.length === 0 ? (
        <p className="text-stone-400 text-sm pb-8">Add items to get started</p>
      ) : (
        <>
          {/* Cart Items */}
          <div className="flex flex-col gap-6 mb-10">
            {items.map((item) => (
              <div key={item.slug} className="flex items-center gap-5">
                {/* Icon thumbnail */}
                <div className="w-16 h-16 rounded-full bg-stone-100 flex items-center justify-center overflow-hidden shrink-0">
                  <span className="material-symbols-outlined text-3xl text-stone-800">
                    {getIconForItem(item.slug)}
                  </span>
                </div>
                {/* Name and price */}
                <div className="flex-grow">
                  <h4 className="font-headline font-extrabold text-[15px] leading-tight mb-0.5">
                    {item.name}
                  </h4>
                  <p className="text-xs text-stone-400 font-medium">
                    ${item.price.toFixed(2)}
                  </p>
                </div>
                {/* Quantity / Remove */}
                <div className="flex items-center bg-[#f2f2f2] rounded-full px-2 py-1 gap-3">
                  <button
                    className="w-5 h-5 flex items-center justify-center text-stone-400 font-bold text-xs hover:text-[#ad2c00] transition-colors"
                    onClick={() => onRemove(item.slug)}
                  >
                    <span className="material-symbols-outlined text-sm">close</span>
                  </button>
                  <span className="text-xs font-extrabold font-headline">{item.quantity}</span>
                </div>
              </div>
            ))}
          </div>

          {/* Subtotal */}
          <div className="flex justify-between items-center mt-12 mb-8">
            <span className="text-stone-400 font-medium">Subtotal</span>
            <span className="text-[28px] font-headline font-black">${total.toFixed(2)}</span>
          </div>

          {/* Complete Order Button */}
          <button className="w-full bg-[#ad2c00] text-white font-headline font-black py-6 rounded-full text-lg uppercase tracking-wider btn-shadow hover:brightness-110 active:scale-[0.98] transition-all mb-4">
            Complete Order
          </button>
        </>
      )}
    </div>
  );
}
