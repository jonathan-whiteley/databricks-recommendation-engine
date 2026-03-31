import { Button } from "@/components/ui/button";
import { X } from "lucide-react";

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

export function CartPanel({ items, onRemove, onClear }: CartPanelProps) {
  const total = items.reduce((sum, item) => sum + item.price * item.quantity, 0);

  return (
    <div className="border rounded-lg p-4 space-y-3">
      <div className="flex justify-between items-center">
        <h2 className="font-semibold text-lg">Cart ({items.length})</h2>
        {items.length > 0 && (
          <Button variant="ghost" size="sm" onClick={onClear}>
            Clear
          </Button>
        )}
      </div>

      {items.length === 0 ? (
        <p className="text-muted-foreground text-sm">Add items to get recommendations</p>
      ) : (
        <div className="space-y-2">
          {items.map((item) => (
            <div key={item.slug} className="flex justify-between items-center text-sm">
              <div>
                <span className="font-medium">{item.name}</span>
                <span className="text-muted-foreground ml-2">${item.price.toFixed(2)}</span>
              </div>
              <Button variant="ghost" size="icon" className="h-6 w-6" onClick={() => onRemove(item.slug)}>
                <X className="h-3 w-3" />
              </Button>
            </div>
          ))}
          <div className="border-t pt-2 flex justify-between font-semibold">
            <span>Total</span>
            <span>${total.toFixed(2)}</span>
          </div>
        </div>
      )}
    </div>
  );
}
