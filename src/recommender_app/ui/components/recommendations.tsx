interface Recommendation {
  product: string;
  score: number;
  rank: number;
}

interface RecommendationsProps {
  recommendations: Recommendation[];
  source: string;
  loading: boolean;
  productNames: Record<string, string>;
  productPrices: Record<string, number>;
  onAddToCart: (slug: string) => void;
  userName?: string;
}

export function Recommendations({
  recommendations,
  source,
  loading,
  productNames,
  productPrices,
  onAddToCart,
  userName,
}: RecommendationsProps) {
  if (loading) {
    return (
      <div className="p-10 bg-[#f5f2f0] flex-grow flex items-center justify-center gap-3">
        <span className="material-symbols-outlined text-brand animate-spin">progress_activity</span>
        <span className="text-stone-400 text-sm font-medium">Getting recommendations...</span>
      </div>
    );
  }

  if (recommendations.length === 0) {
    return (
      <div className="p-10 bg-[#f5f2f0] flex-grow">
        <div className="flex items-center gap-2 mb-4">
          <span className="material-symbols-outlined text-sm text-brand">auto_awesome</span>
          <h3 className="font-headline font-black text-sm uppercase tracking-widest text-brand">
            Curated for You
          </h3>
        </div>
        <p className="text-stone-400 text-sm">Add items to your cart to see recommendations</p>
      </div>
    );
  }

  const maxScore = Math.max(...recommendations.map((r) => r.score));
  const displayName = userName || "You";

  return (
    <div className="p-10 bg-[#f5f2f0] flex-grow">
      {/* Header */}
      <div className="flex justify-between items-center mb-6">
        <h3 className="font-headline font-black text-sm uppercase tracking-widest flex items-center gap-2 text-brand">
          <span className="material-symbols-outlined text-sm">auto_awesome</span>
          Curated for {displayName}
        </h3>
        <span className="text-[10px] font-bold bg-white text-stone-400 px-3 py-1 rounded-full uppercase tracking-tighter shadow-sm">
          {source === "als" ? "Personalized" : "Based on cart"}
        </span>
      </div>

      {/* Recommendation Cards */}
      <div className="flex flex-col gap-4">
        {recommendations.map((rec) => {
          const matchPercent = ((rec.score / maxScore) * 100).toFixed(0);
          return (
            <div
              key={rec.product}
              className="bg-white rounded-[2rem] p-5 flex items-center gap-5 shadow-sm"
            >
              {/* Icon */}
              <div className="w-20 h-20 bg-stone-50 rounded-full flex items-center justify-center overflow-hidden shrink-0">
                <span className="material-symbols-outlined text-4xl text-stone-300">
                  restaurant
                </span>
              </div>

              {/* Content */}
              <div className="flex-grow min-w-0">
                <div className="flex justify-between items-start mb-1">
                  <h4 className="font-headline font-black text-base tracking-tight truncate">
                    {productNames[rec.product] ?? rec.product}
                  </h4>
                  <span className="bg-[#10b981] text-white text-[9px] font-black px-2 py-1 rounded-full uppercase tracking-tight shrink-0 ml-2">
                    {matchPercent}% Match
                  </span>
                </div>
                <div className="flex justify-between items-center mt-3">
                  <span className="font-headline font-black text-sm text-stone-800">
                    ${(productPrices[rec.product] ?? 0).toFixed(2)}
                  </span>
                  <button
                    className="bg-brand-light text-brand px-4 py-1.5 rounded-full text-xs font-black tracking-tight hover:opacity-80 transition-all"
                    onClick={() => onAddToCart(rec.product)}
                  >
                    + Add
                  </button>
                </div>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
