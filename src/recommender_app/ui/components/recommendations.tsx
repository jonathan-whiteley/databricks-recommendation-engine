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

  // Spread scores across a visible range (top item = 95%, last item = ~60%)
  // This prevents all items showing ~100% when raw scores are tightly clustered
  const scores = recommendations.map((r) => r.score);
  const maxScore = Math.max(...scores);
  const minScore = Math.min(...scores);
  const scoreRange = maxScore - minScore;

  function toDisplayPercent(score: number): number {
    if (scoreRange < 0.001) return 95; // all scores identical
    const normalized = (score - minScore) / scoreRange; // 0 to 1
    return Math.round(60 + normalized * 35); // map to 60-95%
  }

  const displayName = userName || "You";

  return (
    <div className="p-10 bg-[#f5f2f0] flex-grow">
      {/* Header */}
      <div className="flex justify-between items-center mb-6">
        <h3 className="font-headline font-black text-sm uppercase tracking-widest flex items-center gap-2 text-brand">
          <span className="material-symbols-outlined text-sm">auto_awesome</span>
          Curated for {displayName}
        </h3>
        <span className="relative text-xs font-black px-4 py-1.5 rounded-full uppercase tracking-wide shadow-md overflow-hidden bg-white text-brand border border-brand-light">
          <span className="absolute inset-0 bg-gradient-to-r from-transparent via-white/60 to-transparent animate-[shimmer_2s_infinite] pointer-events-none" />
          <span className="relative flex items-center gap-1.5">
            <span className="material-symbols-outlined text-xs">{source === "als" ? "psychology" : "shopping_cart"}</span>
            {source === "als" ? "Personalized" : "Based on cart"}
          </span>
        </span>
      </div>

      {/* Recommendation Cards */}
      <div className="flex flex-col gap-4">
        {recommendations.map((rec) => {
          const matchPercent = toDisplayPercent(rec.score);
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
                <h4 className="font-headline font-black text-base tracking-tight truncate mb-2">
                  {productNames[rec.product] ?? rec.product}
                </h4>
                <div className="flex items-center gap-2 mb-3">
                  <div className="flex-grow bg-stone-100 rounded-full h-2">
                    <div
                      className="h-2 rounded-full bg-brand transition-all"
                      style={{ width: `${matchPercent}%` }}
                    />
                  </div>
                  <span className="bg-[#10b981] text-white text-xs font-black px-3 py-1.5 rounded-full uppercase tracking-tight shrink-0">
                    {matchPercent}% Match
                  </span>
                </div>
                <div className="flex justify-between items-center">
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
