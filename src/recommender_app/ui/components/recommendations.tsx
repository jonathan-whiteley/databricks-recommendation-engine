import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Progress } from "@/components/ui/progress";
import { Loader2 } from "lucide-react";

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
}

export function Recommendations({
  recommendations,
  source,
  loading,
  productNames,
  productPrices,
  onAddToCart,
}: RecommendationsProps) {
  if (loading) {
    return (
      <div className="border rounded-lg p-6 flex items-center justify-center">
        <Loader2 className="h-5 w-5 animate-spin mr-2" />
        <span className="text-muted-foreground">Getting recommendations...</span>
      </div>
    );
  }

  if (recommendations.length === 0) {
    return null;
  }

  const maxScore = Math.max(...recommendations.map((r) => r.score));

  return (
    <div className="border rounded-lg p-4 space-y-3">
      <div className="flex justify-between items-center">
        <h2 className="font-semibold text-lg">You might also like</h2>
        <span className="text-xs text-muted-foreground uppercase tracking-wide">
          {source === "als" ? "Personalized" : "Based on cart"}
        </span>
      </div>

      <div className="space-y-2">
        {recommendations.map((rec) => (
          <Card key={rec.product} className="overflow-hidden">
            <CardContent className="p-3 flex items-center gap-3">
              <div className="flex-1 min-w-0">
                <div className="flex items-center gap-2">
                  <span className="font-medium text-sm truncate">
                    {productNames[rec.product] ?? rec.product}
                  </span>
                  <span className="text-muted-foreground text-xs">
                    ${(productPrices[rec.product] ?? 0).toFixed(2)}
                  </span>
                </div>
                <div className="flex items-center gap-2 mt-1">
                  <Progress value={(rec.score / maxScore) * 100} className="h-1.5 flex-1" />
                  <span className="text-xs font-mono text-muted-foreground w-12 text-right">
                    {((rec.score / maxScore) * 100).toFixed(0)}%
                  </span>
                </div>
              </div>
              <Button size="sm" variant="outline" onClick={() => onAddToCart(rec.product)}>
                Add
              </Button>
            </CardContent>
          </Card>
        ))}
      </div>
    </div>
  );
}
