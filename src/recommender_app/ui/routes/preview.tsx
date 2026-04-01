import { createFileRoute } from "@tanstack/react-router";

export const Route = createFileRoute("/preview")({ component: PreviewPage });

const SAMPLE_RECS = [
  { product: "Classic Fries", score: 92 },
  { product: "Iced Lemon Tea", score: 87 },
  { product: "Honey Glazed Rings", score: 74 },
];

function PreviewPage() {
  return (
    <div className="min-h-screen bg-[#fcf9f8] p-10 font-[Plus_Jakarta_Sans]">
      <h1 className="font-headline font-black text-3xl mb-2 tracking-tight">Recommendation Card Options</h1>
      <p className="text-stone-400 mb-10 text-sm">Pick the one that works best for the demo</p>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-12">

        {/* OPTION A: Large percentage + horizontal bar */}
        <div>
          <h2 className="font-headline font-black text-lg uppercase tracking-widest text-brand mb-4">Option A: Large % + Bar</h2>
          <div className="bg-[#f5f2f0] rounded-2xl p-6 space-y-4">
            {SAMPLE_RECS.map((rec) => (
              <div key={rec.product} className="bg-white rounded-[2rem] p-5 shadow-sm">
                <div className="flex items-center gap-5">
                  <div className="w-16 h-16 bg-stone-50 rounded-full flex items-center justify-center shrink-0">
                    <span className="material-symbols-outlined text-3xl text-stone-300">restaurant</span>
                  </div>
                  <div className="flex-grow">
                    <div className="flex justify-between items-center mb-1">
                      <h4 className="font-headline font-black text-base tracking-tight">{rec.product}</h4>
                      <span className="font-headline font-black text-2xl text-brand">{rec.score}%</span>
                    </div>
                    <div className="w-full bg-stone-100 rounded-full h-2.5 mt-2">
                      <div className="h-2.5 rounded-full bg-brand transition-all" style={{ width: `${rec.score}%` }} />
                    </div>
                    <div className="flex justify-between items-center mt-3">
                      <span className="font-headline font-black text-sm text-stone-800">$8.50</span>
                      <button className="bg-brand-light text-brand px-4 py-1.5 rounded-full text-xs font-black">+ Add</button>
                    </div>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>

        {/* OPTION B: Circular gauge */}
        <div>
          <h2 className="font-headline font-black text-lg uppercase tracking-widest text-brand mb-4">Option B: Circular Gauge</h2>
          <div className="bg-[#f5f2f0] rounded-2xl p-6 space-y-4">
            {SAMPLE_RECS.map((rec) => {
              const circumference = 2 * Math.PI * 28;
              const offset = circumference - (rec.score / 100) * circumference;
              return (
                <div key={rec.product} className="bg-white rounded-[2rem] p-5 flex items-center gap-5 shadow-sm">
                  <div className="relative w-20 h-20 shrink-0">
                    <svg className="w-20 h-20 -rotate-90" viewBox="0 0 64 64">
                      <circle cx="32" cy="32" r="28" fill="none" stroke="#f0eded" strokeWidth="5" />
                      <circle
                        cx="32" cy="32" r="28" fill="none"
                        stroke="var(--brand-primary)"
                        strokeWidth="5"
                        strokeLinecap="round"
                        strokeDasharray={circumference}
                        strokeDashoffset={offset}
                      />
                    </svg>
                    <div className="absolute inset-0 flex items-center justify-center">
                      <span className="font-headline font-black text-sm">{rec.score}%</span>
                    </div>
                  </div>
                  <div className="flex-grow">
                    <h4 className="font-headline font-black text-base tracking-tight mb-1">{rec.product}</h4>
                    <span className="text-[10px] font-bold uppercase tracking-tight text-stone-400">Match Score</span>
                    <div className="flex justify-between items-center mt-3">
                      <span className="font-headline font-black text-sm text-stone-800">$8.50</span>
                      <button className="bg-brand-light text-brand px-4 py-1.5 rounded-full text-xs font-black">+ Add</button>
                    </div>
                  </div>
                </div>
              );
            })}
          </div>
        </div>

        {/* OPTION C: Vertical score column */}
        <div>
          <h2 className="font-headline font-black text-lg uppercase tracking-widest text-brand mb-4">Option C: Vertical Bar</h2>
          <div className="bg-[#f5f2f0] rounded-2xl p-6 space-y-4">
            {SAMPLE_RECS.map((rec) => (
              <div key={rec.product} className="bg-white rounded-[2rem] p-5 flex items-stretch gap-4 shadow-sm overflow-hidden">
                {/* Vertical bar */}
                <div className="w-2 rounded-full bg-stone-100 relative shrink-0 min-h-[72px]">
                  <div
                    className="absolute bottom-0 w-full rounded-full bg-brand transition-all"
                    style={{ height: `${rec.score}%` }}
                  />
                </div>
                <div className="flex items-center gap-4 flex-grow">
                  <div className="w-14 h-14 bg-stone-50 rounded-full flex items-center justify-center shrink-0">
                    <span className="material-symbols-outlined text-2xl text-stone-300">restaurant</span>
                  </div>
                  <div className="flex-grow">
                    <div className="flex justify-between items-start mb-1">
                      <h4 className="font-headline font-black text-base tracking-tight">{rec.product}</h4>
                      <span className="font-headline font-black text-xl text-brand">{rec.score}%</span>
                    </div>
                    <div className="flex justify-between items-center mt-2">
                      <span className="font-headline font-black text-sm text-stone-800">$8.50</span>
                      <button className="bg-brand-light text-brand px-4 py-1.5 rounded-full text-xs font-black">+ Add</button>
                    </div>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>

        {/* OPTION D: Enhanced badge + inline bar */}
        <div>
          <h2 className="font-headline font-black text-lg uppercase tracking-widest text-brand mb-4">Option D: Badge + Inline Bar</h2>
          <div className="bg-[#f5f2f0] rounded-2xl p-6 space-y-4">
            {SAMPLE_RECS.map((rec) => (
              <div key={rec.product} className="bg-white rounded-[2rem] p-5 flex items-center gap-5 shadow-sm">
                <div className="w-16 h-16 bg-stone-50 rounded-full flex items-center justify-center shrink-0">
                  <span className="material-symbols-outlined text-3xl text-stone-300">restaurant</span>
                </div>
                <div className="flex-grow">
                  <div className="flex justify-between items-start mb-2">
                    <h4 className="font-headline font-black text-base tracking-tight">{rec.product}</h4>
                    <span className="bg-[#10b981] text-white text-xs font-black px-3 py-1.5 rounded-full uppercase tracking-tight">
                      {rec.score}% Match
                    </span>
                  </div>
                  <div className="flex items-center gap-3 mb-3">
                    <div className="flex-grow bg-stone-100 rounded-full h-2">
                      <div className="h-2 rounded-full bg-brand transition-all" style={{ width: `${rec.score}%` }} />
                    </div>
                    <span className="text-xs font-headline font-black text-stone-500 w-8">{rec.score}%</span>
                  </div>
                  <div className="flex justify-between items-center">
                    <span className="font-headline font-black text-sm text-stone-800">$8.50</span>
                    <button className="bg-brand-light text-brand px-4 py-1.5 rounded-full text-xs font-black">+ Add</button>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>

      </div>
    </div>
  );
}
