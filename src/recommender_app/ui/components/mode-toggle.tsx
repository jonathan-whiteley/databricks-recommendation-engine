interface ModeToggleProps {
  mode: "known" | "anonymous";
  onModeChange: (mode: "known" | "anonymous") => void;
}

export function ModeToggle({ mode, onModeChange }: ModeToggleProps) {
  return (
    <div className="flex bg-[#f0eded] rounded-full p-1">
      <button
        className={`px-4 py-1.5 rounded-full text-xs font-bold transition-all ${
          mode === "known"
            ? "bg-brand text-white"
            : "text-[#5d4038]"
        }`}
        onClick={() => onModeChange("known")}
      >
        Known
      </button>
      <button
        className={`px-4 py-1.5 rounded-full text-xs font-bold transition-all ${
          mode === "anonymous"
            ? "bg-brand text-white"
            : "text-[#5d4038]"
        }`}
        onClick={() => onModeChange("anonymous")}
      >
        Guest
      </button>
    </div>
  );
}
