import { useState, useRef, useEffect } from "react";

interface BrandSettings {
  primaryColor: string;
  logoUrl: string | null;
  storeName: string;
}

const PRESET_COLORS = [
  { name: "Flame", value: "#ad2c00" },
  { name: "Databricks", value: "#FF3621" },
  { name: "Ocean", value: "#0066CC" },
  { name: "Forest", value: "#15803d" },
  { name: "Purple", value: "#7c3aed" },
  { name: "Slate", value: "#475569" },
  { name: "Gold", value: "#b45309" },
  { name: "Rose", value: "#be123c" },
];

const STORAGE_KEY = "lakehouse-market-brand";

function loadSettings(): BrandSettings {
  try {
    const saved = localStorage.getItem(STORAGE_KEY);
    if (saved) return JSON.parse(saved);
  } catch {}
  return { primaryColor: "#ad2c00", logoUrl: null, storeName: "Lakehouse Market" };
}

function saveSettings(settings: BrandSettings) {
  localStorage.setItem(STORAGE_KEY, JSON.stringify(settings));
}

function applyColor(color: string) {
  document.documentElement.style.setProperty("--brand-primary", color);
  // Compute a lighter tint for hover states
  const r = parseInt(color.slice(1, 3), 16);
  const g = parseInt(color.slice(3, 5), 16);
  const b = parseInt(color.slice(5, 7), 16);
  document.documentElement.style.setProperty(
    "--brand-primary-light",
    `rgba(${r}, ${g}, ${b}, 0.12)`
  );
  document.documentElement.style.setProperty(
    "--brand-primary-shadow",
    `rgba(${r}, ${g}, ${b}, 0.4)`
  );
}

interface SettingsPanelProps {
  onBrandChange: (settings: BrandSettings) => void;
}

export function useBrandSettings() {
  const [settings, setSettings] = useState<BrandSettings>(loadSettings);

  useEffect(() => {
    applyColor(settings.primaryColor);
  }, [settings.primaryColor]);

  const updateSettings = (partial: Partial<BrandSettings>) => {
    const updated = { ...settings, ...partial };
    setSettings(updated);
    saveSettings(updated);
  };

  return { settings, updateSettings };
}

export function SettingsPanel({ onBrandChange }: SettingsPanelProps) {
  const [open, setOpen] = useState(false);
  const [settings, setLocalSettings] = useState<BrandSettings>(loadSettings);
  const [customColor, setCustomColor] = useState(settings.primaryColor);
  const fileRef = useRef<HTMLInputElement>(null);
  const panelRef = useRef<HTMLDivElement>(null);

  // Close on outside click
  useEffect(() => {
    function handleClick(e: MouseEvent) {
      if (panelRef.current && !panelRef.current.contains(e.target as Node)) {
        setOpen(false);
      }
    }
    if (open) document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, [open]);

  const apply = (partial: Partial<BrandSettings>) => {
    const updated = { ...settings, ...partial };
    setLocalSettings(updated);
    saveSettings(updated);
    if (partial.primaryColor) applyColor(partial.primaryColor);
    onBrandChange(updated);
  };

  const handleLogoUpload = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;
    const reader = new FileReader();
    reader.onload = () => {
      apply({ logoUrl: reader.result as string });
    };
    reader.readAsDataURL(file);
  };

  const removeLogo = () => {
    apply({ logoUrl: null });
    if (fileRef.current) fileRef.current.value = "";
  };

  return (
    <div className="relative" ref={panelRef}>
      <button
        className="p-2 hover:bg-stone-200 rounded-full transition-all active:scale-95"
        onClick={() => setOpen(!open)}
      >
        <span className="material-symbols-outlined text-[#1c1b1b]">settings</span>
      </button>

      {open && (
        <div className="absolute right-0 top-12 w-80 bg-white rounded-2xl shadow-lg border border-stone-200 p-6 z-50">
          <h3 className="font-headline font-black text-lg tracking-tight mb-5">Branding</h3>

          {/* Store Name */}
          <div className="mb-5">
            <label className="text-xs font-bold uppercase tracking-widest text-stone-400 mb-2 block">
              Store Name
            </label>
            <input
              type="text"
              value={settings.storeName}
              onChange={(e) => apply({ storeName: e.target.value })}
              className="w-full bg-[#f6f3f2] px-4 py-2.5 rounded-xl text-sm font-medium focus:outline-none focus:bg-[#eae7e7] transition-colors"
            />
          </div>

          {/* Color Scheme */}
          <div className="mb-5">
            <label className="text-xs font-bold uppercase tracking-widest text-stone-400 mb-2 block">
              Accent Color
            </label>
            <div className="grid grid-cols-4 gap-2 mb-3">
              {PRESET_COLORS.map((c) => (
                <button
                  key={c.value}
                  className={`w-full aspect-square rounded-xl transition-all flex items-center justify-center ${
                    settings.primaryColor === c.value
                      ? "ring-2 ring-offset-2 ring-stone-400 scale-105"
                      : "hover:scale-105"
                  }`}
                  style={{ backgroundColor: c.value }}
                  onClick={() => {
                    setCustomColor(c.value);
                    apply({ primaryColor: c.value });
                  }}
                  title={c.name}
                >
                  {settings.primaryColor === c.value && (
                    <span className="material-symbols-outlined text-white text-sm">check</span>
                  )}
                </button>
              ))}
            </div>
            {/* Custom color picker */}
            <div className="flex items-center gap-2">
              <input
                type="color"
                value={customColor}
                onChange={(e) => {
                  setCustomColor(e.target.value);
                  apply({ primaryColor: e.target.value });
                }}
                className="w-8 h-8 rounded-lg border-0 cursor-pointer"
              />
              <span className="text-xs text-stone-400 font-mono">{settings.primaryColor}</span>
            </div>
          </div>

          {/* Logo Upload */}
          <div className="mb-4">
            <label className="text-xs font-bold uppercase tracking-widest text-stone-400 mb-2 block">
              Logo
            </label>
            {settings.logoUrl ? (
              <div className="flex items-center gap-3">
                <img
                  src={settings.logoUrl}
                  alt="Logo"
                  className="h-10 w-auto max-w-[120px] object-contain"
                />
                <button
                  className="text-xs text-stone-400 hover:text-red-600 transition-colors"
                  onClick={removeLogo}
                >
                  Remove
                </button>
              </div>
            ) : (
              <button
                className="w-full bg-[#f6f3f2] hover:bg-[#eae7e7] transition-colors px-4 py-3 rounded-xl text-sm text-stone-500 font-medium flex items-center justify-center gap-2"
                onClick={() => fileRef.current?.click()}
              >
                <span className="material-symbols-outlined text-base">upload</span>
                Upload logo
              </button>
            )}
            <input
              ref={fileRef}
              type="file"
              accept="image/*"
              className="hidden"
              onChange={handleLogoUpload}
            />
          </div>

          {/* Reset */}
          <button
            className="w-full text-xs font-bold uppercase tracking-widest text-stone-400 hover:text-stone-600 transition-colors py-2"
            onClick={() => {
              const defaults: BrandSettings = { primaryColor: "#ad2c00", logoUrl: null, storeName: "Lakehouse Market" };
              setLocalSettings(defaults);
              setCustomColor(defaults.primaryColor);
              saveSettings(defaults);
              applyColor(defaults.primaryColor);
              onBrandChange(defaults);
              if (fileRef.current) fileRef.current.value = "";
            }}
          >
            Reset to defaults
          </button>
        </div>
      )}
    </div>
  );
}
