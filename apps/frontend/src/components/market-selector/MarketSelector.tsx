"use client";

import { ChevronDown, MapPin } from "lucide-react";
import { useState } from "react";
import { MARKETS } from "@/lib/constants";
import type { Market, MarketId } from "@/lib/types";
import { cn } from "@/lib/utils";

interface MarketSelectorProps {
  selected: MarketId;
  onChange: (id: MarketId) => void;
}

export function MarketSelector({ selected, onChange }: MarketSelectorProps) {
  const [open, setOpen] = useState(false);
  const current = MARKETS.find((m) => m.id === selected) ?? MARKETS[0];

  return (
    <div className="relative">
      <button
        onClick={() => setOpen((v) => !v)}
        className="flex items-center gap-2 px-3 py-2 rounded-lg border border-surface-border bg-white hover:bg-surface-muted transition-colors text-sm font-medium text-slate-700"
      >
        <MapPin className="w-3.5 h-3.5 text-brand-600" />
        <span>{current.label}</span>
        {current.isLive && (
          <span className="ml-1 bg-emerald-100 text-emerald-700 text-[10px] font-semibold px-1.5 py-0.5 rounded-full">
            LIVE
          </span>
        )}
        <ChevronDown className={cn("w-3.5 h-3.5 text-slate-400 transition-transform", open && "rotate-180")} />
      </button>

      {open && (
        <>
          <div className="fixed inset-0 z-10" onClick={() => setOpen(false)} />
          <div className="absolute top-full left-0 mt-1 w-52 bg-white border border-surface-border rounded-lg shadow-lg z-20 py-1">
            {MARKETS.map((market) => (
              <MarketOption
                key={market.id}
                market={market}
                isSelected={market.id === selected}
                onSelect={(id) => { onChange(id); setOpen(false); }}
              />
            ))}
          </div>
        </>
      )}
    </div>
  );
}

function MarketOption({
  market,
  isSelected,
  onSelect,
}: {
  market: Market;
  isSelected: boolean;
  onSelect: (id: MarketId) => void;
}) {
  return (
    <button
      onClick={() => market.isLive && onSelect(market.id)}
      disabled={!market.isLive}
      className={cn(
        "w-full flex items-center justify-between px-3 py-2 text-sm transition-colors",
        isSelected ? "bg-brand-50 text-brand-700 font-medium" : "text-slate-700 hover:bg-surface-muted",
        !market.isLive && "opacity-40 cursor-not-allowed"
      )}
    >
      <span>{market.label}</span>
      <div className="flex items-center gap-1.5">
        {market.isLive ? (
          <span className="text-[10px] font-semibold text-emerald-600 bg-emerald-50 px-1.5 py-0.5 rounded-full">
            LIVE
          </span>
        ) : (
          <span className="text-[10px] font-semibold text-slate-400 bg-slate-100 px-1.5 py-0.5 rounded-full">
            SOON
          </span>
        )}
      </div>
    </button>
  );
}
