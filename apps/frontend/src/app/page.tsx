"use client";

import { useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import { Sidebar } from "@/components/layout/Sidebar";
import { ChatInterface } from "@/components/chat/ChatInterface";
import { MARKETS } from "@/lib/constants";
import { MapPin, Building2, Shield, Activity, ArrowRight, Database } from "lucide-react";

type View = "overview" | "chat";

const NETWORK_COLORS: Record<string, string> = {
  inova:   "bg-indigo-600",
  hca_va:  "bg-rose-600",
  uva:     "bg-teal-600",
  medstar: "bg-sky-600",
};

const NETWORK_ICONS: Record<string, string> = {
  inova:   "IN",
  hca_va:  "HC",
  uva:     "UV",
  medstar: "MS",
};

const NETWORK_DESCRIPTIONS: Record<string, string> = {
  inova:   "Northern Virginia's largest nonprofit system. 5 hospitals serving Fairfax, Arlington, Alexandria, and Loudoun.",
  hca_va:  "Statewide for-profit network. 11 hospitals across Northern Virginia, Richmond, and Southwest Virginia.",
  uva:     "Academic medical center network. 4 facilities anchored by UVA Medical Center in Charlottesville.",
  medstar: "DC Metro region's largest nonprofit system. 10 hospitals across Washington DC and Maryland.",
};

const NETWORK_STATES: Record<string, string> = {
  inova:   "Virginia",
  hca_va:  "Virginia",
  uva:     "Virginia",
  medstar: "DC / Maryland",
};

export default function HomePage() {
  const router = useRouter();
  const [view, setView] = useState<View>("overview");

  useEffect(() => {
    if (typeof window !== "undefined" && !localStorage.getItem("cr_auth")) {
      router.push("/login");
    }
  }, [router]);

  const market = MARKETS.find((m) => m.id === "dc_metro")!;
  const totalHospitals = market.networks.reduce((a, n) => a + n.hospitalCount, 0);

  return (
    <div className="flex h-screen overflow-hidden bg-surface-muted">
      <Sidebar activeView={view} onViewChange={setView} />

      <main className="flex flex-col flex-1 min-w-0 overflow-hidden">
        {/* Top bar */}
        <header className="flex items-center justify-between px-6 py-3.5 border-b border-surface-border bg-white shrink-0">
          <div className="flex items-center gap-2 text-sm text-slate-500">
            <MapPin className="w-4 h-4 text-brand-600" />
            <span className="font-medium text-slate-800">DC Metro</span>
          </div>

          <div className="flex items-center gap-3">
            <div className="hidden sm:flex items-center gap-1.5 text-xs text-emerald-600 bg-emerald-50 border border-emerald-200 px-2.5 py-1 rounded-full">
              <div className="w-1.5 h-1.5 rounded-full bg-emerald-500 animate-pulse" />
              Live · CMS MRF 2025-2026
            </div>
            <div className="hidden sm:flex items-center gap-1.5 text-xs text-slate-500 bg-slate-50 border border-slate-200 px-2.5 py-1 rounded-full">
              <Shield className="w-3 h-3" />
              No PHI
            </div>
            <button
              onClick={() => {
                localStorage.removeItem("cr_auth");
                router.push("/login");
              }}
              className="text-xs text-slate-400 hover:text-slate-600 transition-colors"
            >
              Sign out
            </button>
          </div>
        </header>

        {view === "overview" ? (
          <div className="flex-1 overflow-y-auto p-6">
            {/* Market header */}
            <div className="mb-6">
              <h1 className="text-xl font-semibold text-slate-900">DC Metro Market</h1>
              <p className="text-sm text-slate-500 mt-0.5">
                {totalHospitals} hospitals across {market.networks.length} networks · Virginia, Washington DC, and Maryland
              </p>
            </div>

            {/* Summary stat strip */}
            <div className="grid grid-cols-3 gap-3 mb-6">
              <div className="bg-white border border-surface-border rounded-xl p-4">
                <div className="flex items-center gap-2 mb-1">
                  <Building2 className="w-4 h-4 text-slate-400" />
                  <span className="text-xs text-slate-500 uppercase tracking-wide">Hospitals</span>
                </div>
                <p className="text-2xl font-bold text-slate-900">{totalHospitals}</p>
              </div>
              <div className="bg-white border border-surface-border rounded-xl p-4">
                <div className="flex items-center gap-2 mb-1">
                  <Activity className="w-4 h-4 text-slate-400" />
                  <span className="text-xs text-slate-500 uppercase tracking-wide">Price Records</span>
                </div>
                <p className="text-2xl font-bold text-slate-900">13.3M+</p>
              </div>
              <div className="bg-white border border-surface-border rounded-xl p-4">
                <div className="flex items-center gap-2 mb-1">
                  <Database className="w-4 h-4 text-slate-400" />
                  <span className="text-xs text-slate-500 uppercase tracking-wide">Data Source</span>
                </div>
                <p className="text-2xl font-bold text-slate-900">CMS MRF</p>
              </div>
            </div>

            {/* Network cards — informational only */}
            <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-4 mb-8">
              {market.networks.map((network) => (
                <div
                  key={network.id}
                  className="bg-white border border-surface-border rounded-2xl overflow-hidden"
                >
                  <div className={`${NETWORK_COLORS[network.id] ?? "bg-brand-600"} p-4`}>
                    <div className="flex items-start justify-between">
                      <div className="w-9 h-9 rounded-xl bg-white/20 flex items-center justify-center">
                        <span className="text-white text-xs font-bold">
                          {NETWORK_ICONS[network.id] ?? "NW"}
                        </span>
                      </div>
                      <span className="text-xs font-medium text-white/70 bg-white/10 px-2 py-0.5 rounded-full">
                        Live
                      </span>
                    </div>
                    <h2 className="text-white font-semibold text-sm mt-3 leading-snug">
                      {network.name}
                    </h2>
                  </div>

                  <div className="p-4">
                    <div className="flex items-center gap-1.5 mb-2">
                      <Building2 className="w-3.5 h-3.5 text-slate-400" />
                      <span className="text-xs text-slate-600">{network.hospitalCount} hospitals</span>
                      <span className="text-slate-300 mx-1">·</span>
                      <MapPin className="w-3.5 h-3.5 text-slate-400" />
                      <span className="text-xs text-slate-600">{NETWORK_STATES[network.id]}</span>
                    </div>
                    <p className="text-xs text-slate-500 leading-relaxed">
                      {NETWORK_DESCRIPTIONS[network.id]}
                    </p>
                  </div>
                </div>
              ))}
            </div>

            {/* Unified CTA */}
            <div className="bg-slate-900 rounded-2xl p-6 flex items-center justify-between">
              <div>
                <h3 className="text-white font-semibold text-base">Compare prices across all {totalHospitals} hospitals</h3>
                <p className="text-slate-400 text-sm mt-0.5">
                  Ask in plain English — compare rates by procedure, payer, or network across the entire DC Metro region
                </p>
              </div>
              <button
                onClick={() => setView("chat")}
                className="flex items-center gap-2 px-5 py-3 bg-brand-600 hover:bg-brand-700 text-white rounded-xl font-medium text-sm transition-colors shrink-0 ml-6"
              >
                Start comparing
                <ArrowRight className="w-4 h-4" />
              </button>
            </div>

            {/* Coming soon markets */}
            <div className="mt-8">
              <p className="text-xs font-medium text-slate-400 mb-3 uppercase tracking-wider">
                Coming Soon
              </p>
              <div className="flex gap-3">
                {MARKETS.filter((m) => !m.isLive).map((market) => (
                  <div
                    key={market.id}
                    className="flex items-center gap-2 bg-white border border-surface-border rounded-xl px-4 py-3 opacity-50"
                  >
                    <MapPin className="w-3.5 h-3.5 text-slate-400" />
                    <span className="text-sm text-slate-500">{market.label}</span>
                    <span className="text-xs text-slate-400 bg-slate-100 px-1.5 py-0.5 rounded-full">
                      Soon
                    </span>
                  </div>
                ))}
              </div>
            </div>
          </div>
        ) : (
          <div className="flex-1 min-h-0">
            <ChatInterface marketId="dc_metro" networkId={null} />
          </div>
        )}
      </main>
    </div>
  );
}
