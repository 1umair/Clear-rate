"use client";

import { LayoutDashboard, MessageSquare, Settings, HelpCircle } from "lucide-react";
import { cn } from "@/lib/utils";

type View = "overview" | "chat";

interface SidebarProps {
  activeView: View;
  onViewChange: (view: View) => void;
}

const nav = [
  { view: "overview" as View, icon: LayoutDashboard, label: "Markets" },
  { view: "chat" as View, icon: MessageSquare, label: "Query" },
];

export function Sidebar({ activeView, onViewChange }: SidebarProps) {
  return (
    <aside className="flex flex-col w-16 lg:w-56 h-screen bg-slate-900 border-r border-slate-800 shrink-0">
      {/* Logo */}
      <div className="flex items-center gap-2.5 px-3 lg:px-4 py-5 border-b border-slate-800">
        <div className="w-8 h-8 rounded-lg bg-brand-600 flex items-center justify-center shrink-0">
          <span className="text-white text-xs font-bold">CR</span>
        </div>
        <span className="hidden lg:block text-white font-semibold text-sm tracking-wide">
          ClearRate
        </span>
      </div>

      {/* Primary Nav */}
      <nav className="flex flex-col gap-1 p-2 flex-1 mt-2">
        {nav.map(({ view, icon: Icon, label }) => (
          <button
            key={view}
            onClick={() => onViewChange(view)}
            className={cn(
              "flex items-center gap-3 px-2 lg:px-3 py-2.5 rounded-lg text-sm font-medium transition-colors w-full text-left",
              activeView === view
                ? "bg-brand-600 text-white"
                : "text-slate-400 hover:text-white hover:bg-slate-800"
            )}
          >
            <Icon className="w-4 h-4 shrink-0" />
            <span className="hidden lg:block">{label}</span>
          </button>
        ))}
      </nav>

      {/* Bottom Nav */}
      <div className="p-2 border-t border-slate-800 space-y-1">
        {[
          { icon: Settings, label: "Settings" },
          { icon: HelpCircle, label: "Help" },
        ].map(({ icon: Icon, label }) => (
          <button
            key={label}
            className="flex items-center gap-3 px-2 lg:px-3 py-2.5 rounded-lg text-sm font-medium text-slate-500 hover:text-white hover:bg-slate-800 transition-colors w-full text-left"
          >
            <Icon className="w-4 h-4 shrink-0" />
            <span className="hidden lg:block">{label}</span>
          </button>
        ))}
      </div>
    </aside>
  );
}
