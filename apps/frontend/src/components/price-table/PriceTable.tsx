"use client";

import { ArrowUpDown, TrendingDown, TrendingUp, Minus } from "lucide-react";
import type { PriceRecord } from "@/lib/types";
import { cn, formatCurrency, priceTier } from "@/lib/utils";

interface PriceTableProps {
  records: PriceRecord[];
  median: number;
}

export function PriceTable({ records, median }: PriceTableProps) {
  if (!records.length) return null;

  return (
    <div className="mt-3 overflow-hidden rounded-lg border border-surface-border bg-white">
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-surface-border bg-surface-muted">
              <th className="text-left px-4 py-2.5 text-xs font-semibold text-slate-500 uppercase tracking-wide">
                Provider / Network
              </th>
              <th className="text-left px-4 py-2.5 text-xs font-semibold text-slate-500 uppercase tracking-wide">
                Plan
              </th>
              <th className="text-left px-4 py-2.5 text-xs font-semibold text-slate-500 uppercase tracking-wide">
                Code
              </th>
              <th className="text-right px-4 py-2.5 text-xs font-semibold text-slate-500 uppercase tracking-wide">
                Rate
              </th>
              <th className="text-center px-4 py-2.5 text-xs font-semibold text-slate-500 uppercase tracking-wide">
                vs Median
              </th>
            </tr>
          </thead>
          <tbody className="divide-y divide-surface-border">
            {records.map((record) => {
              const tier = priceTier(record.negotiatedRate, median);
              const delta = ((record.negotiatedRate - median) / median) * 100;

              return (
                <tr key={record.id} className="hover:bg-surface-muted/50 transition-colors">
                  <td className="px-4 py-3">
                    <div className="font-medium text-slate-800">{record.networkName}</div>
                    {record.providerName && (
                      <div className="text-xs text-slate-400 mt-0.5">{record.providerName}</div>
                    )}
                    {record.city && (
                      <div className="text-xs text-slate-400">{record.city}, {record.stateCode}</div>
                    )}
                  </td>
                  <td className="px-4 py-3">
                    <span className="text-xs text-slate-600 truncate max-w-[140px] block">
                      {record.planName}
                    </span>
                  </td>
                  <td className="px-4 py-3">
                    <span className="font-mono text-xs bg-slate-100 px-1.5 py-0.5 rounded text-slate-700">
                      {record.billingCodeType} {record.billingCode}
                    </span>
                  </td>
                  <td className="px-4 py-3 text-right">
                    <span className="font-semibold text-slate-900">
                      {formatCurrency(record.negotiatedRate)}
                    </span>
                  </td>
                  <td className="px-4 py-3 text-center">
                    <div className="flex items-center justify-center gap-1">
                      {tier === "low" && (
                        <>
                          <TrendingDown className="w-3.5 h-3.5 text-emerald-600" />
                          <span className="price-badge-low">{Math.abs(delta).toFixed(0)}% below</span>
                        </>
                      )}
                      {tier === "high" && (
                        <>
                          <TrendingUp className="w-3.5 h-3.5 text-red-500" />
                          <span className="price-badge-high">{Math.abs(delta).toFixed(0)}% above</span>
                        </>
                      )}
                      {tier === "mid" && (
                        <>
                          <Minus className="w-3.5 h-3.5 text-amber-500" />
                          <span className="price-badge-mid">Near median</span>
                        </>
                      )}
                    </div>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}
