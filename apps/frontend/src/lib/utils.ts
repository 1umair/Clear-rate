import { clsx, type ClassValue } from "clsx";
import { twMerge } from "tailwind-merge";

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

export function formatCurrency(value: number): string {
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  }).format(value);
}

export function formatPercent(value: number): string {
  return new Intl.NumberFormat("en-US", {
    style: "percent",
    minimumFractionDigits: 1,
  }).format(value / 100);
}

export function generateId(): string {
  return Math.random().toString(36).slice(2, 11);
}

export function priceTier(value: number, median: number): "low" | "mid" | "high" {
  const ratio = value / median;
  if (ratio < 0.85) return "low";
  if (ratio > 1.2) return "high";
  return "mid";
}
