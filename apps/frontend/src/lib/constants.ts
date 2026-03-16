import type { Market, SuggestedQuery } from "./types";

// ─────────────────────────────────────────────────────────
// Markets — DC Metro is the live MVP market (VA + DC + MD)
// ─────────────────────────────────────────────────────────

export const MARKETS: Market[] = [
  {
    id: "dc_metro",
    label: "DC Metro",
    stateCode: null,
    isLive: true,
    networks: [
      {
        id: "inova",
        name: "Inova Health System",
        shortName: "Inova",
        hospitalCount: 5,
      },
      {
        id: "hca_va",
        name: "HCA Virginia",
        shortName: "HCA Virginia",
        hospitalCount: 11,
      },
      {
        id: "uva",
        name: "UVA Health",
        shortName: "UVA Health",
        hospitalCount: 4,
      },
      {
        id: "medstar",
        name: "MedStar Health",
        shortName: "MedStar",
        hospitalCount: 10,
      },
    ],
  },
  {
    id: "nc",
    label: "North Carolina",
    stateCode: "NC",
    isLive: false,
    networks: [],
  },
  {
    id: "national",
    label: "National",
    stateCode: null,
    isLive: false,
    networks: [],
  },
];

export const DEFAULT_MARKET_ID = "dc_metro";

// ─────────────────────────────────────────────────────────
// Suggested queries for DC Metro market
// ─────────────────────────────────────────────────────────

export const VA_SUGGESTED_QUERIES: SuggestedQuery[] = [
  {
    id: "sq1",
    label: "MRI Brain — All Networks",
    query: "Compare negotiated rates for a brain MRI across Inova, HCA Virginia, UVA Health, and MedStar",
    category: "comparison",
  },
  {
    id: "sq2",
    label: "Hip Replacement — Lowest Cost",
    query: "Which hospital has the lowest rate for a total hip replacement across the DC Metro region?",
    category: "procedure",
  },
  {
    id: "sq3",
    label: "Colonoscopy by Payer",
    query: "Show me colonoscopy rates at Inova broken down by insurance plan",
    category: "network",
  },
  {
    id: "sq4",
    label: "Inova vs MedStar",
    query: "Compare median ER visit rates between Inova and MedStar hospitals",
    category: "comparison",
  },
  {
    id: "sq5",
    label: "Employer Cost Estimate",
    query: "What are median knee replacement rates across all 30 hospitals in the DC Metro area?",
    category: "cost-modeling",
  },
  {
    id: "sq6",
    label: "Cardiac Cath Lab",
    query: "Which DC Metro hospital has the lowest rate for cardiac catheterization CPT 93454?",
    category: "procedure",
  },
];
