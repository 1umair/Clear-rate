// ─────────────────────────────────────────────────────────
// Domain types aligned with master_price_graph schema
// ─────────────────────────────────────────────────────────

export type BillingCodeType =
  | "CPT" | "HCPCS" | "MS-DRG" | "APC" | "RC" | "ICD" | "NDC" | "UNKNOWN";

export type NegotiatedType =
  | "negotiated"
  | "derived"
  | "fee schedule"
  | "percent of billed charges"
  | "per diem"
  | "case rate";

export type BillingClass = "professional" | "institutional";

export type MarketId = "dc_metro" | "va" | "md" | "dc" | "nc" | "national";

export interface Market {
  id: MarketId;
  label: string;
  stateCode: string | null;
  networks: NetworkInfo[];
  isLive: boolean;
}

export interface NetworkInfo {
  id: string;
  name: string;
  shortName: string;
  logoUrl?: string;
  hospitalCount: number;
}

export interface PriceRecord {
  id: string;
  networkName: string;
  networkId: string;
  procedureName: string;       // Raw name from MRF
  normalizedName: string;      // Standardized by The Normalizer
  billingCode: string;
  billingCodeType: BillingCodeType;
  billingClass: BillingClass;
  negotiatedType: NegotiatedType;
  negotiatedRate: number;
  planName: string;
  providerNpi?: string;
  providerName?: string;
  city?: string;
  stateCode: string;
  zipCode?: string;
  expirationDate?: string;
  lastUpdated: string;
}

export interface PriceComparison {
  procedure: string;
  normalizedName: string;
  billingCode: string;
  records: PriceRecord[];
  stats: {
    min: number;
    max: number;
    median: number;
    mean: number;
    variance: number;
  };
}

// ─────────────────────────────────────────────────────────
// Chat / Agent types
// ─────────────────────────────────────────────────────────

export type MessageRole = "user" | "assistant" | "system";

export interface ChatMessage {
  id: string;
  role: MessageRole;
  content: string;
  timestamp: Date;
  metadata?: {
    sql?: string;
    rowCount?: number;
    executionMs?: number;
    agentNodes?: string[];
  };
}

export interface QueryRequest {
  query: string;
  marketId: MarketId;
  sessionId?: string;
}

export interface QueryResponse {
  answer: string;
  data?: PriceRecord[];
  sql?: string;
  metadata: {
    agentNodes: string[];
    executionMs: number;
    rowCount: number;
    stateFilter: string;
  };
}

// ─────────────────────────────────────────────────────────
// Suggested queries (seeded per market)
// ─────────────────────────────────────────────────────────

export interface SuggestedQuery {
  id: string;
  label: string;
  query: string;
  category: "comparison" | "network" | "procedure" | "cost-modeling";
}
