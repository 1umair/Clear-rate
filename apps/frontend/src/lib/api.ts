import type { QueryRequest, QueryResponse, PriceComparison } from "./types";

const BACKEND_URL = process.env.NEXT_PUBLIC_BACKEND_URL ?? "http://localhost:8000";

// ─────────────────────────────────────────────────────────
// Core fetch wrapper
// ─────────────────────────────────────────────────────────

async function apiFetch<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${BACKEND_URL}${path}`, {
    headers: { "Content-Type": "application/json" },
    ...init,
  });
  if (!res.ok) {
    const err = await res.text();
    throw new Error(`API error ${res.status}: ${err}`);
  }
  return res.json() as Promise<T>;
}

// ─────────────────────────────────────────────────────────
// Agent query (streaming via Server-Sent Events)
// ─────────────────────────────────────────────────────────

export async function* streamQuery(
  request: QueryRequest
): AsyncGenerator<string, QueryResponse | undefined> {
  const res = await fetch(`${BACKEND_URL}/api/v1/query/stream`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(request),
  });

  if (!res.ok || !res.body) {
    throw new Error(`Stream error ${res.status}`);
  }

  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let finalResponse: QueryResponse | undefined;

  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;

      const chunk = decoder.decode(value, { stream: true });
      const lines = chunk.split("\n");

      for (const line of lines) {
        if (line.startsWith("data: ")) {
          const data = line.slice(6).trim();
          if (data === "[DONE]") continue;
          try {
            const parsed = JSON.parse(data);
            if (parsed.type === "token") yield parsed.content;
            if (parsed.type === "final") finalResponse = parsed.response;
          } catch {
            // Partial JSON chunk — skip
          }
        }
      }
    }
  } finally {
    reader.releaseLock();
  }

  return finalResponse;
}

// ─────────────────────────────────────────────────────────
// Non-streaming query
// ─────────────────────────────────────────────────────────

export async function submitQuery(request: QueryRequest): Promise<QueryResponse> {
  return apiFetch<QueryResponse>("/api/v1/query", {
    method: "POST",
    body: JSON.stringify(request),
  });
}

// ─────────────────────────────────────────────────────────
// Price data endpoints
// ─────────────────────────────────────────────────────────

export async function compareProcedure(
  procedure: string,
  stateCode: string = "VA"
): Promise<PriceComparison> {
  const params = new URLSearchParams({ procedure, state_code: stateCode });
  return apiFetch<PriceComparison>(`/api/v1/prices/compare?${params}`);
}

export async function healthCheck(): Promise<{ status: string; db: string }> {
  return apiFetch("/health");
}
