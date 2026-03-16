"use client";

import { useCallback, useRef, useState } from "react";
import { SendHorizontal, Loader2, Sparkles } from "lucide-react";
import { generateId } from "@/lib/utils";
import { submitQuery } from "@/lib/api";
import type { ChatMessage, MarketId, QueryResponse } from "@/lib/types";
import { VA_SUGGESTED_QUERIES } from "@/lib/constants";
import { MessageBubble } from "./MessageBubble";
import { cn } from "@/lib/utils";

const WELCOME_MESSAGE: ChatMessage = {
  id: "welcome",
  role: "assistant",
  content: "Hello! I'm your healthcare price intelligence assistant. I have access to negotiated rate data from **30 hospitals** across **Inova Health**, **HCA Virginia**, **UVA Health**, and **MedStar Health** in the DC Metro region.\n\nAsk me anything — compare procedure rates across networks, benchmark a specific CPT code, model costs for your employee population, or drill into payer-specific rates at any hospital.",
  timestamp: new Date(),
};

interface ChatInterfaceProps {
  marketId: MarketId;
  networkId?: string | null;
}

export function ChatInterface({ marketId, networkId }: ChatInterfaceProps) {
  const [messages, setMessages] = useState<ChatMessage[]>([WELCOME_MESSAGE]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const bottomRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLTextAreaElement>(null);

  const sendMessage = useCallback(
    async (query: string) => {
      if (!query.trim() || loading) return;

      const userMsg: ChatMessage = {
        id: generateId(),
        role: "user",
        content: query.trim(),
        timestamp: new Date(),
      };

      setMessages((prev) => [...prev, userMsg]);
      setInput("");
      setLoading(true);

      // Optimistic assistant placeholder
      const assistantId = generateId();
      setMessages((prev) => [
        ...prev,
        { id: assistantId, role: "assistant", content: "", timestamp: new Date() },
      ]);

      try {
        const response: QueryResponse = await submitQuery({
          query: query.trim(),
          marketId,
          sessionId: "session-" + Date.now(),
        });

        setMessages((prev) =>
          prev.map((m) =>
            m.id === assistantId
              ? {
                  ...m,
                  content: response.answer,
                  metadata: {
                    sql: response.sql,
                    rowCount: response.metadata.rowCount,
                    executionMs: response.metadata.executionMs,
                    agentNodes: response.metadata.agentNodes,
                  },
                }
              : m
          )
        );
      } catch (err) {
        setMessages((prev) =>
          prev.map((m) =>
            m.id === assistantId
              ? {
                  ...m,
                  content:
                    "I encountered an error processing your request. Please check that the backend is running and try again.",
                }
              : m
          )
        );
      } finally {
        setLoading(false);
        setTimeout(() => bottomRef.current?.scrollIntoView({ behavior: "smooth" }), 100);
      }
    },
    [loading, marketId]
  );

  const handleKeyDown = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      sendMessage(input);
    }
  };

  const isEmpty = messages.length === 1 && messages[0].id === "welcome";

  return (
    <div className="flex flex-col h-full">
      {/* Messages */}
      <div className="flex-1 overflow-y-auto custom-scrollbar px-4 py-6 space-y-4">
        {messages.map((msg) => (
          <MessageBubble key={msg.id} message={msg} />
        ))}

        {/* Suggested queries — shown only on fresh session */}
        {isEmpty && (
          <div className="mt-6">
            <p className="text-xs text-slate-400 font-medium mb-3 flex items-center gap-1.5">
              <Sparkles className="w-3.5 h-3.5" />
              Try these queries
            </p>
            <div className="grid grid-cols-1 sm:grid-cols-2 gap-2">
              {VA_SUGGESTED_QUERIES.map((sq) => (
                <button
                  key={sq.id}
                  onClick={() => sendMessage(sq.query)}
                  className="text-left px-3 py-2.5 rounded-lg border border-surface-border bg-white hover:border-brand-500 hover:bg-brand-50 transition-colors text-sm text-slate-600 hover:text-brand-700"
                >
                  <span className="font-medium text-slate-800 block text-xs mb-0.5">
                    {sq.label}
                  </span>
                  {sq.query}
                </button>
              ))}
            </div>
          </div>
        )}

        <div ref={bottomRef} />
      </div>

      {/* Input */}
      <div className="border-t border-surface-border p-4 bg-white">
        <div className="flex items-end gap-3 max-w-4xl mx-auto">
          <div className="flex-1 relative">
            <textarea
              ref={inputRef}
              value={input}
              onChange={(e) => setInput(e.target.value)}
              onKeyDown={handleKeyDown}
              placeholder="Ask about procedure rates, network comparisons, cost estimates..."
              rows={1}
              className={cn(
                "w-full resize-none rounded-xl border border-surface-border bg-surface-muted px-4 py-3 pr-12",
                "text-sm text-slate-800 placeholder:text-slate-400",
                "focus:outline-none focus:ring-2 focus:ring-brand-500 focus:border-transparent",
                "transition-all min-h-[48px] max-h-40"
              )}
              style={{ height: "auto" }}
              onInput={(e) => {
                const el = e.currentTarget;
                el.style.height = "auto";
                el.style.height = Math.min(el.scrollHeight, 160) + "px";
              }}
            />
          </div>
          <button
            onClick={() => sendMessage(input)}
            disabled={!input.trim() || loading}
            className={cn(
              "shrink-0 w-10 h-10 rounded-xl flex items-center justify-center transition-colors",
              input.trim() && !loading
                ? "bg-brand-600 hover:bg-brand-700 text-white"
                : "bg-slate-100 text-slate-300 cursor-not-allowed"
            )}
          >
            {loading ? (
              <Loader2 className="w-4 h-4 animate-spin" />
            ) : (
              <SendHorizontal className="w-4 h-4" />
            )}
          </button>
        </div>
        <p className="text-center text-[11px] text-slate-400 mt-2">
          Rates sourced from CMS machine-readable files · Virginia market · Data may not reflect current rates
        </p>
      </div>
    </div>
  );
}
