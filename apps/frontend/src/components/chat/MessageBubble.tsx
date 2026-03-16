"use client";

import { useState } from "react";
import { ChevronDown, ChevronUp, Code2, Loader2 } from "lucide-react";
import type { ChatMessage } from "@/lib/types";
import { cn } from "@/lib/utils";

// Simple markdown renderer (avoids heavy deps in MVP)
function renderMarkdown(text: string): string {
  return text
    .replace(/\*\*(.*?)\*\*/g, "<strong>$1</strong>")
    .replace(/\*(.*?)\*/g, "<em>$1</em>")
    .replace(/`(.*?)`/g, '<code class="bg-slate-100 px-1 py-0.5 rounded text-xs font-mono">$1</code>')
    .replace(/\n/g, "<br/>");
}

interface MessageBubbleProps {
  message: ChatMessage;
}

export function MessageBubble({ message }: MessageBubbleProps) {
  const [showSql, setShowSql] = useState(false);
  const isUser = message.role === "user";
  const isEmpty = !message.content && message.role === "assistant";

  return (
    <div className={cn("flex", isUser ? "justify-end" : "justify-start")}>
      <div className="flex flex-col gap-1 max-w-[85%]">
        {/* Avatar */}
        {!isUser && (
          <div className="flex items-center gap-2 mb-0.5">
            <div className="w-5 h-5 rounded-full bg-brand-600 flex items-center justify-center">
              <span className="text-white text-[8px] font-bold">CR</span>
            </div>
            <span className="text-[11px] text-slate-400 font-medium">ClearRate AI</span>
          </div>
        )}

        {/* Bubble */}
        {isUser ? (
          <div className="chat-message-user">{message.content}</div>
        ) : (
          <div className="chat-message-assistant">
            {isEmpty ? (
              <span className="flex items-center gap-2 text-slate-400">
                <Loader2 className="w-3.5 h-3.5 animate-spin" />
                Analyzing pricing data...
              </span>
            ) : (
              <div
                className="prose-sm prose-slate max-w-none"
                dangerouslySetInnerHTML={{ __html: renderMarkdown(message.content) }}
              />
            )}
          </div>
        )}

        {/* Metadata footer */}
        {message.metadata && !isEmpty && (
          <div className="flex items-center gap-3 mt-1 ml-1">
            {message.metadata.rowCount !== undefined && (
              <span className="text-[11px] text-slate-400">
                {message.metadata.rowCount.toLocaleString()} records
              </span>
            )}
            {message.metadata.executionMs !== undefined && (
              <span className="text-[11px] text-slate-400">
                {message.metadata.executionMs}ms
              </span>
            )}
            {message.metadata.sql && (
              <button
                onClick={() => setShowSql((v) => !v)}
                className="flex items-center gap-1 text-[11px] text-brand-600 hover:text-brand-700 font-medium"
              >
                <Code2 className="w-3 h-3" />
                {showSql ? "Hide SQL" : "View SQL"}
                {showSql ? <ChevronUp className="w-3 h-3" /> : <ChevronDown className="w-3 h-3" />}
              </button>
            )}
          </div>
        )}

        {/* SQL drawer */}
        {showSql && message.metadata?.sql && (
          <div className="mt-1 p-3 bg-slate-900 rounded-lg overflow-x-auto">
            <pre className="text-xs text-emerald-400 font-mono whitespace-pre-wrap">
              {message.metadata.sql}
            </pre>
          </div>
        )}
      </div>
    </div>
  );
}
