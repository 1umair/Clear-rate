"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";

export default function LoginPage() {
  const router = useRouter();
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  const handleLogin = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!email || !password) {
      setError("Please enter your email and password.");
      return;
    }
    setLoading(true);
    setError("");
    // Simulated auth — replace with real API call
    await new Promise((r) => setTimeout(r, 800));
    localStorage.setItem("cr_auth", "1");
    router.push("/");
  };

  return (
    <div className="min-h-screen flex">
      {/* Left panel — branding */}
      <div className="hidden lg:flex flex-col justify-between w-1/2 bg-slate-900 p-12 relative overflow-hidden">
        {/* Background pattern */}
        <div
          className="absolute inset-0 opacity-[0.03]"
          style={{
            backgroundImage:
              "radial-gradient(circle at 1px 1px, white 1px, transparent 0)",
            backgroundSize: "32px 32px",
          }}
        />
        {/* Logo */}
        <div className="flex items-center gap-3 relative">
          <div className="w-9 h-9 rounded-xl bg-brand-600 flex items-center justify-center">
            <span className="text-white text-sm font-bold">CR</span>
          </div>
          <span className="text-white font-semibold text-lg tracking-wide">ClearRate</span>
        </div>

        {/* Value props */}
        <div className="relative space-y-8">
          <div>
            <h1 className="text-4xl font-bold text-white leading-tight">
              Healthcare pricing,{" "}
              <span className="text-brand-400">finally transparent.</span>
            </h1>
            <p className="mt-4 text-slate-400 text-lg leading-relaxed">
              Query CMS machine-readable files across Virginia hospital networks
              with natural language. Built for self-funded employers, TPAs, and
              benefit consultants.
            </p>
          </div>

          <div className="grid grid-cols-3 gap-4">
            {[
              { stat: "25+", label: "Hospitals" },
              { stat: "3", label: "Networks" },
              { stat: "CMS", label: "April 2026" },
            ].map(({ stat, label }) => (
              <div key={label} className="bg-slate-800 rounded-xl p-4 border border-slate-700">
                <p className="text-2xl font-bold text-white">{stat}</p>
                <p className="text-xs text-slate-400 mt-0.5">{label}</p>
              </div>
            ))}
          </div>

          <div className="flex items-center gap-2 text-xs text-slate-500">
            <div className="w-1.5 h-1.5 rounded-full bg-emerald-500" />
            Virginia market live · No PHI stored · SOC 2 ready
          </div>
        </div>

        <p className="text-xs text-slate-600 relative">
          &copy; {new Date().getFullYear()} ClearRate Inc. · Healthcare Price Intelligence
        </p>
      </div>

      {/* Right panel — login form */}
      <div className="flex-1 flex flex-col items-center justify-center px-6 py-12 bg-surface-muted">
        {/* Mobile logo */}
        <div className="flex items-center gap-2.5 mb-8 lg:hidden">
          <div className="w-8 h-8 rounded-lg bg-brand-600 flex items-center justify-center">
            <span className="text-white text-xs font-bold">CR</span>
          </div>
          <span className="text-slate-900 font-semibold text-base">ClearRate</span>
        </div>

        <div className="w-full max-w-sm">
          <h2 className="text-2xl font-bold text-slate-900">Sign in</h2>
          <p className="mt-1 text-sm text-slate-500">
            Access your price intelligence dashboard
          </p>

          <form onSubmit={handleLogin} className="mt-8 space-y-4">
            <div>
              <label className="block text-xs font-medium text-slate-700 mb-1.5">
                Work email
              </label>
              <input
                type="email"
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                placeholder="you@company.com"
                className="w-full px-3.5 py-2.5 rounded-lg border border-surface-border bg-white text-sm text-slate-900 placeholder:text-slate-400 focus:outline-none focus:ring-2 focus:ring-brand-500 focus:border-transparent transition-all"
              />
            </div>

            <div>
              <div className="flex items-center justify-between mb-1.5">
                <label className="block text-xs font-medium text-slate-700">
                  Password
                </label>
                <a href="#" className="text-xs text-brand-600 hover:text-brand-700">
                  Forgot password?
                </a>
              </div>
              <input
                type="password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                placeholder="••••••••"
                className="w-full px-3.5 py-2.5 rounded-lg border border-surface-border bg-white text-sm text-slate-900 placeholder:text-slate-400 focus:outline-none focus:ring-2 focus:ring-brand-500 focus:border-transparent transition-all"
              />
            </div>

            {error && (
              <p className="text-xs text-red-600 bg-red-50 border border-red-200 rounded-lg px-3 py-2">
                {error}
              </p>
            )}

            <button
              type="submit"
              disabled={loading}
              className="w-full py-2.5 rounded-lg bg-brand-600 hover:bg-brand-700 text-white text-sm font-medium transition-colors disabled:opacity-60 disabled:cursor-not-allowed flex items-center justify-center gap-2"
            >
              {loading ? (
                <>
                  <span className="w-4 h-4 border-2 border-white/30 border-t-white rounded-full animate-spin" />
                  Signing in…
                </>
              ) : (
                "Sign in"
              )}
            </button>
          </form>

          <div className="mt-6 p-3.5 bg-blue-50 border border-blue-200 rounded-lg">
            <p className="text-xs text-blue-700 font-medium">Demo access</p>
            <p className="text-xs text-blue-600 mt-0.5">
              Use any email and password to sign in during the beta.
            </p>
          </div>

          <p className="mt-8 text-center text-xs text-slate-400">
            Don&apos;t have an account?{" "}
            <a href="#" className="text-brand-600 hover:text-brand-700 font-medium">
              Request access
            </a>
          </p>
        </div>
      </div>
    </div>
  );
}
