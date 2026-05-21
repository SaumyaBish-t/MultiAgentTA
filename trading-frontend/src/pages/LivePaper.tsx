import React, { useState, useEffect } from 'react';
import axios from 'axios';
import {
  LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid,
} from 'recharts';
import {
  Wallet, Eye, Rocket, Loader2, AlertTriangle, ArrowDownRight, ArrowUpRight,
  Minus, TrendingUp,
} from 'lucide-react';

const MONITOR = import.meta.env.VITE_MONITOR_URL;

const STRATEGIES = [
  { value: 'trend-following', label: 'Trend-Following' },
  { value: 'multi-factor', label: 'Multi-Factor' },
  { value: 'momentum', label: 'Pure Momentum' },
];

const money = (n: number) => `$${(n ?? 0).toLocaleString(undefined, { maximumFractionDigits: 0 })}`;

const LivePaper: React.FC = () => {
  const [strategy, setStrategy] = useState('trend-following');
  const [plan, setPlan] = useState<any>(null);
  const [executed, setExecuted] = useState<any>(null);
  const [loading, setLoading] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [perf, setPerf] = useState<any>(null);

  const loadPerformance = () => {
    axios.get(`${MONITOR}/live/performance`)
      .then(({ data }) => { if (!data.error) setPerf(data); })
      .catch(() => { /* leave perf null */ });
  };
  useEffect(loadPerformance, []);

  const previewPlan = async () => {
    setLoading('preview'); setError(null); setExecuted(null); setPlan(null);
    try {
      const { data } = await axios.get(`${MONITOR}/live/rebalance/plan`, { params: { strategy } });
      if (data.error) setError(data.error);
      else setPlan(data);
    } catch (e: any) {
      setError(e?.message || 'request failed');
    } finally {
      setLoading(null);
    }
  };

  const executeRebalance = async () => {
    if (!window.confirm('Submit these PAPER orders to the Alpaca paper account?')) return;
    setLoading('execute'); setError(null);
    try {
      const { data } = await axios.post(`${MONITOR}/live/rebalance/execute`, { strategy });
      if (data.error) setError(data.error);
      else { setExecuted(data); setPlan(data); loadPerformance(); }
    } catch (e: any) {
      setError(e?.message || 'request failed');
    } finally {
      setLoading(null);
    }
  };

  return (
    <div>
      <div className="mb-6">
        <h1 className="text-2xl font-black text-text-primary flex items-center space-x-2">
          <Wallet className="text-indigo-600" />
          <span>Live Paper Trading</span>
        </h1>
        <p className="text-sm text-text-muted mt-1">
          Take a backtested strategy live on the Alpaca <b>paper</b> account. Preview the
          rebalance, then execute. Re-run every month — that builds your forward track record.
        </p>
      </div>

      {/* paper-trading banner */}
      <div className="bg-amber-50 border border-amber-200 rounded-xl p-3 mb-6 flex items-center space-x-2">
        <AlertTriangle size={18} className="text-amber-600 shrink-0" />
        <p className="text-xs text-amber-800">
          <b>Paper trading only.</b> All orders are simulated on Alpaca's paper account —
          no real money is ever at risk. US equities only.
        </p>
      </div>

      {/* live performance */}
      {perf && (
        <div className="bg-white rounded-xl border border-border p-6 mb-6">
          <h3 className="font-bold text-text-primary mb-4 flex items-center space-x-2">
            <TrendingUp size={18} className="text-indigo-600" />
            <span>Live Performance</span>
          </h3>
          <div className="grid grid-cols-2 md:grid-cols-3 gap-3 mb-4">
            <div className="bg-page rounded-xl p-4">
              <p className="text-[10px] font-bold text-text-muted uppercase">Total Return</p>
              <p className={`text-2xl font-mono font-bold mt-1 ${(perf.total_return_pct ?? 0) >= 0 ? 'text-positive' : 'text-negative'}`}>
                {(perf.total_return_pct ?? 0) >= 0 ? '+' : ''}{(perf.total_return_pct ?? 0).toFixed(2)}%
              </p>
            </div>
            <div className="bg-page rounded-xl p-4">
              <p className="text-[10px] font-bold text-text-muted uppercase">Current Equity</p>
              <p className="text-2xl font-mono font-bold text-text-primary mt-1">{money(perf.current_equity)}</p>
            </div>
            <div className="bg-page rounded-xl p-4">
              <p className="text-[10px] font-bold text-text-muted uppercase">P&amp;L vs $100k Start</p>
              <p className={`text-2xl font-mono font-bold mt-1 ${(perf.total_pl ?? 0) >= 0 ? 'text-positive' : 'text-negative'}`}>
                {(perf.total_pl ?? 0) >= 0 ? '+' : ''}{money(perf.total_pl)}
              </p>
            </div>
          </div>
          {perf.equity_curve?.length > 1 && (
            <div className="h-56">
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={perf.equity_curve}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#E2E8F0" />
                  <XAxis dataKey="date" tick={{ fontSize: 10 }} minTickGap={48} />
                  <YAxis tick={{ fontSize: 10 }} domain={['auto', 'auto']}
                    tickFormatter={(v) => `$${(v / 1000).toFixed(0)}k`} />
                  <Tooltip formatter={(v: any) => `$${Number(v).toLocaleString()}`} />
                  <Line type="monotone" dataKey="value" stroke="#4F63D2" strokeWidth={2} dot={false} />
                </LineChart>
              </ResponsiveContainer>
            </div>
          )}
          <p className="text-[10px] text-text-muted mt-2">
            This is your forward (live-paper) track record — the honest test against the backtest.
          </p>
        </div>
      )}

      {/* controls */}
      <div className="bg-white rounded-xl border border-border p-4 mb-6 flex flex-wrap items-center gap-3">
        <span className="text-xs font-bold text-text-muted uppercase">Strategy</span>
        <select
          value={strategy}
          onChange={(e) => { setStrategy(e.target.value); setPlan(null); setExecuted(null); }}
          className="px-3 py-2 bg-page rounded-lg text-sm font-semibold outline-none"
        >
          {STRATEGIES.map((s) => <option key={s.value} value={s.value}>{s.label}</option>)}
        </select>
        <button
          onClick={previewPlan}
          disabled={!!loading}
          className="flex items-center space-x-2 px-4 py-2 bg-indigo-600 text-white text-sm font-bold rounded-xl hover:bg-indigo-700 disabled:opacity-50"
        >
          {loading === 'preview' ? <Loader2 size={16} className="animate-spin" /> : <Eye size={16} />}
          <span>Preview Plan</span>
        </button>
        <button
          onClick={executeRebalance}
          disabled={!!loading || !plan || !!executed}
          title={executed ? 'Already executed — Preview again to rebalance once more' : ''}
          className="flex items-center space-x-2 px-4 py-2 bg-emerald-600 text-white text-sm font-bold rounded-xl hover:bg-emerald-700 disabled:opacity-40 disabled:cursor-not-allowed"
        >
          {loading === 'execute' ? <Loader2 size={16} className="animate-spin" /> : <Rocket size={16} />}
          <span>{executed ? 'Executed ✓' : 'Execute Rebalance'}</span>
        </button>
      </div>

      {error && (
        <div className="bg-red-50 border border-red-200 text-red-700 rounded-xl p-4 mb-6 text-sm">{error}</div>
      )}

      {plan && (
        <>
          {/* account summary */}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mb-6">
            <div className="bg-white rounded-xl border border-border p-4">
              <p className="text-[10px] font-bold text-text-muted uppercase">Paper Equity</p>
              <p className="text-xl font-mono font-bold text-text-primary mt-1">{money(plan.equity)}</p>
            </div>
            <div className="bg-white rounded-xl border border-border p-4">
              <p className="text-[10px] font-bold text-text-muted uppercase">Cash</p>
              <p className="text-xl font-mono font-bold text-text-primary mt-1">{money(plan.cash)}</p>
            </div>
            <div className="bg-white rounded-xl border border-border p-4">
              <p className="text-[10px] font-bold text-text-muted uppercase">Target Names</p>
              <p className="text-xl font-mono font-bold text-text-primary mt-1">{plan.targets?.length ?? 0}</p>
            </div>
            <div className="bg-white rounded-xl border border-border p-4">
              <p className="text-[10px] font-bold text-text-muted uppercase">Per Name</p>
              <p className="text-xl font-mono font-bold text-text-primary mt-1">{money(plan.notional_per_name)}</p>
            </div>
          </div>

          {/* rebalance plan */}
          <div className="bg-white rounded-xl border border-border p-6 mb-6">
            <h3 className="font-bold text-text-primary mb-4">
              {executed ? 'Rebalance Executed' : 'Rebalance Plan'}
            </h3>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              <div>
                <p className="text-[10px] font-bold text-negative uppercase flex items-center space-x-1 mb-2">
                  <ArrowDownRight size={12} /><span>Sell ({plan.to_sell?.length ?? 0})</span>
                </p>
                <div className="flex flex-wrap gap-1.5">
                  {(plan.to_sell ?? []).map((t: string) => (
                    <span key={t} className="px-2 py-1 bg-red-50 text-red-700 rounded text-xs font-bold font-mono">{t}</span>
                  ))}
                  {(plan.to_sell ?? []).length === 0 && <span className="text-xs text-text-muted">none</span>}
                </div>
              </div>
              <div>
                <p className="text-[10px] font-bold text-positive uppercase flex items-center space-x-1 mb-2">
                  <ArrowUpRight size={12} /><span>Buy ({plan.to_buy?.length ?? 0})</span>
                </p>
                <div className="flex flex-wrap gap-1.5">
                  {(plan.to_buy ?? []).map((t: string) => (
                    <span key={t} className="px-2 py-1 bg-emerald-50 text-emerald-700 rounded text-xs font-bold font-mono">{t}</span>
                  ))}
                  {(plan.to_buy ?? []).length === 0 && <span className="text-xs text-text-muted">none</span>}
                </div>
              </div>
              <div>
                <p className="text-[10px] font-bold text-text-muted uppercase flex items-center space-x-1 mb-2">
                  <Minus size={12} /><span>Hold ({plan.retained?.length ?? 0})</span>
                </p>
                <div className="flex flex-wrap gap-1.5">
                  {(plan.retained ?? []).map((t: string) => (
                    <span key={t} className="px-2 py-1 bg-gray-100 text-gray-600 rounded text-xs font-bold font-mono">{t}</span>
                  ))}
                  {(plan.retained ?? []).length === 0 && <span className="text-xs text-text-muted">none</span>}
                </div>
              </div>
            </div>

            {executed && (
              <div className="mt-5 pt-4 border-t border-border">
                <p className="text-[10px] font-bold text-text-muted uppercase mb-2">
                  Orders Submitted ({plan.order_count ?? 0})
                </p>
                <div className="space-y-1">
                  {(plan.executed ?? []).map((o: any, i: number) => (
                    <p key={i} className="text-xs font-mono">
                      <span className={o.action === 'BUY' ? 'text-positive' : 'text-negative'}>{o.action}</span>
                      {' '}{o.ticker}{o.notional ? ` ~${money(o.notional)}` : ''} — {o.status}
                    </p>
                  ))}
                </div>
              </div>
            )}
          </div>

          {/* current positions */}
          <div className="bg-white rounded-xl border border-border p-6">
            <h3 className="font-bold text-text-primary mb-3">Current Paper Positions</h3>
            {plan.positions?.length > 0 ? (
              <table className="w-full text-sm">
                <thead>
                  <tr className="text-[10px] text-text-muted uppercase border-b border-border">
                    <th className="text-left py-2">Ticker</th>
                    <th className="text-right">Qty</th>
                    <th className="text-right">Market Value</th>
                    <th className="text-right">Unrealized P&L</th>
                  </tr>
                </thead>
                <tbody>
                  {plan.positions.map((p: any) => (
                    <tr key={p.ticker} className="border-b border-border/50">
                      <td className="py-2 font-mono font-bold">{p.ticker}</td>
                      <td className="text-right font-mono">{p.qty?.toFixed(2)}</td>
                      <td className="text-right font-mono">{money(p.market_value)}</td>
                      <td className={`text-right font-mono ${p.unrealized_pl >= 0 ? 'text-positive' : 'text-negative'}`}>
                        {p.unrealized_pl >= 0 ? '+' : ''}{money(p.unrealized_pl)} ({(p.unrealized_pl_pct ?? 0).toFixed(1)}%)
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            ) : (
              <p className="text-sm text-text-muted">No open positions — the account is all cash.</p>
            )}
          </div>
        </>
      )}

      {!plan && !loading && (
        <div className="bg-white rounded-xl border border-border p-12 text-center text-text-muted">
          Pick a strategy and click <b>Preview Plan</b> to see what would be bought and sold.
        </div>
      )}
    </div>
  );
};

export default LivePaper;
