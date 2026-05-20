import React, { useState } from 'react';
import axios from 'axios';
import {
  LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid,
} from 'recharts';
import { TrendingUp, Play, Settings2, Loader2 } from 'lucide-react';

const MONITOR = import.meta.env.VITE_MONITOR_URL;

const gradeColor = (g: string): string =>
  ({
    A: 'bg-emerald-100 text-emerald-700 border-emerald-200',
    B: 'bg-indigo-100 text-indigo-700 border-indigo-200',
    C: 'bg-amber-100 text-amber-700 border-amber-200',
    D: 'bg-red-100 text-red-700 border-red-200',
  }[g] || 'bg-gray-100 text-gray-700 border-gray-200');

const Metric: React.FC<{ label: string; value: string; tone?: 'pos' | 'neg' }> = ({
  label, value, tone,
}) => (
  <div className="bg-page rounded-xl p-4">
    <p className="text-[10px] font-bold text-text-muted uppercase tracking-wider">{label}</p>
    <p
      className={`text-xl font-mono font-bold mt-1 ${
        tone === 'pos' ? 'text-positive' : tone === 'neg' ? 'text-negative' : 'text-text-primary'
      }`}
    >
      {value}
    </p>
  </div>
);

const ResultPanel: React.FC<{ title: string; data: any }> = ({ title, data }) => {
  const m = data.metrics || {};
  return (
    <div className="bg-white rounded-xl border border-border p-6 mb-6">
      <div className="flex items-center justify-between mb-4">
        <h3 className="font-bold text-text-primary">{title}</h3>
        <div className="flex items-center space-x-2">
          <span className={`px-3 py-1 rounded-lg border text-sm font-black ${gradeColor(data.grade)}`}>
            GRADE {data.grade}
          </span>
          <span
            className={`px-3 py-1 rounded-lg text-xs font-bold text-white ${
              data.passed ? 'bg-emerald-600' : 'bg-red-600'
            }`}
          >
            {data.passed ? 'PASSES' : 'REJECTED'}
          </span>
        </div>
      </div>

      <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mb-5">
        <Metric label="Total Return" value={`${(m.total_return_pct || 0).toFixed(2)}%`}
          tone={(m.total_return_pct || 0) >= 0 ? 'pos' : 'neg'} />
        <Metric label="Benchmark" value={`${(m.benchmark_return_pct || 0).toFixed(2)}%`} />
        <Metric label="Sharpe Ratio" value={(m.sharpe_ratio || 0).toFixed(2)}
          tone={(m.sharpe_ratio || 0) >= 1 ? 'pos' : undefined} />
        <Metric label="Max Drawdown" value={`${(m.max_drawdown_pct || 0).toFixed(2)}%`} tone="neg" />
        <Metric label="Win Rate" value={`${((m.win_rate || 0) * 100).toFixed(0)}%`} />
        <Metric label="Trades" value={`${m.total_trades || 0}`} />
        <Metric label="Profit Factor" value={(m.profit_factor || 0).toFixed(2)} />
        <Metric label="Quality Score" value={`${data.quality_score || 0}/100`} />
      </div>

      {data.final_holdings?.length > 0 && (
        <div className="mb-5">
          <p className="text-[10px] font-bold text-text-muted uppercase tracking-wider mb-2">
            Current Holdings
          </p>
          <div className="flex flex-wrap gap-2">
            {data.final_holdings.map((t: string) => (
              <span key={t} className="px-3 py-1 bg-indigo-50 text-indigo-700 rounded-lg text-xs font-bold font-mono">
                {t}
              </span>
            ))}
          </div>
        </div>
      )}

      {data.equity_curve?.length > 0 && (
        <div className="h-64">
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={data.equity_curve}>
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
    </div>
  );
};

const CrossSectional: React.FC = () => {
  const [market, setMarket] = useState('us');
  const [loading, setLoading] = useState<string | null>(null);
  const [runResult, setRunResult] = useState<any>(null);
  const [optResult, setOptResult] = useState<any>(null);
  const [error, setError] = useState<string | null>(null);

  const runBacktest = async () => {
    setLoading('run'); setError(null); setOptResult(null);
    try {
      const { data } = await axios.get(`${MONITOR}/cross-sectional/run`, { params: { market } });
      if (data.error) setError(data.error);
      else setRunResult(data);
    } catch (e: any) {
      setError(e?.message || 'request failed');
    } finally {
      setLoading(null);
    }
  };

  const runOptimize = async () => {
    setLoading('opt'); setError(null); setRunResult(null);
    try {
      const { data } = await axios.get(`${MONITOR}/cross-sectional/optimize`, { params: { market } });
      if (data.error) setError(data.error);
      else setOptResult(data);
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
          <TrendingUp className="text-indigo-600" />
          <span>Cross-Sectional Momentum</span>
        </h1>
        <p className="text-sm text-text-muted mt-1">
          Ranks the whole universe by 12-1 momentum and holds an equal-weight basket of the
          strongest names, rebalanced monthly — where systematic equity edge actually lives.
        </p>
      </div>

      <div className="bg-white rounded-xl border border-border p-4 mb-6 flex flex-wrap items-center gap-3">
        <span className="text-xs font-bold text-text-muted uppercase">Universe</span>
        <select
          value={market}
          onChange={(e) => setMarket(e.target.value)}
          className="px-3 py-2 bg-page rounded-lg text-sm font-semibold outline-none"
        >
          <option value="us">US</option>
          <option value="in">India (NSE)</option>
          <option value="all">All markets</option>
        </select>
        <button
          onClick={runBacktest}
          disabled={!!loading}
          className="flex items-center space-x-2 px-4 py-2 bg-indigo-600 text-white text-sm font-bold rounded-xl hover:bg-indigo-700 disabled:opacity-50"
        >
          {loading === 'run' ? <Loader2 size={16} className="animate-spin" /> : <Play size={16} />}
          <span>Run Backtest</span>
        </button>
        <button
          onClick={runOptimize}
          disabled={!!loading}
          className="flex items-center space-x-2 px-4 py-2 bg-amber-500 text-white text-sm font-bold rounded-xl hover:bg-amber-600 disabled:opacity-50"
        >
          {loading === 'opt' ? <Loader2 size={16} className="animate-spin" /> : <Settings2 size={16} />}
          <span>Optimize (walk-forward)</span>
        </button>
        {loading === 'opt' && (
          <span className="text-xs text-text-muted">grid-searching 36 combos — this takes ~30–60s…</span>
        )}
      </div>

      {error && (
        <div className="bg-red-50 border border-red-200 text-red-700 rounded-xl p-4 mb-6 text-sm">
          {error}
        </div>
      )}

      {runResult && <ResultPanel title="Backtest Result" data={runResult} />}

      {optResult && (
        <>
          <div className="bg-white rounded-xl border border-border p-6 mb-6">
            <h3 className="font-bold text-text-primary mb-2">Walk-Forward Optimisation</h3>
            <p className="text-xs text-text-muted mb-3">
              Grid: {optResult.grid_size} combos · train/test split @ {optResult.split_date} ·
              the TEST panel below is out-of-sample — the honest result.
            </p>
            <div className="flex flex-wrap gap-2 mb-3">
              {Object.entries(optResult.best_params).map(([k, v]: any) => (
                <span key={k} className="px-3 py-1 bg-indigo-50 text-indigo-700 rounded-lg text-xs font-bold font-mono">
                  {k}={v}
                </span>
              ))}
            </div>
            <p className="text-xs text-text-muted">
              Train (in-sample, optimistic): return {optResult.train.return_pct.toFixed(2)}% ·
              sharpe {optResult.train.sharpe.toFixed(2)} · grade {optResult.train.grade}
            </p>
          </div>
          <ResultPanel title="TEST — Out-of-Sample (the honest result)" data={optResult.test} />
        </>
      )}

      {!runResult && !optResult && !loading && (
        <div className="bg-white rounded-xl border border-border p-12 text-center text-text-muted">
          Pick a universe, then run a backtest — or optimise to grid-search the best parameters.
        </div>
      )}
    </div>
  );
};

export default CrossSectional;
