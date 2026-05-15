import React from 'react';
import { useQuery } from '@tanstack/react-query';
import axios from 'axios';

const fetchSignals = async () => {
  const { data } = await axios.get(`${import.meta.env.VITE_MONITOR_URL}/signals/full`);
  return data;
};

const Signals: React.FC = () => {
  const { data, isLoading, isError } = useQuery({
    queryKey: ['signalsFull'],
    queryFn: fetchSignals,
    refetchInterval: 15000,
  });

  if (isLoading) return <div className="p-6 text-text-muted">Loading signals data...</div>;
  if (isError) return <div className="p-6 text-red-500">Failed to load signals.</div>;

  const { live_feed, signal_registry } = data;

  return (
    <div className="p-6 space-y-6">
      <header>
        <h1 className="text-2xl font-bold tracking-tight">Signal Intelligence</h1>
        <p className="text-text-muted">Active hypotheses, approved signals, and decay tracking</p>
      </header>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <div className="panel space-y-4">
          <h2 className="text-lg font-semibold border-b border-border-color pb-2">Live Hypotheses Feed</h2>
          <div className="space-y-3 max-h-[600px] overflow-y-auto pr-2">
            {live_feed?.map((h: any, i: number) => (
              <div key={i} className="p-4 rounded border border-border-color bg-background-dark/30 hover:bg-background-dark/50 transition-colors">
                <div className="flex justify-between items-start mb-2">
                  <div className="flex items-center gap-2">
                    <span className="font-bold text-lg">{h.ticker}</span>
                    <span className={`px-2 py-0.5 text-xs font-semibold rounded ${h.direction === 'long' ? 'bg-green-500/20 text-green-500' : 'bg-red-500/20 text-red-500'}`}>
                      {h.direction?.toUpperCase()}
                    </span>
                  </div>
                  <span className="text-xs text-text-muted">{new Date(h.created_at).toLocaleString()}</span>
                </div>
                <p className="text-sm text-text-muted mb-3 line-clamp-2">{h.hypothesis_text}</p>
                <div className="flex items-center justify-between text-xs">
                  <div className="flex items-center gap-2">
                    <span className="text-text-muted">Conviction:</span>
                    <div className="w-24 bg-background-dark rounded-full h-2">
                      <div className={`h-2 rounded-full ${h.conviction_score > 0.7 ? 'bg-green-500' : h.conviction_score > 0.4 ? 'bg-yellow-500' : 'bg-red-500'}`} style={{ width: `${h.conviction_score * 100}%` }}></div>
                    </div>
                    <span className="font-mono">{(h.conviction_score * 100).toFixed(0)}%</span>
                  </div>
                  <span className={`px-2 py-1 rounded border ${h.status === 'approved' ? 'bg-green-500/10 border-green-500/30 text-green-500' : 'bg-yellow-500/10 border-yellow-500/30 text-yellow-500'}`}>
                    {h.status?.toUpperCase()}
                  </span>
                </div>
              </div>
            ))}
            {(!live_feed || live_feed.length === 0) && (
              <div className="text-center text-text-muted py-8">No active hypotheses</div>
            )}
          </div>
        </div>

        <div className="space-y-6">
          <div className="panel">
            <h2 className="text-lg font-semibold border-b border-border-color pb-2 mb-4">Signal Registry & Decay</h2>
            <div className="overflow-x-auto">
              <table className="w-full text-sm text-left">
                <thead className="text-xs text-text-muted uppercase bg-background-dark">
                  <tr>
                    <th className="px-4 py-3">Ticker</th>
                    <th className="px-4 py-3">Type</th>
                    <th className="px-4 py-3">Hit Rate</th>
                    <th className="px-4 py-3">Status</th>
                  </tr>
                </thead>
                <tbody>
                  {signal_registry?.map((s: any, i: number) => (
                    <tr key={i} className="border-b border-border-color hover:bg-background-dark/50">
                      <td className="px-4 py-3 font-medium">{s.ticker}</td>
                      <td className="px-4 py-3 text-text-muted">{s.strategy_type}</td>
                      <td className="px-4 py-3 font-mono">{(s.hit_rate * 100).toFixed(1)}%</td>
                      <td className="px-4 py-3">
                        <span className={`px-2 py-1 text-xs rounded-full ${
                          s.decay_status === 'healthy' ? 'bg-green-500/20 text-green-500' : 
                          s.decay_status === 'warning' ? 'bg-yellow-500/20 text-yellow-500' : 
                          'bg-red-500/20 text-red-500'
                        }`}>
                          {s.decay_status?.toUpperCase()}
                        </span>
                      </td>
                    </tr>
                  ))}
                  {(!signal_registry || signal_registry.length === 0) && (
                    <tr><td colSpan={4} className="text-center py-4 text-text-muted">No approved signals</td></tr>
                  )}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default Signals;
