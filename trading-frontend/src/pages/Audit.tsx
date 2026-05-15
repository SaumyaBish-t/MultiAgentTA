import React from 'react';
import { useQuery } from '@tanstack/react-query';
import axios from 'axios';
import { useSSE } from '../hooks/useSSE';

const fetchAudit = async () => {
  const { data } = await axios.get(`${import.meta.env.VITE_MONITOR_URL}/audit/full`);
  return data;
};

const Audit: React.FC = () => {
  const { data, isLoading, isError } = useQuery({
    queryKey: ['auditFull'],
    queryFn: fetchAudit,
    refetchInterval: 10000,
  });

  // Example SSE usage (backend currently just sends ping, but this wires it up)
  const { isConnected } = useSSE(`${import.meta.env.VITE_MONITOR_URL}/audit/stream`);

  if (isLoading) return <div className="p-6 text-text-muted">Loading audit ledger...</div>;
  if (isError) return <div className="p-6 text-red-500">Failed to load audit ledger.</div>;

  const { event_stream, chain_integrity, llm_usage, infrastructure_health } = data;

  return (
    <div className="p-6 space-y-6">
      <header className="flex justify-between items-center">
        <div>
          <h1 className="text-2xl font-bold tracking-tight">Audit Ledger</h1>
          <p className="text-text-muted">Immutable system events and resource utilization</p>
        </div>
        <div className="flex items-center gap-2">
          <div className={`w-2 h-2 rounded-full ${isConnected ? 'bg-green-500' : 'bg-red-500'}`}></div>
          <span className="text-xs text-text-muted">{isConnected ? 'LIVE STREAM' : 'DISCONNECTED'}</span>
        </div>
      </header>

      <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-6">
        <div className="panel flex flex-col justify-center">
          <p className="text-sm text-text-muted">Total Events</p>
          <p className="text-2xl font-mono mt-1">{chain_integrity?.total_events?.toLocaleString()}</p>
        </div>
        <div className="panel flex flex-col justify-center">
          <p className="text-sm text-text-muted">Integrity Check</p>
          <p className="text-2xl font-mono mt-1 text-green-500">{(chain_integrity?.integrity_pct * 100).toFixed(0)}% VERIFIED</p>
        </div>
        <div className="panel flex flex-col justify-center md:col-span-2">
          <p className="text-sm text-text-muted">Today's LLM Token Usage</p>
          <div className="flex gap-4 mt-2">
            <div>
              <p className="text-xs text-text-muted">Groq</p>
              <p className="font-mono">{llm_usage?.today?.groq?.tokens?.toLocaleString() || 0}</p>
            </div>
            <div>
              <p className="text-xs text-text-muted">Cerebras</p>
              <p className="font-mono">{llm_usage?.today?.cerebras?.tokens?.toLocaleString() || 0}</p>
            </div>
            <div>
              <p className="text-xs text-text-muted">OpenRouter</p>
              <p className="font-mono">{llm_usage?.today?.openrouter?.tokens?.toLocaleString() || 0}</p>
            </div>
          </div>
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        <div className="lg:col-span-2 space-y-6">
          <div className="panel">
            <h2 className="text-lg font-semibold border-b border-border-color pb-2 mb-4">Event Stream</h2>
            <div className="space-y-3 max-h-[600px] overflow-y-auto pr-2">
              {event_stream?.map((e: any, i: number) => (
                <div key={i} className="p-3 border-l-2 border-accent-blue bg-background-dark/30 flex flex-col gap-1">
                  <div className="flex justify-between items-center">
                    <div className="flex items-center gap-2">
                      <span className="font-semibold text-sm">{e.event_type?.toUpperCase()}</span>
                      <span className="text-xs text-text-muted px-2 py-0.5 bg-background-light rounded">{e.action}</span>
                    </div>
                    <span className="text-xs text-text-muted font-mono">{new Date(e.timestamp).toLocaleTimeString()}</span>
                  </div>
                  <div className="text-sm text-text-muted">
                    <span className="text-accent-blue font-mono mr-2">[{e.actor}]</span>
                    {e.ticker && <span className="font-bold text-white mr-2">{e.ticker}</span>}
                    {JSON.stringify(e.details)}
                  </div>
                  <div className="flex justify-between items-center mt-1">
                    <span className="text-[10px] text-text-muted font-mono bg-background-dark px-1 rounded truncate max-w-[200px]">
                      hash: {e.immutable_hash}
                    </span>
                    {e.hash_verified && <span className="text-[10px] text-green-500">VERIFIED</span>}
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>

        <div className="space-y-6">
          <div className="panel">
            <h2 className="text-lg font-semibold border-b border-border-color pb-2 mb-4">Infrastructure Health</h2>
            <div className="space-y-3">
              {Object.entries(infrastructure_health || {}).map(([service, info]: [string, any], i) => (
                <div key={i} className="flex justify-between items-center p-2 bg-background-dark/30 rounded">
                  <span className="text-sm font-medium">{service}</span>
                  <div className="flex items-center gap-3">
                    <span className="font-mono text-xs text-text-muted">{info.latency_ms}ms</span>
                    <div className={`w-2 h-2 rounded-full ${info.status === 'healthy' ? 'bg-green-500' : 'bg-red-500'}`}></div>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default Audit;
