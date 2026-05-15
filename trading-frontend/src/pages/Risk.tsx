import React from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import axios from 'axios';

const fetchRisk = async () => {
  const { data } = await axios.get(`${import.meta.env.VITE_MONITOR_URL}/risk/full`);
  return data;
};

const Risk: React.FC = () => {
  const queryClient = useQueryClient();
  const { data, isLoading, isError } = useQuery({
    queryKey: ['riskFull'],
    queryFn: fetchRisk,
    refetchInterval: 5000,
  });

  const toggleKillSwitch = useMutation({
    mutationFn: async (action: 'halt' | 'resume') => {
      const { data } = await axios.post(`${import.meta.env.VITE_MONITOR_URL}/risk/kill-switch`, { action, reason: 'Dashboard manual override' });
      return data;
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['riskFull'] });
    }
  });

  if (isLoading) return <div className="p-6 text-text-muted">Loading risk data...</div>;
  if (isError) return <div className="p-6 text-red-500">Failed to load risk data.</div>;

  const { portfolio_var, drawdown, circuit_breakers, kill_switch_status } = data;

  return (
    <div className="p-6 space-y-6">
      <header className="flex justify-between items-center">
        <div>
          <h1 className="text-2xl font-bold tracking-tight">Risk Terminal</h1>
          <p className="text-text-muted">Live VaR, Drawdown, and Circuit Breakers</p>
        </div>
        <div>
          <button 
            onClick={() => toggleKillSwitch.mutate(kill_switch_status?.trading_halted ? 'resume' : 'halt')}
            className={`px-4 py-2 font-bold rounded shadow-lg transition-colors ${
              kill_switch_status?.trading_halted 
                ? 'bg-yellow-500 hover:bg-yellow-600 text-black' 
                : 'bg-red-600 hover:bg-red-700 text-white'
            }`}
          >
            {toggleKillSwitch.isPending ? 'PROCESSING...' : kill_switch_status?.trading_halted ? 'RESUME TRADING' : 'KILL SWITCH (HALT)'}
          </button>
        </div>
      </header>

      {kill_switch_status?.trading_halted && (
        <div className="p-4 bg-red-500/20 border border-red-500/50 rounded flex items-center gap-3">
          <svg className="w-6 h-6 text-red-500" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
          </svg>
          <div className="text-red-200">
            <strong>SYSTEM HALTED.</strong> All automated trading is currently suspended. Manual intervention required.
          </div>
        </div>
      )}

      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        <div className="panel flex flex-col items-center justify-center py-8">
          <h3 className="text-text-muted mb-2">Value at Risk (95% 1D)</h3>
          <p className="text-4xl font-mono text-orange-500">${portfolio_var?.var_95_1day_usd?.toLocaleString(undefined, {minimumFractionDigits: 2})}</p>
          <p className="text-sm text-text-muted mt-2">{(portfolio_var?.var_as_pct_of_portfolio * 100)?.toFixed(2)}% of Portfolio</p>
        </div>
        <div className="panel flex flex-col items-center justify-center py-8">
          <h3 className="text-text-muted mb-2">Current Drawdown</h3>
          <p className={`text-4xl font-mono ${drawdown?.current_drawdown_pct < -0.05 ? 'text-red-500' : 'text-yellow-500'}`}>
            {(drawdown?.current_drawdown_pct * 100)?.toFixed(2)}%
          </p>
          <p className="text-sm text-text-muted mt-2">Max DD: {(drawdown?.max_drawdown_pct * 100)?.toFixed(2)}%</p>
        </div>
        <div className="panel flex flex-col items-center justify-center py-8">
          <h3 className="text-text-muted mb-2">Conditional VaR (95%)</h3>
          <p className="text-4xl font-mono text-red-500">${portfolio_var?.cvar_95_1day_usd?.toLocaleString(undefined, {minimumFractionDigits: 2})}</p>
          <p className="text-sm text-text-muted mt-2">{portfolio_var?.method}</p>
        </div>
      </div>

      <div className="panel">
        <h2 className="text-lg font-semibold border-b border-border-color pb-2 mb-4">Circuit Breakers</h2>
        <div className="overflow-x-auto">
          <table className="w-full text-sm text-left">
            <thead className="text-xs text-text-muted uppercase bg-background-dark">
              <tr>
                <th className="px-4 py-3">Breaker Type</th>
                <th className="px-4 py-3">Threshold</th>
                <th className="px-4 py-3">Current Value</th>
                <th className="px-4 py-3">Action</th>
                <th className="px-4 py-3">Status</th>
              </tr>
            </thead>
            <tbody>
              {circuit_breakers?.map((cb: any, i: number) => (
                <tr key={i} className="border-b border-border-color hover:bg-background-dark/50">
                  <td className="px-4 py-3 font-medium uppercase">{cb.breaker_type.replace('_', ' ')}</td>
                  <td className="px-4 py-3 font-mono">{cb.threshold}</td>
                  <td className="px-4 py-3 font-mono">{cb.current_value?.toFixed(4)}</td>
                  <td className="px-4 py-3 text-text-muted">{cb.action}</td>
                  <td className="px-4 py-3">
                    <span className={`px-2 py-1 text-xs rounded-full ${
                      cb.triggered ? 'bg-red-500/20 text-red-500' : 'bg-green-500/20 text-green-500'
                    }`}>
                      {cb.triggered ? 'TRIGGERED' : 'SAFE'}
                    </span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
};

export default Risk;
