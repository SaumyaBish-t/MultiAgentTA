import React from 'react';
import { useQuery } from '@tanstack/react-query';
import axios from 'axios';

const fetchPortfolio = async () => {
  const { data } = await axios.get(`${import.meta.env.VITE_MONITOR_URL}/portfolio/full`);
  return data;
};

const Portfolio: React.FC = () => {
  const { data, isLoading, isError } = useQuery({
    queryKey: ['portfolioFull'],
    queryFn: fetchPortfolio,
    refetchInterval: 10000,
  });

  if (isLoading) return <div className="p-6 text-text-muted">Loading portfolio data...</div>;
  if (isError) return <div className="p-6 text-red-500">Failed to load portfolio.</div>;

  const { account, positions, order_blotter, performance_metrics } = data;

  return (
    <div className="p-6 space-y-6">
      <header className="flex justify-between items-center">
        <div>
          <h1 className="text-2xl font-bold tracking-tight">Portfolio Management</h1>
          <p className="text-text-muted">Live view of account equity and positions</p>
        </div>
        {account?.is_paper_trading && (
          <div className="px-3 py-1 bg-yellow-500/20 text-yellow-500 text-sm font-medium rounded-full border border-yellow-500/30">
            PAPER TRADING
          </div>
        )}
      </header>

      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <div className="panel flex flex-col justify-center">
          <p className="text-sm text-text-muted">Total Equity</p>
          <p className="text-3xl font-mono mt-1">${account?.total_equity?.toLocaleString(undefined, {minimumFractionDigits: 2})}</p>
        </div>
        <div className="panel flex flex-col justify-center">
          <p className="text-sm text-text-muted">Daily Return</p>
          <p className={`text-2xl font-mono mt-1 ${account?.daily_return_pct >= 0 ? 'text-green-500' : 'text-red-500'}`}>
            {account?.daily_return_pct > 0 ? '+' : ''}{(account?.daily_return_pct * 100)?.toFixed(2)}%
          </p>
        </div>
        <div className="panel flex flex-col justify-center">
          <p className="text-sm text-text-muted">Available Cash</p>
          <p className="text-2xl font-mono mt-1">${account?.available_cash?.toLocaleString(undefined, {minimumFractionDigits: 2})}</p>
        </div>
        <div className="panel flex flex-col justify-center">
          <p className="text-sm text-text-muted">Unrealized PnL</p>
          <p className={`text-2xl font-mono mt-1 ${account?.unrealized_pnl >= 0 ? 'text-green-500' : 'text-red-500'}`}>
            {account?.unrealized_pnl > 0 ? '+' : ''}${account?.unrealized_pnl?.toLocaleString(undefined, {minimumFractionDigits: 2})}
          </p>
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        <div className="lg:col-span-2 space-y-6">
          <div className="panel">
            <h2 className="text-lg font-semibold mb-4 border-b border-border-color pb-2">Active Positions</h2>
            <div className="overflow-x-auto">
              <table className="w-full text-sm text-left">
                <thead className="text-xs text-text-muted uppercase bg-background-dark">
                  <tr>
                    <th className="px-4 py-3">Ticker</th>
                    <th className="px-4 py-3">Qty</th>
                    <th className="px-4 py-3">Entry</th>
                    <th className="px-4 py-3">Current</th>
                    <th className="px-4 py-3">Value</th>
                    <th className="px-4 py-3">PnL</th>
                  </tr>
                </thead>
                <tbody>
                  {positions?.length > 0 ? positions.map((p: any, i: number) => (
                    <tr key={i} className="border-b border-border-color hover:bg-background-dark/50 transition-colors">
                      <td className="px-4 py-3 font-medium">{p.ticker}</td>
                      <td className="px-4 py-3">{p.quantity}</td>
                      <td className="px-4 py-3 font-mono">${p.avg_entry_price?.toFixed(2)}</td>
                      <td className="px-4 py-3 font-mono">${p.current_price?.toFixed(2)}</td>
                      <td className="px-4 py-3 font-mono">${p.market_value?.toLocaleString(undefined, {minimumFractionDigits: 2})}</td>
                      <td className={`px-4 py-3 font-mono ${p.unrealized_pnl >= 0 ? 'text-green-500' : 'text-red-500'}`}>
                        {p.unrealized_pnl > 0 ? '+' : ''}{p.unrealized_pnl?.toFixed(2)} ({p.unrealized_pnl_pct > 0 ? '+' : ''}{(p.unrealized_pnl_pct * 100)?.toFixed(2)}%)
                      </td>
                    </tr>
                  )) : (
                    <tr>
                      <td colSpan={6} className="px-4 py-8 text-center text-text-muted">No active positions.</td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          </div>
          
          <div className="panel">
            <h2 className="text-lg font-semibold mb-4 border-b border-border-color pb-2">Order Blotter</h2>
            <div className="overflow-x-auto">
              <table className="w-full text-sm text-left">
                <thead className="text-xs text-text-muted uppercase bg-background-dark">
                  <tr>
                    <th className="px-4 py-3">Time</th>
                    <th className="px-4 py-3">Ticker</th>
                    <th className="px-4 py-3">Action</th>
                    <th className="px-4 py-3">Qty</th>
                    <th className="px-4 py-3">Price</th>
                  </tr>
                </thead>
                <tbody>
                  {order_blotter?.map((o: any, i: number) => (
                    <tr key={i} className="border-b border-border-color hover:bg-background-dark/50 transition-colors">
                      <td className="px-4 py-3 text-text-muted whitespace-nowrap">{new Date(o.filled_at).toLocaleTimeString()}</td>
                      <td className="px-4 py-3 font-medium">{o.ticker}</td>
                      <td className={`px-4 py-3 font-medium ${o.action.toLowerCase() === 'buy' ? 'text-blue-500' : 'text-purple-500'}`}>
                        {o.action.toUpperCase()}
                      </td>
                      <td className="px-4 py-3">{o.quantity}</td>
                      <td className="px-4 py-3 font-mono">${o.filled_price?.toFixed(2)}</td>
                    </tr>
                  ))}
                  {(!order_blotter || order_blotter.length === 0) && (
                    <tr>
                      <td colSpan={5} className="px-4 py-8 text-center text-text-muted">No recent orders.</td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          </div>
        </div>

        <div className="space-y-6">
          <div className="panel">
            <h2 className="text-lg font-semibold mb-4 border-b border-border-color pb-2">Metrics (30d)</h2>
            <div className="space-y-4">
              <div className="flex justify-between">
                <span className="text-text-muted">Sharpe Ratio</span>
                <span className="font-mono">{performance_metrics?.sharpe_30d?.toFixed(2)}</span>
              </div>
              <div className="flex justify-between">
                <span className="text-text-muted">Sortino Ratio</span>
                <span className="font-mono">{performance_metrics?.sortino_30d?.toFixed(2)}</span>
              </div>
              <div className="flex justify-between">
                <span className="text-text-muted">Win Rate</span>
                <span className="font-mono">{(performance_metrics?.win_rate * 100)?.toFixed(1)}%</span>
              </div>
              <div className="flex justify-between">
                <span className="text-text-muted">Max Drawdown</span>
                <span className="font-mono text-red-500">{(performance_metrics?.max_drawdown * 100)?.toFixed(1)}%</span>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default Portfolio;
