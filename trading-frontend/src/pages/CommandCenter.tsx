import React, { useState, useEffect } from 'react';
import { useQuery } from '@tanstack/react-query';
import axios from 'axios';
import { 
  TrendingUp, 
  TrendingDown, 
  AlertTriangle, 
  Activity, 
  ShieldCheck, 
  Globe, 
  BarChart3, 
  LayoutDashboard 
} from 'lucide-react';
import { 
  AreaChart, 
  Area, 
  XAxis, 
  YAxis, 
  CartesianGrid, 
  Tooltip, 
  ResponsiveContainer 
} from 'recharts';
import { PaperTradingStatus } from '../components/PaperTradingStatus';

const CommandCenter: React.FC = () => {
  const [liveValue, setLiveValue] = useState<number | null>(null);
  const [wsStatus, setWsStatus] = useState<'connecting' | 'open' | 'closed'>('connecting');

  // Fetch status and portfolio data
  const { data: status } = useQuery({
    queryKey: ['system-status'],
    queryFn: () => axios.get(`${import.meta.env.VITE_MONITOR_URL}/status`).then(res => res.data),
    refetchInterval: 10000
  });

  const { data: portfolio } = useQuery({
    queryKey: ['portfolio'],
    queryFn: () => axios.get(`${import.meta.env.VITE_MONITOR_URL}/portfolio`).then(res => res.data),
  });

  const { data: history } = useQuery({
    queryKey: ['portfolio-history'],
    queryFn: () => axios.get(`${import.meta.env.VITE_MONITOR_URL}/performance?period=30d`).then(res => res.data),
  });

  // WebSocket for live updates
  useEffect(() => {
    const ws = new WebSocket(import.meta.env.VITE_WS_URL);
    
    ws.onopen = () => setWsStatus('open');
    ws.onclose = () => setWsStatus('closed');
    ws.onmessage = (event) => {
      const data = JSON.parse(event.data);
      if (data.portfolio_value) {
        setLiveValue(data.portfolio_value);
      }
    };

    return () => ws.close();
  }, []);

  const displayValue = liveValue ?? status?.portfolio_value ?? 100000.00;
  const pnlPct = status?.daily_pnl_pct ?? 0.0;
  const isPositive = pnlPct >= 0;

  return (
    <div className="space-y-6">
      <PaperTradingStatus />
      {/* Header Stat Bar */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <div className="panel flex flex-col justify-between">
          <span className="label-caps">Total Portfolio Value</span>
          <div className="flex items-baseline space-x-2 mt-2">
            <span className="text-2xl font-bold font-mono tracking-tight">
              ${displayValue.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
            </span>
            <div className={`flex items-center text-xs font-bold ${isPositive ? 'text-positive' : 'text-negative'}`}>
              {isPositive ? <TrendingUp size={12} className="mr-1" /> : <TrendingDown size={12} className="mr-1" />}
              {Math.abs(pnlPct).toFixed(2)}%
            </div>
          </div>
          <div className="mt-2 h-1 w-full bg-indigo-50 rounded-full overflow-hidden">
             <div className="h-full bg-cyan-500" style={{ width: '65%' }}></div>
          </div>
        </div>

        <div className="panel flex flex-col justify-between">
          <span className="label-caps">Max Drawdown</span>
          <div className="flex items-baseline space-x-2 mt-2">
            <span className="text-2xl font-bold font-mono tracking-tight text-negative">
              {status?.drawdown ? (status.drawdown * 100).toFixed(2) : '0.00'}%
            </span>
          </div>
          <span className="text-[10px] text-text-muted mt-1 italic">Peak: $142,500.00</span>
        </div>

        <div className="panel flex flex-col justify-between">
          <span className="label-caps">Active Signals</span>
          <div className="flex items-baseline space-x-2 mt-2">
            <span className="text-2xl font-bold font-mono tracking-tight text-indigo-600">
              {status?.alert_count ?? 0}
            </span>
            <span className="text-xs text-text-muted">Validated</span>
          </div>
          <div className="flex space-x-1 mt-2">
            {[1, 2, 3, 4, 5].map(i => (
              <div key={i} className={`h-1.5 w-full rounded-full ${i <= 4 ? 'bg-indigo-500' : 'bg-indigo-100'}`}></div>
            ))}
          </div>
        </div>

        <div className="panel flex flex-col justify-between border-l-4 border-l-positive">
          <span className="label-caps">Market Regime</span>
          <div className="flex items-center space-x-2 mt-2">
            <Activity size={18} className="text-positive" />
            <span className="text-xl font-bold uppercase tracking-wide text-positive">
              {status?.regime ?? 'BULLISH'}
            </span>
          </div>
          <span className="text-[10px] text-text-muted mt-1 font-medium">Confidence: 78% | VIX: 14.2</span>
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Main Chart */}
        <div className="lg:col-span-2 panel h-[450px] flex flex-col">
          <div className="flex justify-between items-center mb-6">
            <div>
              <h3 className="text-lg font-bold">Portfolio Performance</h3>
              <p className="text-xs text-text-muted">Equity curve vs S&P 500 Benchmark</p>
            </div>
            <div className="flex bg-indigo-50 p-1 rounded-lg">
              {['1D', '1W', '1M', '3M', 'YTD'].map(p => (
                <button key={p} className={`px-3 py-1 text-[10px] font-bold rounded-md ${p === '1M' ? 'bg-white shadow-sm text-indigo-600' : 'text-text-muted'}`}>
                  {p}
                </button>
              ))}
            </div>
          </div>
          <div className="flex-1 -ml-4">
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={mockChartData}>
                <defs>
                  <linearGradient id="colorValue" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="#06B6D4" stopOpacity={0.1}/>
                    <stop offset="95%" stopColor="#06B6D4" stopOpacity={0}/>
                  </linearGradient>
                </defs>
                <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#F1F5F9" />
                <XAxis 
                  dataKey="name" 
                  tick={{ fontSize: 10, fill: '#94A3B8' }} 
                  axisLine={false} 
                  tickLine={false} 
                />
                <YAxis 
                  hide 
                  domain={['dataMin - 1000', 'dataMax + 1000']} 
                />
                <Tooltip 
                  contentStyle={{ borderRadius: '8px', border: '1px solid #E2E8F0', boxShadow: '0 4px 6px -1px rgb(0 0 0 / 0.1)' }}
                />
                <Area 
                  type="monotone" 
                  dataKey="value" 
                  stroke="#06B6D4" 
                  strokeWidth={3}
                  fillOpacity={1} 
                  fill="url(#colorValue)" 
                />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        </div>

        {/* Side Panels */}
        <div className="space-y-6">
          {/* System Health */}
          <div className="panel">
            <div className="flex justify-between items-center mb-4">
              <span className="label-caps">Pipeline Status</span>
              <div className="flex items-center space-x-1">
                <div className={`h-2 w-2 rounded-full ${wsStatus === 'open' ? 'bg-positive animate-pulse' : 'bg-negative'}`}></div>
                <span className="text-[10px] font-bold text-text-muted uppercase">Live Feed</span>
              </div>
            </div>
            <div className="space-y-3">
              {status?.phases && Object.entries(status.phases).map(([phase, state]: [string, any]) => (
                <div key={phase} className="flex justify-between items-center text-sm">
                  <div className="flex items-center space-x-2">
                    <div className={`h-2 w-2 rounded-full ${state === 'healthy' ? 'bg-positive' : 'bg-warning'}`}></div>
                    <span className="capitalize text-text-secondary">{phase.replace('phase', 'Phase ')}</span>
                  </div>
                  <span className="text-[10px] font-mono text-text-muted uppercase">Syncing</span>
                </div>
              ))}
            </div>
          </div>

          {/* Recent Alerts */}
          <div className="panel flex-1">
             <div className="flex justify-between items-center mb-4">
              <span className="label-caps">Active Alerts</span>
              <span className="text-[10px] text-indigo-600 font-bold cursor-pointer">View All</span>
            </div>
            <div className="space-y-4">
              <div className="flex space-x-3 p-2 bg-negative-bg rounded-lg border border-negative/10">
                <AlertTriangle size={16} className="text-negative mt-0.5" />
                <div>
                  <p className="text-xs font-bold text-negative">Risk Limit Breach</p>
                  <p className="text-[10px] text-text-secondary">NVDA position exceeds 15% allocation limit.</p>
                </div>
              </div>
              <div className="flex space-x-3 p-2 bg-warning-bg rounded-lg border border-warning/10">
                <Activity size={16} className="text-warning mt-0.5" />
                <div>
                  <p className="text-xs font-bold text-warning">Signal Decay Warning</p>
                  <p className="text-[10px] text-text-secondary">AAPL Mean Reversion alpha dropping below threshold.</p>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

const mockChartData = [
  { name: '09:30', value: 128400 },
  { name: '10:00', value: 129100 },
  { name: '10:30', value: 128700 },
  { name: '11:00', value: 130200 },
  { name: '11:30', value: 131500 },
  { name: '12:00', value: 130800 },
  { name: '12:30', value: 132400 },
  { name: '13:00', value: 134100 },
  { name: '13:30', value: 133500 },
  { name: '14:00', value: 134560 },
];

export default CommandCenter;
