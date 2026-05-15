import React, { useState, useEffect } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import axios from 'axios';
import { 
  Search, 
  Info, 
  Cpu, 
  Target, 
  Zap, 
  ArrowUpRight, 
  ArrowDownRight,
  RefreshCw,
  Play
} from 'lucide-react';
import { TradingViewChart } from '../components/charts/TradingViewChart';

const StrategyComparison: React.FC = () => {
  const queryClient = useQueryClient();
  const [selectedTicker, setSelectedTicker] = useState('AAPL');
  const [period, setPeriod] = useState('3m');
  const [timeframe, setTimeframe] = useState('1d');

  // Pipeline state
  const [pipelineRunId, setPipelineRunId] = useState<string | null>(null);
  const [pipelineProgress, setPipelineProgress] = useState(0);
  const [pipelineStage, setPipelineStage] = useState('');
  const [pipelineRunning, setPipelineRunning] = useState(false);
  const [showPipelinePrompt, setShowPipelinePrompt] = useState(false);

  const { data: tickers } = useQuery({
    queryKey: ['available-tickers'],
    queryFn: () => axios.get(`${import.meta.env.VITE_MONITOR_URL}/strategy-comparison/tickers/available`).then(res => res.data),
  });

  const { data: comparison, isLoading } = useQuery({
    queryKey: ['strategy-comparison', selectedTicker, period, timeframe],
    queryFn: () => axios.get(`${import.meta.env.VITE_MONITOR_URL}/strategy-comparison/${selectedTicker}?period=${period}&timeframe=${timeframe}`).then(res => res.data),
  });

  // Fetch live price every 10 seconds for header display
  const { data: livePrice } = useQuery({
    queryKey: ['live-price', selectedTicker],
    queryFn: () => axios.get(`${import.meta.env.VITE_MONITOR_URL}/strategy-comparison/${selectedTicker}/live-price`).then(res => res.data),
    refetchInterval: 10000
  });

  // Derive displayed price: prefer livePrice, fallback to comparison
  const displayPrice = livePrice?.price ?? comparison?.current_price;
  const displayChange = livePrice?.change ?? comparison?.price_change ?? 0;
  const displayChangePct = livePrice?.change_pct ?? comparison?.price_change_pct ?? 0;
  const lastTickTime = livePrice?.timestamp ? new Date(livePrice.timestamp).toLocaleTimeString() : new Date().toLocaleTimeString();

  const handleTickerChange = async (newTicker: string) => {
    setSelectedTicker(newTicker);
    try {
        const res = await axios.get(`${import.meta.env.VITE_MONITOR_URL}/pipeline/status/${newTicker}`);
        if (!res.data.has_strategy) {
            setShowPipelinePrompt(true);
        } else {
            setShowPipelinePrompt(false);
        }
    } catch(e) {
        console.error("Failed to fetch pipeline status", e)
    }
  };

  const handleRunPipeline = async () => {
    setPipelineRunning(true);
    setPipelineProgress(0);
    setShowPipelinePrompt(false);

    try {
        const res = await axios.post(`${import.meta.env.VITE_MONITOR_URL}/pipeline/run-full-cycle`, {
            ticker: selectedTicker,
            force_refresh: true
        });
        
        const { run_id, skip_pipeline } = res.data;

        if (skip_pipeline) {
            setPipelineRunning(false);
            return;
        }

        const sse = new EventSource(`${import.meta.env.VITE_MONITOR_URL}/realtime/stream/pipeline/${run_id}`);
        setPipelineRunId(run_id);

        sse.onmessage = (event) => {
            const data = JSON.parse(event.data);
            setPipelineProgress(data.progress || 0);
            setPipelineStage(data.message || '');

            if (data.completed || data.failed) {
                setPipelineRunning(false);
                sse.close();
                queryClient.invalidateQueries({queryKey: ['strategy-comparison', selectedTicker]});
            }
        };
        
        sse.onerror = () => {
            setPipelineRunning(false);
            sse.close();
        }
    } catch (e) {
        console.error("Failed to start pipeline", e);
        setPipelineRunning(false);
    }
  };

  const stats = comparison?.performance_comparison;

  return (
    <div className="space-y-6">
      {/* Search & Selector Bar */}
      <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
        <div className="relative flex-1 max-w-md">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 text-text-muted" size={18} />
          <select 
            value={selectedTicker}
            onChange={(e) => handleTickerChange(e.target.value)}
            className="w-full pl-10 pr-4 py-2 bg-white border border-border rounded-xl shadow-sm appearance-none focus:outline-none focus:ring-2 focus:ring-indigo-500 font-bold"
          >
            {tickers?.tickers.map((t: any) => (
              <option key={t.symbol} value={t.symbol}>{t.symbol} — {t.company_name}</option>
            ))}
          </select>
        </div>

        <div className="flex gap-2">
            <div className="flex bg-white p-1 rounded-xl shadow-sm border border-border overflow-x-auto">
            {['1min', '5m', '30m', '1h', '6h', '1d', '1w'].map(tf => (
                <button 
                key={tf} 
                onClick={() => setTimeframe(tf)}
                className={`px-3 py-1.5 text-[10px] sm:text-xs font-bold rounded-lg transition-all whitespace-nowrap ${timeframe === tf ? 'bg-indigo-600 text-white shadow-md' : 'text-text-muted hover:bg-indigo-50'}`}
                >
                {tf.toUpperCase()}
                </button>
            ))}
            </div>

            <div className="flex bg-white p-1 rounded-xl shadow-sm border border-border overflow-x-auto">
            {['1d', '1w', '1m', '3m', '1y'].map(p => (
                <button 
                key={p} 
                onClick={() => setPeriod(p)}
                className={`px-3 py-1.5 text-[10px] sm:text-xs font-bold rounded-lg transition-all whitespace-nowrap ${period === p ? 'bg-slate-800 text-white shadow-md' : 'text-text-muted hover:bg-slate-50'}`}
                >
                {p.toUpperCase()}
                </button>
            ))}
            </div>
        </div>
      </div>

      <div className="space-y-6">
        {/* Main Content Area */}
        <div className="space-y-6 relative">
          
          {/* AI Pipeline Loading Overlay */}
          {pipelineRunning && (
            <div className="absolute inset-0 bg-white/95 rounded-xl z-20 flex flex-col items-center justify-center gap-4 shadow-lg border border-indigo-100">
                <div className="w-16 h-16 relative">
                <div className="w-16 h-16 border-4 border-indigo-200 border-t-indigo-600 rounded-full animate-spin"/>
                </div>
                <div className="text-center">
                <p className="text-lg font-bold text-indigo-900">AI Research Pipeline Running</p>
                <p className="text-sm text-gray-500 mt-1">{pipelineStage}</p>
                </div>
                <div className="w-64 bg-gray-100 rounded-full h-3">
                <div className="bg-indigo-600 h-3 rounded-full transition-all duration-500" style={{width: `${pipelineProgress}%`}} />
                </div>
                <p className="text-xs font-mono text-gray-400">{pipelineProgress}%</p>
                <div className="grid grid-cols-4 gap-1 mt-2 w-64">
                {['Data','Research','Strategy','Backtest'].map((stage, i) => (
                    <div key={stage} className={`text-xs text-center p-1 rounded ${pipelineProgress > i * 25 ? 'bg-indigo-50 text-indigo-700 font-medium' : 'text-gray-400'}`}>
                    {stage}
                    </div>
                ))}
                </div>
            </div>
          )}

          {/* Chart Section */}
          <div className="panel p-0 overflow-hidden">
            <div className="p-5 border-b border-border flex justify-between items-end">
              <div>
                <h2 className="text-2xl font-bold flex items-center space-x-2">
                  <span>{comparison?.company_name ?? selectedTicker}</span>
                  <span className="text-text-muted font-mono font-normal">({selectedTicker})</span>
                  {comparison?.market && (
                      <span className="text-xs px-2 py-0.5 bg-slate-100 text-slate-600 rounded-full font-bold ml-2">
                          {comparison.market === 'US' ? '🇺🇸 US' : '🇮🇳 IN'}
                      </span>
                  )}
                </h2>
                <div className="flex items-center space-x-4 mt-1">
                  <div className="flex items-baseline space-x-1">
                    <span className="text-lg font-mono font-bold">
                      ${displayPrice?.toLocaleString(undefined, {minimumFractionDigits: 2}) ?? '...'}
                    </span>
                    <span className={`text-xs font-bold ${displayChange >= 0 ? 'text-positive' : 'text-negative'}`}>
                      {displayChange >= 0 ? '+' : ''}{displayChange?.toFixed(2)} 
                      ({displayChangePct >= 0 ? '+' : ''}{displayChangePct?.toFixed(2)}%)
                    </span>
                  </div>
                  <span className="text-[10px] text-text-muted uppercase font-bold tracking-widest bg-emerald-50 text-emerald-600 px-2 py-0.5 rounded animate-pulse">
                    ● LIVE
                  </span>
                </div>
              </div>
              <div className="flex flex-col items-end">
                 {showPipelinePrompt || comparison?.strategy_needs_generation ? (
                    <button onClick={handleRunPipeline} className="flex items-center space-x-2 px-4 py-2 bg-indigo-600 text-white text-xs font-bold rounded-xl hover:bg-indigo-700 shadow-md transition-colors animate-pulse">
                        <Play size={14} />
                        <span>Initiate AI Research</span>
                    </button>
                 ) : (
                    <button onClick={handleRunPipeline} className="flex items-center space-x-2 px-4 py-2 bg-white text-indigo-600 border border-indigo-200 text-xs font-bold rounded-xl hover:bg-indigo-50 shadow-sm transition-colors">
                        <RefreshCw size={14} />
                        <span>Force Re-Run Pipeline</span>
                    </button>
                 )}
                <span className="text-[10px] text-text-muted mt-2">Last tick: {lastTickTime}</span>
              </div>
            </div>
            
            <div className="p-0 border-b border-border">
               <TradingViewChart ticker={selectedTicker} period={period} timeframe={timeframe} data={comparison} />
            </div>
          </div>

        </div>

        {/* Bottom Section: Hypothesis, Execution Logic, Stats */}
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          <div className="panel bg-indigo-900 text-white border-none">
            <div className="flex items-center space-x-2 mb-4">
              <Cpu className="text-cyan-400" size={20} />
              <span className="label-caps text-cyan-400">Current AI Hypothesis</span>
            </div>
            {comparison?.active_strategy ? (
              <>
                <div className="flex items-center space-x-2 mb-2">
                  <span className="text-lg font-bold">Strategy Active</span>
                </div>
                <p className="text-sm text-indigo-200 leading-relaxed">
                  Generated by AI pipeline. Signals are active on the chart.
                </p>
              </>
            ) : (
              <p className="text-sm text-indigo-300">No active hypothesis found for this ticker. Run a discovery job to generate insights.</p>
            )}
          </div>

          <div className="panel">
            <div className="flex items-center space-x-2 mb-4">
              <Target className="text-indigo-600" size={20} />
              <span className="label-caps">Execution Logic</span>
            </div>
            {comparison?.active_strategy ? (
              <div className="space-y-4">
                <div>
                  <p className="text-sm font-bold text-text-primary">{comparison.active_strategy.strategy_name}</p>
                  <p className="text-[11px] text-text-muted">Type: {comparison.active_strategy.strategy_type}</p>
                </div>
              </div>
            ) : (
              <p className="text-sm text-text-muted">No active live strategy. Signal currently in observation phase.</p>
            )}
          </div>

          <div className="panel space-y-6">
            <span className="label-caps border-b border-border pb-2 block">Performance Stats</span>
            <div className="space-y-4">
              <div>
                <p className="text-[10px] text-text-muted uppercase font-bold mb-1">Strategy Return</p>
                <div className="flex items-baseline space-x-2">
                  <span className={`text-2xl font-bold font-mono ${(stats?.strategy_return_pct ?? 0) >= 0 ? 'text-positive' : 'text-negative'}`}>
                    {stats?.strategy_return_pct >= 0 ? '+' : ''}{stats?.strategy_return_pct?.toFixed(2)}%
                  </span>
                </div>
              </div>
            </div>
          </div>

        </div>
      </div>
    </div>
  );
};

export default StrategyComparison;
