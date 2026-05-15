import React, { useMemo } from 'react';
import {
  ComposedChart,
  Area,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ReferenceLine,
  Scatter,
  ResponsiveContainer
} from 'recharts';
import { useQuery } from '@tanstack/react-query';
import axios from 'axios';

interface StrategyComparisonChartProps {
  ticker: string;
  period: string;
}

const StrategyComparisonChart: React.FC<StrategyComparisonChartProps> = ({ ticker, period }) => {
  const { data, isLoading } = useQuery({
    queryKey: ['strategy-comparison', ticker, period],
    queryFn: async () => {
      const response = await axios.get(
        `${import.meta.env.VITE_MONITOR_URL}/strategy-comparison/${ticker}?period=${period}`
      );
      return response.data;
    },
    refetchInterval: 30000,
  });

  const combinedData = useMemo(() => {
    if (!data) return [];
    
    // Merge market_data and strategy_data by date
    return data.market_data.map((m: any, index: number) => {
      const s = data.strategy_data.find((sd: any) => sd.date === m.date) || {};
      return {
        ...m,
        strategy_value: s.strategy_value,
        signal_active: s.signal_active
      };
    });
  }, [data]);

  const entryPoints = useMemo(() => {
    if (!data) return [];
    return data.trade_markers
      .filter((t: any) => t.action === 'entry')
      .map((t: any) => {
        const point = combinedData.find((d: any) => d.date === t.date);
        return {
          ...t,
          x: t.date,
          y: point ? point.normalized_price : t.price
        };
      });
  }, [data, combinedData]);

  const exitPoints = useMemo(() => {
    if (!data) return [];
    return data.trade_markers
      .filter((t: any) => t.action === 'exit')
      .map((t: any) => {
        const point = combinedData.find((d: any) => d.date === t.date);
        return {
          ...t,
          x: t.date,
          y: point ? point.normalized_price : t.price
        };
      });
  }, [data, combinedData]);

  // Custom entry triangle marker (pointing up = entry, amber)
  const EntryMarker = (props: any) => {
    const { cx, cy } = props;
    if (cx === undefined || cy === undefined) return null;
    return (
      <polygon
        points={`${cx},${cy - 10} ${cx - 6},${cy + 2} ${cx + 6},${cy + 2}`}
        fill="#F5A623"
        stroke="#B85C00"
        strokeWidth="1"
      />
    );
  };

  // Custom exit triangle (pointing down = exit)
  const ExitMarker = (props: any) => {
    const { cx, cy } = props;
    if (cx === undefined || cy === undefined) return null;
    return (
      <polygon
        points={`${cx},${cy + 10} ${cx - 6},${cy - 2} ${cx + 6},${cy - 2}`}
        fill="#DC2626"
        stroke="#991B1B"
        strokeWidth="1"
      />
    );
  };

  // Custom crosshair tooltip
  const ComparisonTooltip = ({ active, payload, label }: any) => {
    if (!active || !payload?.length) return null;
    const market = payload.find((p: any) => p.dataKey === 'normalized_price');
    const strategy = payload.find((p: any) => p.dataKey === 'strategy_value');
    const diff = market && strategy
      ? (strategy.value - market.value).toFixed(2)
      : null;
      
    return (
      <div className="bg-white border border-indigo-200 rounded-lg shadow-lg p-3 text-sm">
        <p className="text-text-muted font-mono text-xs mb-1">{label}</p>
        {market && (
          <p className="text-cyan-600 font-mono">
            Market: {market.value >= 0 ? '+' : ''}{market.value.toFixed(2)}%
          </p>
        )}
        {strategy && (
          <p className="text-indigo-600 font-mono">
            Strategy: {strategy.value >= 0 ? '+' : ''}{strategy.value.toFixed(2)}%
          </p>
        )}
        {diff !== null && (
          <p className={`font-mono font-bold mt-1 ${
            Number(diff) >= 0 ? 'text-positive' : 'text-negative'
          }`}>
            Alpha: {Number(diff) >= 0 ? '+' : ''}{diff}%
          </p>
        )}
      </div>
    );
  };

  if (isLoading) return <div className="h-64 flex items-center justify-center text-text-muted">Loading chart data...</div>;

  return (
    <div className="w-full h-[500px]">
      <ResponsiveContainer width="100%" height="100%">
        <ComposedChart
          data={combinedData}
          margin={{ top: 10, right: 30, bottom: 10, left: 10 }}
        >
          <defs>
            <linearGradient id="colorMarket" x1="0" y1="0" x2="0" y2="1">
              <stop offset="5%" stopColor="#06B6D4" stopOpacity={0.3}/>
              <stop offset="95%" stopColor="#06B6D4" stopOpacity={0}/>
            </linearGradient>
            <linearGradient id="colorStrategy" x1="0" y1="0" x2="0" y2="1">
              <stop offset="5%" stopColor="#4F63D2" stopOpacity={0.2}/>
              <stop offset="95%" stopColor="#4F63D2" stopOpacity={0}/>
            </linearGradient>
          </defs>

          <CartesianGrid strokeDasharray="3 3" stroke="#F1F5F9" vertical={false} />

          <XAxis
            dataKey="date"
            tick={{ fontFamily: 'Inter, sans-serif', fontSize: 11, fill: '#64748B' }}
            axisLine={{ stroke: '#E2E8F0' }}
            tickLine={false}
            minTickGap={30}
          />
          <YAxis
            tickFormatter={(v) => `${v >= 0 ? '+' : ''}${v.toFixed(1)}%`}
            tick={{ fontFamily: 'Inter, sans-serif', fontSize: 11, fill: '#64748B' }}
            axisLine={false}
            tickLine={false}
            orientation="right"
          />

          <Tooltip content={<ComparisonTooltip />} />
          <ReferenceLine y={0} stroke="#CBD5E1" strokeDasharray="4 4" />

          {/* Market price area — cyan gradient */}
          <Area
            type="monotone"
            dataKey="normalized_price"
            stroke="#06B6D4"
            strokeWidth={2}
            fillOpacity={1}
            fill="url(#colorMarket)"
            name="Market Return"
            isAnimationActive={false}
          />

          {/* Strategy line — indigo area */}
          <Area
            type="monotone"
            dataKey="strategy_value"
            stroke="#4F63D2"
            strokeWidth={2.5}
            fillOpacity={1}
            fill="url(#colorStrategy)"
            name="Strategy Return"
            isAnimationActive={false}
          />

          <Legend
            verticalAlign="top"
            align="right"
            iconType="circle"
            formatter={(value) => (
              <span className="font-mono text-[11px] text-text-secondary">
                {value}
              </span>
            )}
          />
        </ComposedChart>
      </ResponsiveContainer>
    </div>
  );
};

export default StrategyComparisonChart;
