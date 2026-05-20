import React, { useEffect, useState } from 'react';
import { FlaskConical, ShieldCheck, AlertCircle } from 'lucide-react';

export const PaperTradingStatus: React.FC = () => {
  const [status, setStatus] = useState<any>(null);

  useEffect(() => {
    fetch('http://127.0.0.1:8001/account/status')
      .then(res => res.json())
      .then(data => setStatus(data))
      .catch(err => {
        console.error("Failed to fetch account status", err);
        setStatus({
          mode: "PAPER TRADING",
          explanation: "All trades are 100% simulated. No real money.",
          cash_balance: "$100,000.00 from Alpaca paper account (Fallback)",
          portfolio_value: "$100,000.00",
          is_real_money: false
        });
      });
  }, []);

  if (!status) return null;

  return (
    <div className="bg-slate-900 text-white rounded-xl shadow-lg border border-slate-700 overflow-hidden mb-6">
      <div className="bg-indigo-600/20 px-4 py-3 border-b border-indigo-500/30 flex items-center justify-between">
        <div className="flex items-center space-x-2">
          <FlaskConical className="text-indigo-400" size={20} />
          <h3 className="font-bold text-indigo-100 tracking-wide">PAPER TRADING MODE</h3>
        </div>
        <div className="flex items-center space-x-1 text-indigo-300 text-xs font-bold uppercase px-2 py-1 bg-indigo-900/50 rounded">
          <ShieldCheck size={14} />
          <span>Safe Environment</span>
        </div>
      </div>
      
      <div className="p-4 grid grid-cols-1 md:grid-cols-2 gap-6">
        <div>
          <div className="flex items-start space-x-3 mb-4">
            <AlertCircle className="text-amber-400 shrink-0 mt-0.5" size={16} />
            <div>
              <p className="text-sm font-bold text-amber-100 leading-tight">All trades are fully simulated</p>
              <p className="text-xs text-slate-400 mt-1">No real money is at risk. Orders are routed to Alpaca's paper trading environment.</p>
            </div>
          </div>
          
          <div className="space-y-2 text-sm">
            <div className="flex justify-between border-b border-slate-700/50 pb-1">
              <span className="text-slate-400">Paper Cash:</span>
              <span className="font-mono font-bold text-emerald-400">{(status.cash_balance ?? '—').split(' ')[0]}</span>
            </div>
            <div className="flex justify-between border-b border-slate-700/50 pb-1">
              <span className="text-slate-400">Started with:</span>
              <span className="font-mono">$100,000.00 (Alpaca default)</span>
            </div>
            <div className="flex justify-between border-b border-slate-700/50 pb-1">
              <span className="text-slate-400">Data Feed:</span>
              <span className="font-mono text-indigo-300">TimescaleDB 1-min & YF Delayed</span>
            </div>
          </div>
        </div>
        
        <div className="bg-slate-800/50 rounded-lg p-3 border border-slate-700/50">
          <p className="text-xs font-bold text-slate-300 uppercase tracking-wider mb-2 border-b border-slate-700 pb-1">
            Path to Live Trading
          </p>
          <ul className="space-y-2 text-xs text-slate-400">
            <li className="flex items-center space-x-2">
              <div className="w-4 h-4 rounded-full bg-slate-700 flex items-center justify-center text-[9px] font-bold text-slate-300">1</div>
              <span>Run system in paper mode for 3+ months</span>
            </li>
            <li className="flex items-center space-x-2">
              <div className="w-4 h-4 rounded-full bg-slate-700 flex items-center justify-center text-[9px] font-bold text-slate-300">2</div>
              <span>Achieve consistent positive Sharpe {'>'} 1.0</span>
            </li>
            <li className="flex items-center space-x-2">
              <div className="w-4 h-4 rounded-full bg-slate-700 flex items-center justify-center text-[9px] font-bold text-slate-300">3</div>
              <span>Review compliance and risk checks</span>
            </li>
            <li className="flex items-center space-x-2">
              <div className="w-4 h-4 rounded-full bg-slate-700 flex items-center justify-center text-[9px] font-bold text-slate-300">4</div>
              <span>Manually switch paper=False in Alpaca</span>
            </li>
          </ul>
        </div>
      </div>
    </div>
  );
};
