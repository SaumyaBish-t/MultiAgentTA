import React from 'react';
import { Link, useLocation } from 'react-router-dom';
import { 
  LayoutDashboard, 
  BarChart3, 
  ShieldAlert, 
  Zap, 
  History, 
  Search, 
  Bell, 
  Settings,
  Menu,
  X,
  Database,
  TrendingUp
} from 'lucide-react';

const Shell: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const location = useLocation();

  const navItems = [
    { name: 'Command Center', icon: LayoutDashboard, path: '/' },
    { name: 'Portfolio', icon: Database, path: '/portfolio' },
    { name: 'Strategy Lab', icon: Zap, path: '/strategy' },
    { name: 'Cross-Sectional', icon: TrendingUp, path: '/cross-sectional' },
    { name: 'Signal Intel', icon: BarChart3, path: '/signals' },
    { name: 'Risk Terminal', icon: ShieldAlert, path: '/risk' },
    { name: 'Audit Ledger', icon: History, path: '/audit' },
  ];

  const [isUSMarketOpen, setIsUSMarketOpen] = React.useState(false);
  const [isINMarketOpen, setIsINMarketOpen] = React.useState(false);
  const [usTime, setUsTime] = React.useState('');
  const [inTime, setInTime] = React.useState('');

  React.useEffect(() => {
    const checkMarket = () => {
      const now = new Date();
      
      // US Market Check (America/New_York)
      const formatterUS = new Intl.DateTimeFormat('en-US', {
        timeZone: 'America/New_York', hour: 'numeric', minute: 'numeric', hour12: false, weekday: 'short'
      });
      const partsUS = formatterUS.formatToParts(now);
      let hourUS = 0, minuteUS = 0, weekdayUS = '';
      partsUS.forEach(part => {
        if (part.type === 'hour') hourUS = parseInt(part.value);
        if (part.type === 'minute') minuteUS = parseInt(part.value);
        if (part.type === 'weekday') weekdayUS = part.value;
      });
      const isWeekendUS = weekdayUS === 'Sat' || weekdayUS === 'Sun';
      const isUSOpen = !isWeekendUS && ((hourUS === 9 && minuteUS >= 30) || (hourUS > 9 && hourUS < 16));
      const timeStrUS = `${hourUS.toString().padStart(2, '0')}:${minuteUS.toString().padStart(2, '0')}`;
      setIsUSMarketOpen(isUSOpen);
      setUsTime(timeStrUS);

      // Indian Market Check (Asia/Kolkata)
      const formatterIN = new Intl.DateTimeFormat('en-US', {
        timeZone: 'Asia/Kolkata', hour: 'numeric', minute: 'numeric', hour12: false, weekday: 'short'
      });
      const partsIN = formatterIN.formatToParts(now);
      let hourIN = 0, minuteIN = 0, weekdayIN = '';
      partsIN.forEach(part => {
        if (part.type === 'hour') hourIN = parseInt(part.value);
        if (part.type === 'minute') minuteIN = parseInt(part.value);
        if (part.type === 'weekday') weekdayIN = part.value;
      });
      const isWeekendIN = weekdayIN === 'Sat' || weekdayIN === 'Sun';
      const isINOpen = !isWeekendIN && ((hourIN === 9 && minuteIN >= 15) || (hourIN > 9 && hourIN < 15) || (hourIN === 15 && minuteIN <= 30));
      const timeStrIN = `${hourIN.toString().padStart(2, '0')}:${minuteIN.toString().padStart(2, '0')}`;
      setIsINMarketOpen(isINOpen);
      setInTime(timeStrIN);
    };

    checkMarket();
    const interval = setInterval(checkMarket, 60000);
    return () => clearInterval(interval);
  }, []);

  return (
    <div className="flex h-screen bg-page">
      {/* Sidebar */}
      <aside className="w-64 bg-sidebar text-white flex flex-col shadow-xl z-20">
        <div className="p-6 flex items-center space-x-3 border-b border-white/5">
          <div className="bg-cyan-500 p-2 rounded-lg">
             <Zap size={24} className="text-white" />
          </div>
          <div>
            <h1 className="text-xl font-black tracking-tighter leading-none">FORGE</h1>
            <p className="text-[10px] text-cyan-400 font-bold uppercase tracking-widest mt-1">Terminal v1.0</p>
          </div>
        </div>

        <nav className="flex-1 px-4 py-6 space-y-2 overflow-y-auto">
          {navItems.map((item) => {
            const isActive = location.pathname === item.path;
            return (
              <Link
                key={item.name}
                to={item.path}
                className={`flex items-center space-x-3 px-4 py-3 rounded-xl transition-all duration-200 group ${
                  isActive 
                  ? 'bg-indigo-600 text-white shadow-lg' 
                  : 'text-indigo-300 hover:bg-white/5 hover:text-white'
                }`}
              >
                <item.icon size={20} className={`${isActive ? 'text-white' : 'text-indigo-400 group-hover:text-cyan-400'}`} />
                <span className="font-semibold text-sm">{item.name}</span>
                {isActive && <div className="ml-auto w-1.5 h-1.5 bg-cyan-400 rounded-full animate-pulse"></div>}
              </Link>
            );
          })}
        </nav>

        <div className="p-4 mt-auto border-t border-white/5">
          <div className="bg-indigo-950/50 rounded-2xl p-4 border border-white/5">
            <div className="flex items-center justify-between mb-2">
              <span className="text-[10px] font-bold text-indigo-400 uppercase">Live Account</span>
              <div className="h-2 w-2 rounded-full bg-positive"></div>
            </div>
            <p className="text-xs font-bold text-white">Paper Trading Mode</p>
            <p className="text-[10px] text-indigo-300 mt-1">API Status: Connected</p>
          </div>
        </div>
      </aside>

      {/* Main Content */}
      <main className="flex-1 flex flex-col min-w-0 overflow-hidden">
        {/* Header */}
        <header className="h-16 bg-white border-b border-border flex items-center justify-between px-8 z-10">
          <div className="flex items-center flex-1 max-w-xl">
             <div className="relative w-full">
                <Search className="absolute left-3 top-1/2 -translate-y-1/2 text-text-muted" size={16} />
                <input 
                  type="text" 
                  placeholder="Global Terminal Search (Alt+K)..." 
                  className="w-full pl-10 pr-4 py-2 bg-page border-none rounded-xl text-sm focus:ring-2 focus:ring-indigo-500/20 outline-none"
                />
             </div>
          </div>

            <div className="flex items-center space-x-6 ml-6">
            <div className="flex items-center space-x-3">
              {/* US Market Indicator */}
              <div className={`flex items-center space-x-2 px-3 py-1.5 rounded-full border ${isUSMarketOpen ? 'bg-indigo-50 border-indigo-100' : 'bg-gray-50 border-gray-200'}`}>
                 <span className={`text-[10px] font-bold ${isUSMarketOpen ? 'text-indigo-600' : 'text-gray-500'}`}>🇺🇸 US:</span>
                 <span className={`text-[10px] font-bold uppercase ${isUSMarketOpen ? 'text-positive' : 'text-text-muted'}`}>
                   {isUSMarketOpen ? `[● OPEN ${usTime} EST]` : '[● CLOSED]'}
                 </span>
              </div>
              
              {/* Indian Market Indicator */}
              <div className={`flex items-center space-x-2 px-3 py-1.5 rounded-full border ${isINMarketOpen ? 'bg-indigo-50 border-indigo-100' : 'bg-gray-50 border-gray-200'}`}>
                 <span className={`text-[10px] font-bold ${isINMarketOpen ? 'text-indigo-600' : 'text-gray-500'}`}>🇮🇳 IN:</span>
                 <span className={`text-[10px] font-bold uppercase ${isINMarketOpen ? 'text-positive' : 'text-text-muted'}`}>
                   {isINMarketOpen ? `[● OPEN ${inTime} IST]` : '[● CLOSED]'}
                 </span>
              </div>
            </div>

            <button className="relative p-2 text-text-secondary hover:bg-page rounded-xl transition-colors">
              <Bell size={20} />
              <div className="absolute top-2 right-2 h-2 w-2 bg-negative rounded-full border-2 border-white"></div>
            </button>

            <button className="p-2 text-text-secondary hover:bg-page rounded-xl transition-colors">
              <Settings size={20} />
            </button>

            <div className="flex items-center space-x-3 pl-6 border-l border-border">
               <div className="text-right">
                  <p className="text-xs font-bold text-text-primary">Saumya Bisht</p>
                  <p className="text-[10px] text-text-muted">Senior Quant</p>
               </div>
               <div className="h-10 w-10 bg-indigo-600 rounded-xl flex items-center justify-center text-white font-black shadow-lg">
                  SB
               </div>
            </div>
          </div>
        </header>

        {/* Scrollable Area */}
        <div className="flex-1 overflow-y-auto p-8">
          {children}
        </div>
      </main>
    </div>
  );
};

export default Shell;
