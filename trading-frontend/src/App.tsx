import React from 'react';
import { BrowserRouter as Router, Routes, Route, Navigate } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';

import Shell from './components/layout/Shell';
import CommandCenter from './pages/CommandCenter';
import StrategyComparison from './pages/StrategyComparison';

import Portfolio from './pages/Portfolio';
import Signals from './pages/Signals';
import Risk from './pages/Risk';
import Audit from './pages/Audit';
import CrossSectional from './pages/CrossSectional';
import LivePaper from './pages/LivePaper';

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: 1,
      refetchOnWindowFocus: false,
    },
  },
});

function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <Router>
        <Shell>
          <Routes>
            <Route path="/" element={<CommandCenter />} />
            <Route path="/strategy" element={<StrategyComparison />} />
            <Route path="/cross-sectional" element={<CrossSectional />} />
            <Route path="/live-paper" element={<LivePaper />} />
            <Route path="/portfolio" element={<Portfolio />} />
            <Route path="/signals" element={<Signals />} />
            <Route path="/risk" element={<Risk />} />
            <Route path="/audit" element={<Audit />} />
            <Route path="*" element={<Navigate to="/" replace />} />
          </Routes>
        </Shell>
      </Router>
    </QueryClientProvider>
  );
}

export default App;
