=== TABLE CONTENTS ===
✅ portfolio_positions: 7 rows — Portfolio page - active positions
✅ orders: 1 rows — Portfolio page - order blotter
⚠️  EMPTY executions: 0 rows — Portfolio page - fill details
✅ execution_performance: 5 rows — Portfolio page - slippage
✅ portfolio_performance: 9 rows — Portfolio page - equity curve
✅ pnl_attribution: 2 rows — Portfolio page - PnL breakdown
✅ portfolios: 1 rows — Portfolio page - account summary
✅ trading_signals: 44 rows — Signals page - signal feed
✅ research_hypotheses: 27 rows — Signals page - hypotheses
✅ sentiment_scores: 27 rows — Signals page - agent consensus
✅ technical_signals: 10 rows — Signals page - technical agent
✅ fundamental_scores: 9 rows — Signals page - fundamental agent
✅ macro_signals: 40 rows — Signals page - macro agent
✅ backtest_results: 17 rows — Signals page - backtest data
✅ walk_forward_results: 12 rows — Signals page - WF validation
✅ portfolio_risk_snapshots: 20 rows — Risk page - VaR/snapshots
✅ var_calculations: 40 rows — Risk page - VaR per position
✅ correlation_matrix_snapshots: 12 rows — Risk page - heatmap
✅ circuit_breakers: 4 rows — Risk page - kill switch status
✅ risk_events: 31 rows — Risk page - breach history
✅ audit_log: 136 rows — Audit page - event stream
✅ compliance_rules: 12 rows — Audit page - rules
✅ rule_violations: 68 rows — Audit page - violations
✅ daily_reports: 36 rows — Audit page - reports
✅ approved_signals: 8 rows — Cross-page - approved signals
✅ rebalance_events: 7 rows — Cross-page - rebalancing
✅ signal_generation_runs: 1 rows — Cross-page - pipeline runs
✅ research_runs: 18 rows — Cross-page - research runs

=== REDIS KEYS ===
✅ portfolio:current:state: b'{"value": 100000.0, "positions": [{"ticker": "AAPL", "shar
✅ portfolio:drawdown:current: b'0.021'
✅ portfolio:peak:value: b'100000.0'
❌ MISSING portfolio:alert:level: null
❌ MISSING risk:var:portfolio:current: null
❌ MISSING risk:trading:halted: null
❌ MISSING monitoring:regime:current: null
❌ MISSING signals:rankings:current: null
❌ MISSING monitoring:pnl:latest: null
✅ monitoring:health:latest: b'{"overall": "healthy", "phases": {"phase1": {"status": "he

=== ALPACA PAPER ACCOUNT ===
✅ Paper cash:     $  100,000.00
✅ Portfolio val:  $  100,000.00
✅ Buying power:   $  200,000.00
✅ Open positions: 0
✅ Open orders:    0
   Trading blocked: False

=== API STATUS ===
✅ :8000/health
✅ :8001/status
❌ 404 :8001/portfolio/full
❌ 404 :8001/signals/full
❌ 404 :8001/risk/full
❌ 404 :8001/audit/full
❌ 404 :8001/compliance/status
✅ :8001/performance?period=30d (EMPTY DATA)
✅ :8001/health/detailed
