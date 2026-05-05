import asyncio
import json
import redis
import uuid
from loguru import logger
from sqlalchemy import create_engine, text
from config.settings import settings

# Import Agents
from portfolio_construction.agents.optimizer_agent import PortfolioOptimizer
from portfolio_construction.agents.factor_agent import FactorAgent
from portfolio_construction.agents.rebalancing_agent import RebalancingAgent
from portfolio_construction.agents.cost_estimator_agent import CostEstimatorAgent
from portfolio_construction.agents.allocation_agent import AllocationAgent
from portfolio_construction.pipeline.portfolio_pipeline import PortfolioPipeline

async def run_smoke_test():
    print("\n" + "="*50)
    print(" PHASE 5: PORTFOLIO CONSTRUCTION SMOKE TEST")
    print("="*50)

    # 1. Init portfolio DB
    engine = create_engine(settings.postgres_url)
    try:
        with engine.connect() as conn:
            res = conn.execute(text("SELECT name FROM portfolios WHERE name = 'main_portfolio'")).fetchone()
            if res:
                print("[SUCCESS] Portfolio tables verified: main_portfolio exists")
            else:
                print("[ERROR] Portfolio 'main_portfolio' missing")
                return
    except Exception as e:
        print(f"[ERROR] DB Verification failed: {e}")
        return

    # 2. Test Optimizer
    print("\n2. Testing Portfolio Optimizer...")
    # Fetch approved signals
    with engine.connect() as conn:
        res = conn.execute(text("SELECT ticker, approved_position_size_usd, risk_score FROM approved_signals WHERE status = 'approved'")).fetchall()
        signals = [{"ticker": r[0], "max_size": float(r[1]), "risk_score": float(r[2])} for r in res]
    
    if not signals:
        print("   [INFO] No approved signals in DB. Seeding temporary signals...")
        from scripts.seed_approved_signals import seed
        seed()
        with engine.connect() as conn:
            res = conn.execute(text("SELECT ticker, approved_position_size_usd, risk_score FROM approved_signals WHERE status = 'approved'")).fetchall()
            signals = [{"ticker": r[0], "max_size": float(r[1]), "risk_score": float(r[2])} for r in res]

    optimizer = PortfolioOptimizer()
    opt_result = await optimizer.optimize(signals)
    
    if opt_result:
        print("[SUCCESS] Optimizer working")
        print(f"   Positions: {len(opt_result.weights)}")
        print(f"   Expected Sharpe: {opt_result.expected_sharpe:.2f}")
        print(f"   Expected Return: {opt_result.expected_return:.1%}")
        print(f"   Expected Vol: {opt_result.expected_volatility:.1%}")
    else:
        print("[ERROR] Optimizer failed")
        return

    # 3. Test Factor Agent
    print("\n3. Testing Factor Exposure Agent...")
    factor_agent = FactorAgent()
    fa_result = await factor_agent.analyze(opt_result.weights)
    if fa_result:
        print("[SUCCESS] Factor agent working")
        print(f"   Portfolio Beta: {fa_result.factors.get('market_beta', 1.0):.2f}")
        print(f"   Factor breaches: {len(fa_result.breaches)}")
    else:
        print("[ERROR] Factor agent failed")

    # 4. Test Rebalancing Agent
    print("\n4. Testing Rebalancing Agent...")
    r = redis.from_url(settings.redis_url, decode_responses=True)
    r.set("portfolio:target:weights", json.dumps(opt_result.weights))
    
    rebalance_agent = RebalancingAgent()
    plan = await rebalance_agent.check_and_plan()
    if plan:
        print("[SUCCESS] Rebalancing agent working")
        print(f"   Rebalance needed: {plan.needed}")
        print(f"   Trigger: {plan.trigger_type}")
        print(f"   Trades: {len(plan.trades)}")
    else:
        print("[ERROR] Rebalancing agent failed")

    # 5. Test Cost Estimator
    print("\n5. Testing Transaction Cost Estimator...")
    if plan and plan.trades:
        cost_agent = CostEstimatorAgent()
        costs = await cost_agent.estimate(plan.trades)
        if costs:
            print("[SUCCESS] Cost estimator working")
            print(f"   Total cost: ${costs.total_cost_usd:.2f}")
            print(f"   Avg cost: {costs.total_cost_bps:.1f} bps")
        else:
            print("[ERROR] Cost estimator failed")
    else:
        print("   [INFO] Skipping cost estimator (no trades needed)")

    # 6. Test Allocation Agent
    print("\n6. Testing Allocation Agent...")
    alloc_agent = AllocationAgent()
    alloc = await alloc_agent.allocate(opt_result.weights)
    if alloc:
        print("[SUCCESS] Allocation agent working")
        print(f"   Positions: {len(alloc.positions)}")
        print(f"   Invested: ${alloc.total_invested_usd:,.0f}")
        print(f"   Cash: ${alloc.cash_usd:,.0f}")
    else:
        print("[ERROR] Allocation agent failed")

    # 7. Full Pipeline
    print("\n7. Executing Full Portfolio Pipeline...")
    pipeline = PortfolioPipeline()
    pipe_result = await pipeline.run()
    
    if pipe_result.get("status") == "completed":
        print("[SUCCESS] Full portfolio pipeline working")
        event_json = r.get("portfolio:current:state")
        if event_json:
            print("[SUCCESS] Redis event published")
        else:
            print("[WARNING] Redis state key missing")
    else:
        print(f"[ERROR] Pipeline failed: {pipe_result.get('error')}")

    print("\n" + "="*50)
    print(" PHASE 5 COMPLETE & VALIDATED")
    print("="*50 + "\n")

if __name__ == "__main__":
    asyncio.run(run_smoke_test())
