import asyncio
import uuid
from unittest.mock import patch, MagicMock
from execution.pipeline.execution_pipeline import ExecutionPipeline
from execution.agents.order_generation_agent import OrderBatch
from execution.agents.smart_order_router_agent import RoutingResult
from execution.agents.post_trade_agent import PostTradeResult

async def test_execution_pipeline():
    print("Testing Full Execution Pipeline...")
    pipeline = ExecutionPipeline()
    
    rebalance_plan = {
        "rebalance_id": str(uuid.uuid4()),
        "trades": [{"ticker": "AAPL", "action": "buy", "shares": 10, "value": 2000}]
    }
    
    # Mocking internal agent calls for a fast smoke test
    with patch("execution.agents.order_generation_agent.OrderGeneratorAgent.generate_from_plan") as mock_gen, \
         patch("execution.agents.smart_order_router_agent.SmartOrderRouter.route") as mock_route, \
         patch("execution.agents.execution_monitor_agent.ExecutionMonitorAgent.monitor_until_complete") as mock_mon, \
         patch("execution.agents.post_trade_agent.PostTradeAnalyzer.analyze") as mock_ana, \
         patch("execution.brokers.alpaca_adapter.AlpacaBrokerAdapter.get_account") as mock_acc, \
         patch("execution.brokers.alpaca_adapter.AlpacaBrokerAdapter.get_positions") as mock_pos:
         
        # Setup mocks
        mock_gen.return_value = OrderBatch(
            batch_id=uuid.uuid4(), batch_type="rebalance", orders=[], 
            total_buy_value=2000, total_sell_value=0, 
            execution_strategy="immediate", estimated_completion_time="Immediate"
        )
        mock_route.return_value = RoutingResult(
            batch_id=uuid.uuid4(), submitted=1, failed=0, 
            submitted_orders=[{"ticker": "AAPL", "broker_order_id": "b-1"}], 
            failed_orders=[], total_value_submitted=2000.0
        )
        mock_mon.return_value = None
        mock_ana.return_value = PostTradeResult(
            batch_id=uuid.uuid4(), quality_score=0.95, avg_slippage_bps=1.2, recommendations=[]
        )
        mock_acc.return_value = {"portfolio_value": 100000.0, "cash": 50000.0, "buying_power": 100000.0}
        mock_pos.return_value = []

        print("\nStarting pipeline.run()...")
        result = await pipeline.run(rebalance_plan)
        
        print(f"\nPipeline Result:")
        print(f"  Run ID:    {result.run_id}")
        print(f"  Submitted: {result.orders_submitted}")
        print(f"  Quality:   {result.execution_quality_score:.2f}")
        print(f"  Duration:  {result.duration_seconds:.1f}s")
        print(f"  Status:    SUCCESS")

if __name__ == "__main__":
    asyncio.run(test_execution_pipeline())
