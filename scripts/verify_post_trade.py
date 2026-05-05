import asyncio
import uuid
from unittest.mock import patch, MagicMock
from execution.agents.post_trade_agent import PostTradeAnalyzer

async def test_post_trade_analysis():
    print("Testing Post-Trade Analysis Agent...")
    analyzer = PostTradeAnalyzer()
    
    batch_id = uuid.uuid4()
    
    # Mock data for the 'fetch' node
    mock_filled_orders = [
        {
            "id": uuid.uuid4(),
            "ticker": "AAPL",
            "action": "buy",
            "filled_shares": 100,
            "filled_avg_price": 195.50,
            "arrival_price": 195.10,
            "slippage_bps": 20.5
        },
        {
            "id": uuid.uuid4(),
            "ticker": "MSFT",
            "action": "buy",
            "filled_shares": 50,
            "filled_avg_price": 420.10,
            "arrival_price": 420.00,
            "slippage_bps": 2.3
        }
    ]
    
    print(f"\nRunning analysis for Batch {batch_id}...")
    initial_state = {
        "batch_id": batch_id,
        "filled_orders": mock_filled_orders,
        "execution_metrics": {},
        "slippage_analysis": {},
        "timing_analysis": {},
        "quality_score": 0.0,
        "learnings": [],
        "recommendations": [],
        "error": None
    }
    final_state = await analyzer.app.ainvoke(initial_state)
    
    print(f"\nAnalysis Result:")
    print(f"  Quality Score: {final_state['quality_score']:.2f}")
    print(f"  Avg Slippage:  {final_state['slippage_analysis']['avg_slippage_bps']:.1f} bps")
    print(f"  Recommendations:")
    for rec in final_state["recommendations"]:
        print(f"    - {rec}")

if __name__ == "__main__":
    asyncio.run(test_post_trade_analysis())
