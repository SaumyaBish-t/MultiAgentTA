import asyncio
import uuid
import sys
from loguru import logger

# Add root to pythonpath if needed
sys.path.append(".")

from signal_generation.agents.strategy_coder_agent import StrategyCoderAgent

async def main():
    logger.info("Starting StrategyCoder test...")
    
    # Mock hypothesis matching what we might get from Phase 2
    mock_hypothesis = {
        "id": str(uuid.uuid4()),
        "ticker": "AAPL",
        "expected_direction": "long",
        "expected_timeframe": "swing",
        "hypothesis_type": "technical",
        "description": "AAPL has shown strong momentum breaking above the 50-day moving average on high volume."
    }
    
    coder = StrategyCoderAgent()
    result = await coder.generate(mock_hypothesis)
    
    logger.info("Test finished.")
    logger.info(f"Success: {result.get('success')}")
    logger.info(f"Strategy Type: {result.get('strategy_type')}")
    logger.info(f"Generated Parameters: {result.get('parameters')}")
    logger.info(f"Errors: {result.get('validation_errors')}")
    
    if result.get('success'):
        print("\n=== GENERATED CODE ===\n")
        print(result.get('current_code'))
        print("\n======================\n")
        
if __name__ == "__main__":
    asyncio.run(main())
