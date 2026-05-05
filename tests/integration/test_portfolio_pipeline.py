import pytest
import json
import redis
from portfolio_construction.pipeline.portfolio_pipeline import PortfolioPipeline
from config.settings import settings

@pytest.mark.asyncio
async def test_full_pipeline_integration():
    pipeline = PortfolioPipeline()
    result = await pipeline.run()
    
    # Assert pipeline completed
    assert result["status"] == "completed"
    assert "run_id" in result
    
    # Check Redis for event
    r = redis.from_url(settings.redis_url, decode_responses=True)
    # Note: listener might have cleared or we check the published event
    # For integration test, we can check if the current state key exists
    data = r.get("portfolio:current:state")
    assert data is not None
    
    state = json.loads(data)
    assert "value" in state
    assert "positions" in state
