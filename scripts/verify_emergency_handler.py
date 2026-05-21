import asyncio
import json
import redis
import uuid
from execution.agents.emergency_handler import EmergencyHandler

async def test_emergency_handler():
    print("Testing Emergency Execution Handler...")
    handler = EmergencyHandler()
    r = redis.from_url("redis://localhost:16379", decode_responses=True)
    
    # Start the listener in the background
    handler.start_listener()
    await asyncio.sleep(1) # Wait for listener to start
    
    # 1. Trigger Force Close via Redis
    print("\nSimulating Force Close event for AAPL...")
    r.publish("risk.position.force_close", json.dumps({
        "ticker": "AAPL",
        "reason": "SMOKE_TEST_FORCE_CLOSE"
    }))
    
    await asyncio.sleep(2) # Wait for handler to pick it up
    
    # 2. Trigger Deleveraging (Reduce All) via Redis
    print("\nSimulating Deleveraging event (50%)...")
    r.publish("risk.circuit_breaker.reduce", json.dumps({
        "factor": 0.5,
        "reason": "SMOKE_TEST_REDUCE"
    }))
    
    await asyncio.sleep(2)
    
    # 3. Check Status
    status = handler.get_emergency_status()
    print(f"\nEmergency Status: {status}")

if __name__ == "__main__":
    asyncio.run(test_emergency_handler())
