"""
Direct Master Orchestrator Execution
"""

import asyncio
from monitoring.pipeline.master_orchestrator import master_orchestrator

async def run_master():
    print("Initializing Master Orchestrator...")
    await master_orchestrator.startup()
    
    print("\nStarting Master System Run...")
    result = await master_orchestrator.run(run_type="manual")
    
    print("\nMASTER RUN SUMMARY:")
    print(f"  Run ID:           {result.run_id}")
    print(f"  Duration:         {result.duration_seconds:.2f}s")
    print(f"  Phases OK:        {result.phases_completed}")
    print(f"  Phases Failed:    {result.phases_failed}")
    print(f"  Portfolio Value:  ${result.portfolio_value:,.2f}")
    print(f"  Feedback Actions: {result.feedback_actions}")

if __name__ == "__main__":
    asyncio.run(run_master())
