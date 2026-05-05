import asyncio
from execution.brokers.alpaca_adapter import AlpacaBrokerAdapter

async def verify_adapter():
    print("Verifying Alpaca Broker Adapter...")
    adapter = AlpacaBrokerAdapter()
    
    # 1. Test Account
    print("\nTesting get_account()...")
    acc = adapter.get_account()
    print(f"  Account Number: {acc['account_number']}")
    print(f"  Cash: ${acc['cash']:,.2f}")
    
    # 2. Test Market Data
    print("\nTesting get_latest_price('AAPL')...")
    price = adapter.get_latest_price("AAPL")
    print(f"  AAPL Price: ${price:.2f}")
    
    # 3. Test Positions
    print("\nTesting get_positions()...")
    pos = adapter.get_positions()
    print(f"  Positions Count: {len(pos)}")
    
    # 4. Test Clock
    print("\nTesting get_market_clock()...")
    clock = adapter.get_market_clock()
    print(f"  Market Open: {clock['is_open']}")
    print(f"  Next Close: {clock['next_close']}")

if __name__ == "__main__":
    asyncio.run(verify_adapter())
