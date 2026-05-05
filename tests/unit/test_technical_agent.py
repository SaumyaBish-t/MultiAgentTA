import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from alpha_research.agents.technical_agent import calculate_indicators_node, detect_signals_node

def _generate_dummy_price_data(bars=60):
    base_time = datetime.now(timezone.utc)
    data = []
    price = 100.0
    for i in range(bars):
        price += np.random.normal(0, 1)
        data.append({
            "timestamp": (base_time + timedelta(days=i)).isoformat(),
            "open": price,
            "high": price + 2,
            "low": price - 2,
            "close": price,
            "volume": 100000
        })
    return {"timestamp": [d["timestamp"] for d in data], 
            "open": [d["open"] for d in data],
            "high": [d["high"] for d in data],
            "low": [d["low"] for d in data],
            "close": [d["close"] for d in data],
            "volume": [d["volume"] for d in data]}


@pytest.mark.asyncio
async def test_rsi_calculation_known_values():
    # Make price go up strictly for 15 bars -> RSI should be near 100
    data = _generate_dummy_price_data()
    data["close"] = [100.0 + i for i in range(60)] # Strict uptrend
    
    state = {"ticker": "AAPL", "price_data": data}
    res = await calculate_indicators_node(state)
    
    assert res["indicators"]["rsi_14"] > 95.0


@pytest.mark.asyncio
async def test_macd_calculation():
    data = _generate_dummy_price_data()
    state = {"ticker": "AAPL", "price_data": data}
    res = await calculate_indicators_node(state)
    
    ind = res["indicators"]
    assert "macd" in ind
    assert "macd_signal" in ind
    assert "macd_hist" in ind


@pytest.mark.asyncio
async def test_bollinger_bands():
    data = _generate_dummy_price_data()
    state = {"ticker": "AAPL", "price_data": data}
    res = await calculate_indicators_node(state)
    
    ind = res["indicators"]
    assert ind["bb_upper"] > ind["current_price"]
    assert ind["bb_lower"] < ind["current_price"]
    assert ind["bb_bandwidth"] > 0


@pytest.mark.asyncio
async def test_ema_crossover_detection():
    # Mocking indicators to trigger a specific signal
    state = {
        "ticker": "AAPL",
        "timeframe": "1d",
        "indicators": {
            "current_price": 150,
            "ema_200": 140,
            "price_vs_ema200": "above",
            "rsi_14": 50,
            "macd": 0,
            "macd_signal": 0,
            "macd_hist": 0,
            "bb_bandwidth": 0.2,
            "vol_ratio": 1.0
        }
    }
    
    res = await detect_signals_node(state)
    signals = res["signals"]
    
    ema_signal = next((s for s in signals if s["indicator_name"] == "EMA_200"), None)
    assert ema_signal is not None
    assert ema_signal["signal_direction"] == "bullish"


@pytest.mark.asyncio
async def test_insufficient_data_handled():
    # Less than 50 bars should be caught in fetch, but calculate shouldn't crash
    data = _generate_dummy_price_data(bars=10)
    state = {"ticker": "AAPL", "price_data": data}
    res = await calculate_indicators_node(state)
    # It will still calculate what it can or leave NaNs, but won't crash
    assert "indicators" in res
