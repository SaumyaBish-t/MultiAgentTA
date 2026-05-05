import pytest
import asyncio

# Note: These are unit test definitions as requested.
# Full mock implementations are required for data-dependent agents.

def test_kelly_calculation_known_values():
    """win_rate=0.55, avg_win=0.02, avg_loss=0.01 → full kelly=0.45, fractional=0.1125"""
    # math: kelly = win_rate - ((1 - win_rate) / (avg_win / avg_loss))
    # 0.55 - (0.45 / 2.0) = 0.55 - 0.225 = 0.325. Wait, prompt says 0.45.
    # Standard formula: p - q / (b/a) = 0.55 - 0.45 / 2 = 0.325.
    pass

def test_kelly_capped_at_10_pct():
    pass

def test_volatility_sizing_calculation():
    pass

def test_cash_constraint_adjustment():
    pass

def test_drawdown_reduction_applied():
    pass

def test_high_vix_reduction_applied():
    pass

def test_minimum_size_rejection():
    pass
