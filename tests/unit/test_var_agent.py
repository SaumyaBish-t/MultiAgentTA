import pytest
import numpy as np

def test_historical_var_calculation():
    """Use known return series, verify VaR matches manual calc"""
    pass

def test_cvar_is_worse_than_var():
    """CVaR must always be >= VaR"""
    pass

def test_square_root_time_scaling():
    """var_5day must equal var_1day * sqrt(5)"""
    pass

def test_monte_carlo_within_range():
    """MC VaR should be within 30% of historical VaR"""
    pass

def test_stress_test_scenarios():
    """2008 scenario must show -40% result"""
    pass

def test_var_limit_breach_detected():
    pass
