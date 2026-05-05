import pytest
from alpha_research.agents.fundamental_agent import score_factors_node

@pytest.mark.asyncio
async def test_value_score_pe_thresholds():
    # PE < 15 -> 1.0, 15-25 -> 0.7, > 40 -> 0.2
    
    # PE < 15
    state1 = {"computed_ratios": {"pe_ratio": 10, "ps_ratio": 0}}
    res1 = await score_factors_node(state1)
    # PS = 0 is ignored, default 0.5. pe=1.0. avg=(1.0+0.5)/2 = 0.75
    assert res1["factor_scores"]["value"] == 0.75
    
    # PE > 40
    state2 = {"computed_ratios": {"pe_ratio": 50, "ps_ratio": 0}}
    res2 = await score_factors_node(state2)
    assert res2["factor_scores"]["value"] == 0.35 # (0.2 + 0.5) / 2


@pytest.mark.asyncio
async def test_growth_score_revenue_growth():
    # Rev growth > 20% -> 1.0, 10-20% -> 0.7, 0-10% -> 0.4, < 0 -> 0.1
    
    # > 20%
    state1 = {"computed_ratios": {"revenue_growth_yoy": 0.25}}
    res1 = await score_factors_node(state1)
    assert res1["factor_scores"]["growth"] == 1.0
    
    # < 0%
    state2 = {"computed_ratios": {"revenue_growth_yoy": -0.05}}
    res2 = await score_factors_node(state2)
    assert res2["factor_scores"]["growth"] == 0.1


@pytest.mark.asyncio
async def test_quality_score_roe_debt():
    # ROE > 20% -> 1.0
    # D/E < 0.5 -> 1.0
    
    state = {"computed_ratios": {"roe": 0.25, "debt_to_equity": 0.2}}
    res = await score_factors_node(state)
    assert res["factor_scores"]["quality"] == 1.0


@pytest.mark.asyncio
async def test_overall_score_weighted_combination():
    # overall = value*0.3 + growth*0.4 + quality*0.3
    state = {
        "computed_ratios": {
            "pe_ratio": 10, "ps_ratio": 1,         # Value = 1.0
            "revenue_growth_yoy": 0.25,            # Growth = 1.0
            "roe": 0.25, "debt_to_equity": 0.2     # Quality = 1.0
        }
    }
    res = await score_factors_node(state)
    assert res["overall_score"] == 1.0
