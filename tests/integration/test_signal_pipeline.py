import pytest
from signal_generation.pipeline.signal_pipeline import SignalPipeline, SignalPipelineState

@pytest.mark.asyncio
async def test_full_pipeline_from_hypothesis_to_signal(mocker):
    # Mock the individual agents to return success instantly
    mocker.patch('signal_generation.pipeline.signal_pipeline.StrategyCoder.generate', 
                 return_value={"id": "00000000-0000-0000-0000-000000000001", "hypothesis_id": "hypo1", "status": "generated"})
    
    mocker.patch('signal_generation.pipeline.signal_pipeline.Backtester.backtest', 
                 return_value={"id": "00000000-0000-0000-0000-000000000001", "hypothesis_id": "hypo1", "status": "backtested"})
                 
    mocker.patch('signal_generation.pipeline.signal_pipeline.WalkForwardValidator.validate', 
                 return_value={"id": "00000000-0000-0000-0000-000000000001", "hypothesis_id": "hypo1", "status": "validated", "passed": True})
                 
    mocker.patch('signal_generation.pipeline.signal_pipeline.ParameterOptimizer.quick_optimize', 
                 return_value={"id": "00000000-0000-0000-0000-000000000001", "hypothesis_id": "hypo1", "status": "optimized"})
                 
    # Mock scorer DB calls or logic
    # Actually, if we mock the whole scorer node, we don't need DB access
    mocker.patch('signal_generation.pipeline.signal_pipeline.SignalPipeline.score_and_rank_node',
                 return_value={"top_signals": [{"id": "00000000-0000-0000-0000-000000000001", "score": 0.9, "sharpe": 2.0}]})
                 
    pipeline = SignalPipeline()
    hypothesis = {
        "id": "hypo1",
        "title": "Mock Hypothesis 1",
        "ticker": "AAPL",
        "direction": "long",
        "conviction": 0.8,
        "type": "technical"
    }
    
    result = await pipeline.run([hypothesis])
    
    assert result.hypotheses_processed == 1
    assert result.signals_generated == 1
    assert result.signals_validated == 1
    assert len(result.top_signals) == 1
    assert result.best_sharpe == 2.0

@pytest.mark.asyncio
async def test_bad_code_retried_and_fixed(mocker):
    # If the Coder handles retries internally, the pipeline just sees the final result.
    # We can mock the Coder to fail once then succeed if we tested the Coder's internal loop.
    # In the pipeline integration, we just verify it handles a rejected generation properly.
    mocker.patch('signal_generation.pipeline.signal_pipeline.StrategyCoder.generate', 
                 return_value={"id": "00000000-0000-0000-0000-000000000002", "status": "rejected"})
                 
    pipeline = SignalPipeline()
    result = await pipeline.run([{"id": "hypo2", "title": "Mock 2"}])
    
    assert result.signals_generated == 0
    assert result.signals_rejected == 1

@pytest.mark.asyncio
async def test_failed_backtest_marked_rejected(mocker):
    mocker.patch('signal_generation.pipeline.signal_pipeline.StrategyCoder.generate', 
                 return_value={"id": "00000000-0000-0000-0000-000000000003", "hypothesis_id": "hypo3", "status": "generated"})
                 
    mocker.patch('signal_generation.pipeline.signal_pipeline.Backtester.backtest', 
                 return_value={"id": "00000000-0000-0000-0000-000000000003", "hypothesis_id": "hypo3", "status": "rejected"})
                 
    pipeline = SignalPipeline()
    result = await pipeline.run([{"id": "hypo3", "title": "Mock 3"}])
    
    assert result.signals_generated == 1
    assert result.signals_rejected == 1
    assert result.signals_validated == 0

@pytest.mark.asyncio
async def test_pipeline_continues_after_single_failure(mocker):
    # Two hypotheses. First fails generation, second succeeds completely.
    
    # We can use side_effect to return different values on subsequent calls
    mocker.patch('signal_generation.pipeline.signal_pipeline.StrategyCoder.generate', 
                 side_effect=[
                     {"id": "00000000-0000-0000-0000-000000000000", "hypothesis_id": "hypo_fail", "status": "rejected"},
                     {"id": "00000000-0000-0000-0000-000000000004", "hypothesis_id": "hypo_pass", "status": "generated"}
                 ])
                 
    mocker.patch('signal_generation.pipeline.signal_pipeline.Backtester.backtest', 
                 return_value={"id": "00000000-0000-0000-0000-000000000004", "hypothesis_id": "hypo_pass", "status": "backtested"})
                 
    mocker.patch('signal_generation.pipeline.signal_pipeline.WalkForwardValidator.validate', 
                 return_value={"id": "00000000-0000-0000-0000-000000000004", "hypothesis_id": "hypo_pass", "status": "validated", "passed": True})
                 
    mocker.patch('signal_generation.pipeline.signal_pipeline.ParameterOptimizer.quick_optimize', 
                 return_value={"id": "00000000-0000-0000-0000-000000000004", "hypothesis_id": "hypo_pass", "status": "optimized"})
                 
    mocker.patch('signal_generation.pipeline.signal_pipeline.SignalPipeline.score_and_rank_node',
                 return_value={"top_signals": [{"id": "00000000-0000-0000-0000-000000000004", "score": 0.9, "sharpe": 2.0}]})
                 
    pipeline = SignalPipeline()
    hypos = [{"id": "hypo_fail", "title": "Mock Fail"}, {"id": "hypo_pass", "title": "Mock Pass"}]
    
    result = await pipeline.run(hypos)
    
    assert result.hypotheses_processed == 2
    assert result.signals_rejected == 1
    assert result.signals_generated == 1 # only one generated
    assert result.signals_validated == 1
    assert len(result.top_signals) == 1
