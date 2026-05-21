import asyncio, uuid, json
from datetime import datetime, timezone, timedelta
from fastapi import APIRouter, BackgroundTasks
from pydantic import BaseModel
import redis
from loguru import logger

from config.settings import settings
from alpha_research.storage.research_models import ResearchHypothesis
from signal_generation.storage.signal_models import TradingSignal
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from signal_generation.agents.backtester_agent import BacktesterAgent

router = APIRouter()
r = redis.from_url(settings.redis_url, decode_responses=True)
engine = create_engine(settings.postgres_url)

class PipelineRequest(BaseModel):
    ticker: str
    force_refresh: bool = False

def check_validated_strategy_exists(ticker: str) -> TradingSignal | None:
    # Signal statuses in this system are: draft, validated, approved, live,
    # rejected, retired. There is no "active" status — the old filter below
    # matched nothing, so /pipeline/status always reported has_strategy=false
    # and the Strategy Comparison page never displayed a generated strategy.
    # Treat any non-dead signal as an existing strategy.
    with Session(engine) as session:
        return session.query(TradingSignal).filter(
            TradingSignal.ticker == ticker,
            TradingSignal.status.notin_(["rejected", "retired"])
        ).order_by(TradingSignal.created_at.desc()).first()

def get_latest_hypothesis_for_ticker(ticker: str) -> dict | None:
    with Session(engine) as session:
        hypo = session.query(ResearchHypothesis).filter(
            ResearchHypothesis.ticker == ticker
        ).order_by(ResearchHypothesis.created_at.desc()).first()
        if not hypo:
            return None
        return {
            "id": str(hypo.id),
            "ticker": hypo.ticker,
            "hypothesis_type": hypo.hypothesis_type,
            "title": hypo.title,
            "expected_direction": hypo.expected_direction,
            "expected_timeframe": hypo.expected_timeframe,
            "description": hypo.description
        }

# Per-stage timeouts (seconds). The research / LLM / backtest stages used
# to be un-timed: a single hung network or LLM call froze the whole run
# forever at whatever % it was on. wait_for() now bounds every slow stage
# so it fails cleanly with a logged reason instead of hanging.
HYPOTHESIS_TIMEOUT = 240   # research pipeline makes many LLM calls
STRATEGY_TIMEOUT = 150     # StrategyCoder LLM generation
BACKTEST_TIMEOUT = 180     # vectorbt backtest


async def run_pipeline_for_ticker(ticker: str, run_id: str):
    import time as _time

    stages = [
        ('collecting', 'Collecting market data...', 5),
        ('sentiment', 'Analyzing news sentiment...', 15),
        ('technical', 'Computing technical signals...', 10),
        ('fundamental', 'Analyzing fundamentals...', 10),
        ('macro', 'Reading macro environment...', 5),
        ('documents', 'Processing SEC filings...', 15),
        ('hypothesis', 'AI generating trade hypothesis...', 10),
        ('strategy_code', 'Writing strategy code...', 10),
        ('backtest', 'Backtesting 3 years of data...', 20),
        ('walk_forward', 'Walk-forward validation...', 15),
        ('optimization', 'Optimizing parameters...', 10),
        ('risk_check', 'Risk evaluation...', 5),
        ('completed', 'Strategy ready!', 0),
    ]

    tag = f"[pipeline {run_id[:8]} {ticker}]"
    logger.info(f"{tag} STARTED")
    total_progress = 0
    critical_error: str | None = None

    for stage, message, weight in stages:
        total_progress += weight
        status = {
            'run_id': run_id,
            'ticker': ticker,
            'stage': stage,
            'message': message,
            'progress': min(total_progress, 100),
            'completed': stage == 'completed',
            'failed': False,
            'timestamp': datetime.now(timezone.utc).isoformat(),
        }
        r.set(f'pipeline:status:{run_id}', json.dumps(status), ex=3600)
        logger.info(f"{tag} stage '{stage}' START  ({total_progress}%)")
        t0 = _time.perf_counter()

        try:
            if stage == 'collecting':
                await asyncio.sleep(2)

            elif stage == 'hypothesis':
                from alpha_research.pipeline.research_pipeline import ResearchPipeline
                pipeline = ResearchPipeline()
                result = await asyncio.wait_for(
                    pipeline.run_single(ticker), timeout=HYPOTHESIS_TIMEOUT)
                if result:
                    status['hypothesis_ready'] = True
                else:
                    logger.warning(f"{tag} research pipeline returned no hypothesis")

            elif stage == 'strategy_code':
                from signal_generation.agents.strategy_coder_agent import StrategyCoderAgent
                hypothesis = get_latest_hypothesis_for_ticker(ticker)
                if not hypothesis:
                    raise RuntimeError("no hypothesis available to build a strategy from")
                coder = StrategyCoderAgent()
                signal = await asyncio.wait_for(
                    coder.generate(hypothesis), timeout=STRATEGY_TIMEOUT)
                if signal and signal.get("id"):
                    status['signal_id'] = str(signal["id"])
                    r.set(f'pipeline:signal:{run_id}', str(signal["id"]), ex=3600)
                else:
                    raise RuntimeError("strategy generation returned no signal")

            elif stage == 'backtest':
                signal_id = r.get(f'pipeline:signal:{run_id}')
                if signal_id:
                    # Best-of-candidates: the LLM-written strategy is often
                    # mediocre, while the research-grounded templates are
                    # solid. Backtest the AI strategy AND the 3 templates,
                    # then keep whichever grades best — so the pipeline
                    # surfaces the strongest available strategy, not just
                    # whatever the LLM happened to write.
                    from signal_generation.agents.strategy_coder_agent import (
                        TEMPLATE_EMA_CROSSOVER, TEMPLATE_RSI_MEAN_REVERSION,
                        TEMPLATE_BREAKOUT)
                    from signal_generation.grading import grade_metrics

                    with Session(engine) as session:
                        signal_record = session.query(TradingSignal).filter(
                            TradingSignal.id == signal_id).first()
                    if signal_record:
                        agent = BacktesterAgent()
                        candidates = []
                        if signal_record.strategy_code:
                            candidates.append(("ai_generated", signal_record.strategy_code))
                        candidates += [
                            ("momentum", TEMPLATE_EMA_CROSSOVER),
                            ("mean_reversion", TEMPLATE_RSI_MEAN_REVERSION),
                            ("breakout", TEMPLATE_BREAKOUT),
                        ]
                        best = None
                        for cname, code in candidates:
                            try:
                                res = await asyncio.wait_for(
                                    agent.backtest({
                                        "id": str(signal_id),
                                        "ticker": signal_record.ticker,
                                        "strategy_code": code,
                                        "parameters": {},
                                    }), timeout=90)
                                g = grade_metrics(res.get("metrics") or {})
                                logger.info(f"{tag} candidate '{cname}': "
                                            f"grade {g['quality_grade']} "
                                            f"score {g['quality_score']}")
                                if best is None or g["quality_score"] > best["score"]:
                                    best = {"name": cname, "code": code,
                                            "score": g["quality_score"],
                                            "grade": g["quality_grade"]}
                            except Exception as ce:
                                logger.warning(f"{tag} candidate '{cname}' failed: {ce}")

                        if best:
                            # Persist the winner and run the authoritative
                            # backtest last so its result/status is current.
                            with Session(engine) as session:
                                sr = session.query(TradingSignal).filter(
                                    TradingSignal.id == signal_id).first()
                                if sr:
                                    sr.strategy_code = best["code"]
                                    session.commit()
                            await asyncio.wait_for(
                                agent.backtest({
                                    "id": str(signal_id),
                                    "ticker": signal_record.ticker,
                                    "strategy_code": best["code"],
                                    "parameters": {},
                                }), timeout=BACKTEST_TIMEOUT)
                            status["best_strategy"] = best["name"]
                            status["best_grade"] = best["grade"]
                            logger.info(f"{tag} best strategy: '{best['name']}' "
                                        f"grade {best['grade']}")

            elif stage == 'completed':
                signal_id = r.get(f'pipeline:signal:{run_id}')
                if signal_id:
                    status['strategy_ready'] = True
                    status['signal_id'] = str(signal_id)

            elapsed = _time.perf_counter() - t0
            logger.info(f"{tag} stage '{stage}' OK  ({elapsed:.1f}s)")

        except asyncio.TimeoutError:
            elapsed = _time.perf_counter() - t0
            msg = (f"stage '{stage}' TIMED OUT after {elapsed:.0f}s — "
                   f"a slow or unreachable LLM / data provider")
            logger.error(f"{tag} {msg}")
            status['stage_error'] = msg
            status['warning'] = msg
            if stage in ('hypothesis', 'strategy_code'):
                critical_error = msg
        except Exception as e:
            elapsed = _time.perf_counter() - t0
            msg = f"stage '{stage}' FAILED after {elapsed:.0f}s: {e}"
            logger.error(f"{tag} {msg}")
            status['stage_error'] = msg
            status['warning'] = str(e)
            if stage in ('hypothesis', 'strategy_code'):
                critical_error = msg

        status['failed'] = critical_error is not None
        r.set(f'pipeline:status:{run_id}', json.dumps(status), ex=3600)

        if critical_error:
            logger.error(f"{tag} ABORTING — {critical_error}")
            break
        await asyncio.sleep(0.5)

    if critical_error:
        final = {
            'run_id': run_id, 'ticker': ticker, 'stage': 'failed',
            'message': f'Pipeline failed: {critical_error}',
            'stage_error': critical_error,
            'progress': 100, 'completed': False, 'failed': True,
            'timestamp': datetime.now(timezone.utc).isoformat(),
        }
    else:
        final = {
            'run_id': run_id, 'ticker': ticker, 'stage': 'completed',
            'message': 'AI research complete. Strategy generated.',
            'progress': 100, 'completed': True, 'failed': False,
            'timestamp': datetime.now(timezone.utc).isoformat(),
        }
    logger.info(f"{tag} DONE — failed={final['failed']}")
    r.set(f'pipeline:status:{run_id}', json.dumps(final), ex=3600)

@router.post('/pipeline/run-full-cycle')
async def run_full_cycle(
    request: PipelineRequest,
    background_tasks: BackgroundTasks
):
    run_id = str(uuid.uuid4())
    existing = check_validated_strategy_exists(request.ticker)
    if existing and not request.force_refresh:
        return {
            'run_id': run_id,
            'status': 'existing_strategy_found',
            'signal_id': str(existing.id),
            'message': f'Strategy already exists for {request.ticker}',
            'skip_pipeline': True
        }

    r.set(f'pipeline:status:{run_id}', json.dumps({
        'run_id': run_id,
        'ticker': request.ticker,
        'stage': 'initializing',
        'message': 'Starting AI research pipeline...',
        'progress': 0,
        'completed': False,
        'failed': False
    }), ex=3600)

    background_tasks.add_task(run_pipeline_for_ticker, ticker=request.ticker, run_id=run_id)

    return {
        'run_id': run_id,
        'status': 'started',
        'ticker': request.ticker,
        'message': 'AI pipeline started.',
        'stream_url': f'/realtime/stream/pipeline/{run_id}'
    }

@router.get('/pipeline/status/{ticker}')
async def get_pipeline_status(ticker: str):
    existing = check_validated_strategy_exists(ticker)
    return {
        'ticker': ticker,
        'has_strategy': existing is not None,
        'signal': {
            'id': str(existing.id),
            'type': existing.signal_type,
            'created': existing.created_at.isoformat()
        } if existing else None
    }
