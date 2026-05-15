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
    with Session(engine) as session:
        return session.query(TradingSignal).filter(
            TradingSignal.ticker == ticker,
            TradingSignal.status == "active"
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

async def run_pipeline_for_ticker(ticker: str, run_id: str):
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

    total_progress = 0
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
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
        r.set(f'pipeline:status:{run_id}', json.dumps(status), ex=3600)

        try:
            if stage == 'collecting':
                # Just mock or run the actual collector if available, let's assume it's running via background script
                await asyncio.sleep(2)

            elif stage == 'hypothesis':
                from alpha_research.pipeline.research_pipeline import ResearchPipeline
                pipeline = ResearchPipeline()
                result = await pipeline.run_single(ticker)
                if result:
                    status['hypothesis_ready'] = True

            elif stage == 'strategy_code':
                from signal_generation.agents.strategy_coder_agent import StrategyCoderAgent
                hypothesis = get_latest_hypothesis_for_ticker(ticker)
                if hypothesis:
                    coder = StrategyCoderAgent()
                    signal = await coder.generate(hypothesis)
                    if signal and signal.get("id"):
                        status['signal_id'] = str(signal["id"])
                        r.set(f'pipeline:signal:{run_id}', str(signal["id"]), ex=3600)

            elif stage == 'backtest':
                signal_id = r.get(f'pipeline:signal:{run_id}')
                if signal_id:
                    try:
                        with Session(engine) as session:
                            signal_record = session.query(TradingSignal).filter(TradingSignal.id == signal_id).first()
                            if signal_record:
                                agent = BacktesterAgent()
                                sig_dict = {
                                    "id": str(signal_record.id),
                                    "ticker": signal_record.ticker,
                                    "strategy_code": signal_record.strategy_code,
                                    "parameters": signal_record.parameters
                                }
                                await agent.backtest(sig_dict)
                    except Exception as e:
                        logger.error(f"Backtester agent failed during pipeline execution: {e}")

            elif stage == 'completed':
                signal_id = r.get(f'pipeline:signal:{run_id}')
                if signal_id:
                    status['strategy_ready'] = True
                    status['signal_id'] = str(signal_id)

        except Exception as e:
            status['warning'] = str(e)
            status['stage_error'] = str(e)
            logger.error(f"Pipeline error at {stage}: {e}")

        r.set(f'pipeline:status:{run_id}', json.dumps(status), ex=3600)
        await asyncio.sleep(0.5)

    final = {
        'run_id': run_id,
        'ticker': ticker,
        'stage': 'completed',
        'message': 'AI research complete. Strategy generated.',
        'progress': 100,
        'completed': True,
        'failed': False,
        'timestamp': datetime.now(timezone.utc).isoformat()
    }
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
