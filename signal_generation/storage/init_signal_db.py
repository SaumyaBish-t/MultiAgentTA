import uuid
from datetime import datetime, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from loguru import logger

from config.settings import settings
# Import FundamentalBase to ensure all metadata is gathered
from data_ingestion.storage.models import FundamentalBase
# Import the new Phase 3 models so they are registered with FundamentalBase.metadata
import signal_generation.storage.signal_models as signal_models
# Also import Phase 2 models in case we need to reference them
import alpha_research.storage.research_models as research_models

def init_signal_db():
    logger.info("Initializing Phase 3: Signal Generation database tables...")
    
    # Use the fundamentals database URL from settings
    engine = create_engine(settings.postgres_url)
    
    # Create all tables (this will create any tables defined in signal_models that don't exist yet)
    try:
        FundamentalBase.metadata.create_all(engine)
        logger.info("✅ Successfully created Phase 3 tables.")
    except Exception as e:
        logger.error(f"❌ Failed to create tables: {e}")
        return

    # Insert a test SignalGenerationRun to verify
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    session = SessionLocal()
    
    try:
        # Check if we already have a mock run
        existing = session.query(signal_models.SignalGenerationRun).first()
        if not existing:
            # First, we need a valid hypothesis_id. We'll grab one from research_hypotheses
            # or generate a fake one if none exists (though foreign keys aren't strictly enforced
            # unless specified, actually hypothesis_id does not have a ForeignKey constraint in our SignalGenerationRun model,
            # wait, it does NOT have a FK constraint in our model definition, so any UUID works).
            mock_run = signal_models.SignalGenerationRun(
                id=uuid.uuid4(),
                hypothesis_id=uuid.uuid4(),
                signals_generated=5,
                signals_backtested=5,
                signals_passed=2,
                signals_rejected=3,
                best_sharpe=1.85,
                best_signal_id=None,
                duration_seconds=45.5,
                status="completed",
                started_at=datetime.now(timezone.utc),
                completed_at=datetime.now(timezone.utc)
            )
            session.add(mock_run)
            session.commit()
            logger.info("✅ Inserted mock SignalGenerationRun for verification.")
        else:
            logger.info("ℹ️ Mock SignalGenerationRun already exists.")
            
    except Exception as e:
        session.rollback()
        logger.error(f"❌ Failed to insert mock data: {e}")
    finally:
        session.close()
        engine.dispose()

if __name__ == "__main__":
    init_signal_db()
