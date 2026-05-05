"""
Phase 8: Drift Detection Agent
==============================
Monitors feature distributions for statistical drift.
Integrates with Phase 2 (Machine Learning) models.
"""

import asyncio
import json
import numpy as np
from scipy import stats
from datetime import datetime, timezone
from typing import List, Dict, Any
from loguru import logger
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
import redis

from config.settings import settings
from monitoring.storage.monitoring_models import FeaturePerformanceDrift

class DriftDetectionAgent:
    
    def __init__(self):
        self.engine = create_engine(settings.postgres_url)
        self.redis = redis.from_url(settings.redis_url)
        
    def calculate_drift(self, baseline: List[float], current: List[float]) -> Dict[str, Any]:
        """Calculates drift score using Kolmogorov-Smirnov test."""
        if not baseline or not current:
            return {"drift_score": 0, "p_value": 1.0, "is_drifting": False}
        
        # KS Test
        statistic, p_value = stats.ks_2samp(baseline, current)
        
        # We consider drift if p-value < 0.05
        is_drifting = p_value < 0.05
        
        return {
            "drift_score": float(statistic),
            "p_value": float(p_value),
            "is_drifting": is_drifting,
            "baseline_stats": {
                "mean": float(np.mean(baseline)),
                "std": float(np.std(baseline)),
                "n": len(baseline)
            },
            "current_stats": {
                "mean": float(np.mean(current)),
                "std": float(np.std(current)),
                "n": len(current)
            }
        }

    async def check_feature_drift(self, feature_name: str, baseline_data: List[float]):
        """Fetches live data from Redis and checks for drift."""
        # Live data stored in Redis during ingestion/inference
        live_key = f"features:live:{feature_name}"
        live_data_raw = self.redis.lrange(live_key, 0, 1000)
        
        if not live_data_raw:
            return
            
        live_data = [float(x) for x in live_data_raw]
        
        result = self.calculate_drift(baseline_data, live_data)
        
        # Save to DB
        with Session(self.engine) as session:
            drift = FeaturePerformanceDrift(
                feature_name=feature_name,
                drift_score=result["drift_score"],
                is_drifting=result["is_drifting"],
                p_value=result["p_value"],
                baseline_stats=result["baseline_stats"],
                current_stats=result["current_stats"],
                detected_at=datetime.now(timezone.utc)
            )
            session.add(drift)
            session.commit()
            
        if result["is_drifting"]:
            logger.warning(f"DRIFT DETECTED | {feature_name} | score: {result['drift_score']:.3f} | p-value: {result['p_value']:.4f}")
            # Emit alert via Redis
            self.redis.publish("monitoring:drift", json.dumps({
                "feature": feature_name,
                "score": result["drift_score"],
                "timestamp": datetime.now(timezone.utc).isoformat()
            }))

if __name__ == "__main__":
    agent = DriftDetectionAgent()
    # Mock run
    baseline = np.random.normal(0, 1, 1000).tolist()
    current = np.random.normal(0.2, 1.1, 1000).tolist()
    res = agent.calculate_drift(baseline, current)
    print(f"Drift check result: {res}")
