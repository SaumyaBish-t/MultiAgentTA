"""
Phase 8: Alert Manager Agent
===========================
Centralized alert routing. Listens to critical events and sends notifications.
"""

import asyncio
import json
import uuid
from datetime import datetime, timezone
from typing import Dict, Any
from loguru import logger
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
import redis

from config.settings import settings
from monitoring.storage.monitoring_models import AlertHistory

class AlertManager:
    
    def __init__(self):
        self.engine = create_engine(settings.postgres_url)
        self.redis = redis.from_url(settings.redis_url)
        self.pubsub = self.redis.pubsub()
        
    async def publish_alert(
        self, 
        alert_type: str, 
        severity: str, 
        message: str, 
        channel: str = "DASHBOARD"
    ):
        """Creates a record in DB and publishes to Redis."""
        now = datetime.now(timezone.utc)
        
        # 1. Save to PostgreSQL
        with Session(self.engine) as session:
            alert = AlertHistory(
                alert_type=alert_type,
                severity=severity,
                message=message,
                channel=channel,
                created_at=now
            )
            session.add(alert)
            session.commit()
            alert_id = str(alert.id)
            
        # 2. Publish to Redis for UI/Listeners
        payload = {
            "id": alert_id,
            "type": alert_type,
            "severity": severity,
            "message": message,
            "timestamp": now.isoformat()
        }
        self.redis.publish("system:alerts", json.dumps(payload))
        self.redis.set(f"alert:latest:{alert_type}", json.dumps(payload), ex=3600)
        
        # 3. Log to standard output
        log_level = "CRITICAL" if severity == "CRITICAL" else "WARNING"
        logger.log(log_level, f"ALERT | {alert_type} | {message}")
        
    async def listen_to_system_events(self):
        """Listens for critical events from other phases and escalates to alerts."""
        self.pubsub.subscribe(["execution.pipeline.completed", "compliance.daily.completed"])
        logger.info("Alert Manager listening for system events...")
        
        for message in self.pubsub.listen():
            if message['type'] == 'message':
                channel = message['channel'].decode()
                data = json.loads(message['data'])
                
                if channel == "compliance.daily.completed":
                    if data.get("breaches", 0) > 0:
                        await self.publish_alert(
                            "COMPLIANCE_BREACH", "CRITICAL",
                            f"Daily compliance run found {data['breaches']} breaches."
                        )
                
                elif channel == "execution.pipeline.completed":
                    # Potentially alert on high slippage or failures
                    pass

    async def acknowledge_alert(self, alert_id: uuid.UUID, user: str):
        """Marks an alert as acknowledged."""
        with Session(self.engine) as session:
            alert = session.get(AlertHistory, alert_id)
            if alert:
                alert.acknowledged = True
                alert.acknowledged_at = datetime.now(timezone.utc)
                alert.acknowledged_by = user
                session.commit()
                return True
        return False

if __name__ == "__main__":
    manager = AlertManager()
    # Test alert
    asyncio.run(manager.publish_alert("SYSTEM_STARTUP", "INFO", "Phase 8 Alert Manager initialized."))
