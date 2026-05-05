from pydantic import BaseModel, ConfigDict
from datetime import datetime, date
from uuid import UUID
from typing import Any, Optional

class PortfolioBase(BaseModel):
    name: str
    strategy: str
    total_capital: float
    invested_capital: float = 0.0
    cash: float
    status: str
    
class PortfolioCreate(PortfolioBase):
    pass
    
class PortfolioResponse(PortfolioBase):
    id: UUID
    created_at: datetime
    updated_at: datetime
    
    model_config = ConfigDict(from_attributes=True)
