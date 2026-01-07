# models/metrics.py
from sqlmodel import SQLModel, Field
from typing import Optional
from datetime import datetime
from uuid import UUID, uuid4

class Metric(SQLModel, table=True):
    id: UUID = Field(default_factory=uuid4, primary_key=True)
    endpoint: str
    method: str
    response_time: float
    status_code: int
    timestamp: datetime = Field(default_factory=datetime.utcnow)