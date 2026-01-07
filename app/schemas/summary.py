# schemas/summary.py
from pydantic import BaseModel
from typing import Dict, List, Any

class HierarchicalSummary(BaseModel):
    iceberg: Dict[str, List[Dict[str, Any]]]
    minio: Dict[str, List[Dict[str, str]]]

class PerformanceMetric(BaseModel):
    id: str
    endpoint: str
    method: str
    response_time: float
    status_code: int
    timestamp: str