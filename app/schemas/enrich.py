# schemas/enrich.py
from pydantic import BaseModel

class Query(BaseModel):
    question: str
    table_name: str
    versions_deep: int = 0

class Analyze(BaseModel):
    table_name: str
    versions_deep: int = 0