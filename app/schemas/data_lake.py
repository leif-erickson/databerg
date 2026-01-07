# schemas/data_lake.py
from pydantic import BaseModel
from typing import List, Dict, Any

class FileAdd(BaseModel):
    table_name: str
    data: List[Dict[str, Any]]

class FileGet(BaseModel):
    table_name: str
    filters: Dict[str, Any] = {}

class FileDelete(BaseModel):
    table_name: str
    filters: Dict[str, Any]