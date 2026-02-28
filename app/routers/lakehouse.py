# routers/data_lake.py
from fastapi import APIRouter, Depends, HTTPException
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import StringType, IntegerType, NestedField, BinaryType
from pyiceberg.table import Table
from typing import List, Dict, Any
from databerg.app.schemas.lakehouse import FileAdd, FileGet, FileDelete
from auth.auth import get_current_user
from models.user import User
import os
import polars as pl
import duckdb
from utils import error_handler

router = APIRouter()

CATALOG_URI = "jdbc:postgresql://postgres:5432/data_platform?user=user&password=password"
catalog = load_catalog(
    "jdbc",
    **{
        "uri": CATALOG_URI,
        "s3.endpoint": os.environ["MINIO_ENDPOINT"],
        "s3.access-key-id": os.environ["MINIO_ACCESS_KEY"],
        "s3.secret-access-key": os.environ["MINIO_SECRET_KEY"],
        "warehouse": "s3://warehouse/",
    }
)

FILE_SCHEMA = Schema(
    NestedField(field_id=1, name="file_id", field_type=IntegerType(), required=False),
    NestedField(field_id=2, name="file_name", field_type=StringType(), required=True),
    NestedField(field_id=3, name="content", field_type=StringType(), required=False),
    NestedField(field_id=4, name="image_content", field_type=BinaryType(), required=False),
)

def get_or_create_table(table_name: str) -> Table:
    identifier = ("iceberg_catalog", table_name)
    if identifier in catalog.list_tables("iceberg_catalog"):
        return catalog.load_table(identifier)
    else:
        return catalog.create_table(identifier, schema=FILE_SCHEMA)

@router.post("/files/")
@error_handler(retries=3, exceptions=(Exception,))
def add_file(file: FileAdd, current_user: User = Depends(get_current_user)):
    table = get_or_create_table(file.table_name)
    df = pl.DataFrame(file.data)
    table.append(df)
    return {"status": "added"}

@router.get("/files/", response_model=List[Dict[str, Any]])
@error_handler(retries=3, exceptions=(Exception,))
def get_files(file: FileGet = Depends(), current_user: User = Depends(get_current_user)):
    table = get_or_create_table(file.table_name)
    scan = table.scan()
    for key, value in file.filters.items():
        scan = scan.filter(f"{key} == '{value}'")
    df = scan.to_polars()
    return df.to_dicts()

@router.delete("/files/")
@error_handler(retries=3, exceptions=(Exception,))
def delete_file(file: FileDelete, current_user: User = Depends(get_current_user)):
    table = get_or_create_table(file.table_name)
    scan = table.scan()
    for key, value in file.filters.items():
        scan = scan.filter(f"{key} != '{value}'")
    df = scan.to_polars()
    table.overwrite(df)
    return {"status": "deleted"}