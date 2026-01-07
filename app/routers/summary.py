# routers/summary.py
from fastapi import APIRouter, Depends
from pyiceberg.catalog import load_catalog
from pyiceberg.table import Table
import os
from boto3 import client as boto_client
from botocore.client import Config
from sqlmodel import Session, select
from typing import List
from schemas.summary import HierarchicalSummary, PerformanceMetric
from models.metrics import Metric
from auth.auth import get_current_user
from models.user import User
from database import get_session
from utils import error_handler

router = APIRouter()

CATALOG_URI = f"jdbc:postgresql://{os.environ['DATABASE_URL'].split('@')[1]}?user={os.environ['POSTGRES_USER']}&password={os.environ['POSTGRES_PASSWORD']}"
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

minio_client = boto_client(
    's3',
    endpoint_url=os.environ["MINIO_ENDPOINT"],
    aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
    aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
    config=Config(signature_version='s3v4')
)

def get_file_type(key: str) -> str:
    ext = key.split('.')[-1].lower()
    if ext in ['csv', 'tsv']:
        return 'csv'
    elif ext in ['jpg', 'png', 'gif', 'bmp']:
        return 'image'
    elif ext == 'docx':
        return 'docx'
    else:
        return 'other'

@router.get("/hierarchical", response_model=HierarchicalSummary)
@error_handler(retries=3, exceptions=(Exception,))
def get_hierarchical_summary(current_user: User = Depends(get_current_user)):
    iceberg_summary = {}
    namespaces = catalog.list_namespaces()
    for ns in namespaces:
        ns_str = '.'.join(ns)
        tables = catalog.list_tables(ns)
        table_details = []
        for table_id in tables:
            table = catalog.load_table(table_id)
            schema_dict = {field.name: str(field.field_type) for field in table.schema.fields}
            table_details.append({"table": '.'.join(table_id), "schema": schema_dict})
        iceberg_summary[ns_str] = table_details
    
    minio_summary = {}
    buckets = [bucket['Name'] for bucket in minio_client.list_buckets()['Buckets']]
    for bucket in buckets:
        objects = minio_client.list_objects_v2(Bucket=bucket, Delimiter='/')
        file_list = []
        for prefix in objects.get('CommonPrefixes', []):
            folder = prefix['Prefix']
            folder_objects = minio_client.list_objects_v2(Bucket=bucket, Prefix=folder)
            for obj in folder_objects.get('Contents', []):
                key = obj['Key']
                file_list.append({"path": key, "type": get_file_type(key)})
        for obj in objects.get('Contents', []):
            key = obj['Key']
            if not key.endswith('/'):
                file_list.append({"path": key, "type": get_file_type(key)})
        minio_summary[bucket] = file_list
    
    return {"iceberg": iceberg_summary, "minio": minio_summary}

@router.get("/performance", response_model=List[PerformanceMetric])
@error_handler(retries=3, exceptions=(Exception,))
def get_performance_metrics(session: Session = Depends(get_session), current_user: User = Depends(get_current_user)):
    metrics = session.exec(select(Metric).order_by(Metric.timestamp.desc())).all()
    return [
        PerformanceMetric(
            id=str(m.id),
            endpoint=m.endpoint,
            method=m.method,
            response_time=m.response_time,
            status_code=m.status_code,
            timestamp=m.timestamp.isoformat()
        ) for m in metrics
    ]