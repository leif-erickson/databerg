# routers/enrich.py
from fastapi import APIRouter, Depends, HTTPException
from pyiceberg.catalog import load_catalog
import os
import polars as pl
import duckdb
from langchain_community.llms import Ollama
from langchain_community.utilities.sql_database import SQLDatabase
from langchain.chains import create_sql_query_chain
from schemas.enrich import Query, Analyze
from auth.auth import get_current_user
from models.user import User
import tempfile
import spacy
import easyocr
from typing import List, Dict, Any
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

nlp = spacy.load("en_core_web_sm")
reader = easyocr.Reader(['en'])

def get_table(table_name: str):
    identifier = ("iceberg_catalog", table_name)
    if identifier in catalog.list_tables("iceberg_catalog"):
        return catalog.load_table(identifier)
    raise HTTPException(status_code=404, detail="Table not found")

def load_data_with_versions(table: Table, versions_deep: int) -> pl.DataFrame:
    snapshots = table.metadata.snapshots
    if not snapshots:
        return pl.DataFrame()
    num_versions = min(versions_deep + 1, len(snapshots))
    selected_snapshots = snapshots[-num_versions:]
    dfs = []
    for i, snap in enumerate(selected_snapshots):
        df = table.scan(snapshot_id=snap.snapshot_id).to_polars()
        df = df.with_columns(pl.lit(len(selected_snapshots) - i - 1).alias("version"))
        dfs.append(df)
    return pl.concat(dfs)

def perform_nlp_analysis(df: pl.DataFrame) -> Dict[str, Any]:
    if 'content' not in df.columns:
        return {"nlp": "No text column found"}
    texts = df['content'].to_list()
    entities = []
    for text in texts:
        doc = nlp(text[:1000])
        entities.append([(ent.text, ent.label_) for ent in doc.ents])
    return {"entities": entities}

def perform_image_analysis(df: pl.DataFrame) -> Dict[str, Any]:
    if 'image_content' not in df.columns:
        return {"images": "No image column found"}
    images = df['image_content'].to_list()
    extracted_texts = []
    for img_bytes in images:
        if img_bytes:
            result = reader.readtext(img_bytes)
            extracted_texts.append([text[1] for text in result])
        else:
            extracted_texts.append([])
    return {"extracted_texts": extracted_texts}

def perform_table_analysis(df: pl.DataFrame) -> Dict[str, Any]:
    return {"stats": df.describe().to_dicts()}

@router.post("/query")
@error_handler(retries=3, exceptions=(Exception,))
def query_data(query: Query, current_user: User = Depends(get_current_user)):
    table = get_table(query.table_name)
    df = load_data_with_versions(table, query.versions_deep)
    if df.is_empty():
        return {"result": []}
    
    db_path = "/tmp/data.duckdb"
    con = duckdb.connect(db_path)
    df.write_parquet("/tmp/data.parquet")
    con.execute("CREATE OR REPLACE TABLE data AS SELECT * FROM '/tmp/data.parquet'")
    db = SQLDatabase.from_uri(f"duckdb:///{db_path}")
    
    llm = Ollama(model="llama3", base_url="http://ollama:11434")
    chain = create_sql_query_chain(llm, db)
    sql_query = chain.invoke({"question": query.question})
    
    if sql_query.startswith("```sql"):
        sql_query = sql_query.strip("```sql\n").strip("```")
    
    result = con.query(sql_query).pl()
    con.close()
    
    return {"result": result.to_dicts()}

@router.post("/analyze")
@error_handler(retries=3, exceptions=(Exception,))
def analyze_data(analyze: Analyze, current_user: User = Depends(get_current_user)):
    table = get_table(analyze.table_name)
    df = load_data_with_versions(table, analyze.versions_deep)
    if df.is_empty():
        return {"analysis": {}}
    
    analysis = {}
    analysis["table"] = perform_table_analysis(df)
    analysis["nlp"] = perform_nlp_analysis(df)
    analysis["images"] = perform_image_analysis(df)
    
    db_path = "/tmp/data.duckdb"
    con = duckdb.connect(db_path)
    df.write_parquet("/tmp/data.parquet")
    con.execute("CREATE OR REPLACE TABLE data AS SELECT * FROM '/tmp/data.parquet'")
    db = SQLDatabase.from_uri(f"duckdb:///{db_path}")
    
    llm = Ollama(model="llama3", base_url="http://ollama:11434")
    chain = create_sql_query_chain(llm, db)
    summary_query = chain.invoke({"question": "Provide a comprehensive summary of the data, including key statistics and insights."})
    if summary_query.startswith("```sql"):
        summary_query = summary_query.strip("```sql\n").strip("```")
    summary_result = con.query(summary_query).pl()
    analysis["summary"] = summary_result.to_dicts()
    
    con.close()
    
    return {"analysis": analysis}