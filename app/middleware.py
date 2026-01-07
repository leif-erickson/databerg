# middleware.py
from fastapi import Request
from sqlmodel import Session
from models.metrics import Metric
from database import engine
import time

async def log_metrics(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    with Session(engine) as session:
        metric = Metric(
            endpoint=request.url.path,
            method=request.method,
            response_time=process_time,
            status_code=response.status_code
        )
        session.add(metric)
        session.commit()
    return response