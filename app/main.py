# main.py
from databerg.app.routers import lakehouse
from fastapi import FastAPI, Depends
from prometheus_fastapi_instrumentator import Instrumentator
from routers import users, enrich, summary
from auth.auth import get_current_user
from middleware import log_metrics

app = FastAPI(title="Data Platform API")

app.include_router(users.router, prefix="/users", tags=["users"])
app.include_router(lakehouse.router, prefix="/lakehouse", tags=["lakehouse"])
app.include_router(enrich.router, prefix="/enrich", tags=["enrich"])
app.include_router(summary.router, prefix="/summary", tags=["summary"])

instrumentator = Instrumentator().instrument(app).expose(app)

app.middleware("http")(log_metrics)

@app.get("/health")
def health():
    return {"status": "healthy"}s