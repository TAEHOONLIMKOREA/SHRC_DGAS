from fastapi import FastAPI
from .router import router

def create_app() -> FastAPI:
    app = FastAPI(title="Drone Photo Ingest API", version="1.0.0")
    app.include_router(router, prefix="/v1")
    return app

app = create_app()

# 실행:
# uvicorn app.main:app --host 0.0.0.0 --port 8000
