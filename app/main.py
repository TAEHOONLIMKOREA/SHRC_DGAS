from fastapi import FastAPI
from .router import router
from .scheduler import start_scheduler
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


def create_app() -> FastAPI:
    app = FastAPI(title="Drone Photo Ingest API", version="1.0.0")

    app.include_router(router, prefix="/v1")

    @app.on_event("startup")
    async def on_startup():
        logging.info("Start APScheduler for telemetry sync")
        start_scheduler()

    return app

app = create_app()
# 실행:
# uvicorn app.main:app --host 0.0.0.0 --port 8000
