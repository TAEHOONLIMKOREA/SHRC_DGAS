from fastapi import FastAPI
from .router import router
from .scheduler import start_scheduler
import logging
from fastapi.middleware.cors import CORSMiddleware
from .telemetry_service import load_uuid_to_num, UUID_TO_NUM  # ← 추가

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


def create_app() -> FastAPI:
    app = FastAPI(title="Drone Photo Ingest API", version="1.0.0")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],            # 또는 ["http://localhost:8080"] 로 제한 가능
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(router, prefix="/v1")

    @app.on_event("startup")
    async def on_startup():
        logging.info("Start APScheduler for telemetry sync")
        # start_scheduler()
        global UUID_TO_NUM
        mapping = await load_uuid_to_num()
        UUID_TO_NUM.clear()
        UUID_TO_NUM.update(mapping)
        logging.info(f"UUID_TO_NUM loaded: {UUID_TO_NUM}")

    return app

app = create_app()
# 실행:
# uvicorn app.main:app --host 0.0.0.0 --port 8000
