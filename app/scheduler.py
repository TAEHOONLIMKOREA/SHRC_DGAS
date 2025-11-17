# app/scheduler.py

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from datetime import datetime, timedelta
from .telemetry_service import sync_recent_telemetry

ROBOT_IDS = [
    "01fb056f-a3fb-4c38-9f97-ff11b9dea241",
    "163c4473-d37b-4bec-a293-208a69bd2b0d",
    "f6024fc0-e542-4858-9ff4-f7365ef914de",
    "37e05a23-0c44-4384-b48b-031ce0e33e38",
    "dce87884-1065-41f9-b50c-8f6656a8313e",
]


def _fmt(dt: datetime) -> str:
    return dt.strftime("%Y%m%d%H%M%S")


async def _job():
    now = datetime.utcnow()
    from_dt = now - timedelta(hours=3)

    from_ts = _fmt(from_dt)
    to_ts = _fmt(now)

    print(f"🔁 Telemetry scheduler run: {from_ts} → {to_ts}")

    for rid in ROBOT_IDS:
        try:
            rows = await sync_recent_telemetry(rid, from_ts, to_ts)
            print(f"✅ {rid}: {rows} rows upserted")
        except Exception as e:
            print(f"❌ {rid} sync error: {e}")


def start_scheduler():
    scheduler = AsyncIOScheduler()
    scheduler.add_job(_job, "interval", hours=3)
    scheduler.start()
