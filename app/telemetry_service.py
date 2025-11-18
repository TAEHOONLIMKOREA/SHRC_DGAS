import asyncio
import httpx
import json
from datetime import datetime, timedelta, timezone, date
from typing import Any, Dict, List
from datetime import datetime
from dateutil import parser
from sqlalchemy import text
from .database import async_session
from .config import settings
import orjson
import logging
from collections import defaultdict
from app.database import engine
import asyncpg
import time

logger = logging.getLogger(__name__) 

API_BASE = "https://api.m1ucs.com"

HEADERS = {
    "X-Api-Key": settings.time_EXTERNAL_API_KEY,
    "User-Agent": "Mozilla/5.0 (compatible; SHRC/1.0; +httpx)"
}


client_list = httpx.AsyncClient(
    headers=HEADERS,
    timeout=60.0,
    follow_redirects=False
)

client_detail = httpx.AsyncClient(
    headers=HEADERS,
    timeout=120.0,
    follow_redirects=False,
    verify=False
)



async def get_asyncpg_connection():
    """
    SQLAlchemy 엔진에서 asyncpg 원본 connection을 추출
    """
    async with engine.begin() as conn:
        raw = await conn.get_raw_connection()
        return raw.driver_connection


# ---------------------------------------------------------
# msgId → 테이블 매핑
# ---------------------------------------------------------
MSG_TABLE_MAP = {
    24: "gps_raw_int_24",
    141: "altitude_141",
    147: "battery_status_147",
    1101: "unknown_1101",
    # 필요한 msgId 여기에 계속 추가
}

# ---------------------------------------------------------
# 날짜 처리 함수
# ---------------------------------------------------------
def _parse_ts(ts: str) -> datetime:
    return datetime.strptime(ts, "%Y%m%d%H%M%S")


def _parse_ts_to_date(ts: str) -> date:
    return _parse_ts(ts).date()


def _iter_dates(start: date, end: date):
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)


# ---------------------------------------------------------
# JSON payload → DB 컬럼 flatten
# ---------------------------------------------------------
def flatten_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    JSON payload의 키 변환:
      - "voltages[0]" → voltages_0
    """
    row = {}

    for key, value in payload.items():
        if "[" in key and "]" in key:
            base = key.split("[")[0]
            idx = key.split("[")[1].replace("]", "")
            new_key = f"{base}_{idx}"
            row[new_key] = value
        else:
            row[key] = value

    return row


# ---------------------------------------------------------
# 메시지 목록 조회
# ---------------------------------------------------------
async def fetch_message_list(robot_id: str, from_ts: str, to_ts: str):
    url = f"{API_BASE}/ext/robots/{robot_id}/telemetries"
    params = {"from": from_ts, "to": to_ts}

    res = await client_list.get(url, params=params)

    if res.status_code == 302:
        raise ValueError("🚫 302 Redirect → 서버가 요청을 차단했습니다.")

    res.raise_for_status()
    data = orjson.loads(res.content)
    return data if isinstance(data, list) else [data]


# ---------------------------------------------------------
# 메시지 상세 조회
# ---------------------------------------------------------
async def fetch_message_detail(robot_id: str, msg_id: int, from_ts: str, to_ts: str):
    url = f"{API_BASE}/ext/robots/{robot_id}/telemetries/{msg_id}"
    params = {"from": from_ts, "to": to_ts}

    res = await client_detail.get(url, params=params)

    if res.status_code == 302:
        raise ValueError("🚫 상세 조회에서 302 Redirect 발생")

    res.raise_for_status()
    return orjson.loads(res.content)


UUID_TO_NUM = {
    "01fb056f-a3fb-4c38-9f97-ff11b9dea241": 1,
    "163c4473-d37b-4bec-a293-208a69bd2b0d": 2,
    "f6024fc0-e542-4858-9ff4-f7365ef914de": 3,
    "37e05a23-0c44-4384-b48b-031ce0e33e38": 4,
    "dce87884-1065-41f9-b50c-8f6656a8313e": 5,
}
# ---------------------------------------------------------
# Timescale Hypertable 저장
# ---------------------------------------------------------
async def save_message_to_table(table: str, robot_id: str, payload: Dict[str, Any]):
    """
    shrc.{table} 에 insert
    - payload['time'] 는 ISO8601 문자열 (Z 포함)
    - robot_id 는 FK
    """

    async with async_session() as session:

        cols = ["time", "robot_id"] + [
            key for key in payload.keys() if key != "time"
        ]

        raw_time = payload["time"]

        dt = parser.isoparse(raw_time)


        mapped_robot_id = UUID_TO_NUM.get(robot_id)
        if mapped_robot_id is None:
            raise ValueError(f"Unknown robot_id: {robot_id}")
        
        values = {
            "time": dt,
            "robot_id": mapped_robot_id
        }

        for key, value in payload.items():
            if key != "time":
                values[key] = value

        columns_sql = ", ".join(cols)
        placeholders = ", ".join([f":{c}" for c in cols])

        sql = f"""
        INSERT INTO shrc.{table} ({columns_sql})
        VALUES ({placeholders});
        """
        print(sql)
        await session.execute(text(sql), values)
        await session.commit()

async def save_batch_copy_preprocessed(table: str, rows: list[dict], robot_id: str):

    if not rows:
        return

    batch_columns = None
    batch_values = []   # tuple 형태로 적용해야 binary COPY 됨

    # ------------------------
    # 1) 기존 전처리와 동일하게 처리
    # ------------------------
    for payload in rows:
        cols = ["time", "robot_id"] + [key.lower() for key in payload.keys() if key != "time"]

        if batch_columns is None:
            batch_columns = cols

        dt = parser.isoparse(payload["time"])

        mapped_robot_id = UUID_TO_NUM.get(robot_id)
        if mapped_robot_id is None:
            raise ValueError(f"Unknown robot_id: {robot_id}")

        row_values = [dt, mapped_robot_id]

        for key, value in payload.items():
            if key != "time":
                row_values.append(value)

        batch_values.append(tuple(row_values))  # <-- tuple 이어야 함

    # ------------------------
    # 2) SQLAlchemy raw → asyncpg connection
    # ------------------------
    async with engine.begin() as conn:
        raw = await conn.get_raw_connection()
        asyncpg_conn = raw.driver_connection

        await asyncpg_conn.copy_records_to_table(
        table_name=table,               # "gps_raw_int_24"
        schema_name="shrc",             # ← 스키마를 여기에 분리해서 적어야 함
        records=batch_values,
        columns=batch_columns)


# ---------------------------------------------------------
# 전체 데이터 sync (일자 단위)
# ---------------------------------------------------------
async def sync_telemetry_range(robot_id: str, from_ts: str, to_ts: str) -> int:

    start_date = _parse_ts_to_date(from_ts)
    end_date   = _parse_ts_to_date(to_ts)

    total = 0

    for day in _iter_dates(start_date, end_date):
        day_from = day.strftime("%Y%m%d000000")
        day_to   = day.strftime("%Y%m%d235959")

        print(f"\n===== {day} 목록 조회 =====")

        msg_list = await fetch_message_list(robot_id, day_from, day_to)

        for item in msg_list:
            msg_id = item.get("msgId")
            msg_name = item.get("msgName")

            if msg_id not in MSG_TABLE_MAP:
                print(f"⚠ msgId={msg_id} ({msg_name}) → 저장 테이블 없음")
                continue

            print(f" → 상세 조회 msgId={msg_id} ({msg_name})")

            detail = await fetch_message_detail(robot_id, msg_id, day_from, day_to)

            # 🔥 리스트면 전체 저장, 단일 객체면 하나만 저장
            detail_list = detail if isinstance(detail, list) else [detail]

            table = MSG_TABLE_MAP[msg_id]

            for payload in detail_list:
                print(payload)

                flat = flatten_payload(payload)
                print(flat)

                await save_message_to_table(table, robot_id, flat)
                total += 1

    return total


async def sync_recent_telemetry(robot_id: str, from_ts: str, to_ts: str) -> int:
    start_time = time.time()
    logger.info(f"[SYNC START] robot_id={robot_id}, range={from_ts} → {to_ts}")

    total = 0
    msg_list = await fetch_message_list(robot_id, from_ts, to_ts)
    logger.info(f"[MSG LIST] robot_id={robot_id}, count={len(msg_list)} received")

    tasks = []
    msg_ids = []

    for item in msg_list:
        msg_id = item.get("msgId")
        if msg_id not in MSG_TABLE_MAP:
            logger.debug(f"[SKIP] msgId={msg_id} (not in MSG_TABLE_MAP)")
            continue

        tasks.append(fetch_message_detail(robot_id, msg_id, from_ts, to_ts))
        msg_ids.append(msg_id)

    if not tasks:
        logger.warning(f"[NO VALID DATA] robot_id={robot_id} - No messages to process")
        return 0

    logger.info(f"[DETAIL REQUEST] total={len(tasks)} messages, starting async fetch")

    # 병렬 실행
    try:
        details = await asyncio.gather(*tasks)
    except Exception as e:
        logger.error(f"[DETAIL FETCH ERROR] robot_id={robot_id}, error={e}", exc_info=True)
        raise

    # -------------------------------------------------------
    # 2) 테이블별로 payload를 버퍼링
    # -------------------------------------------------------
    # buffer[table_name] = [payload, payload, ...]
    buffer = defaultdict(list)

    for msg_id, detail in zip(msg_ids, details):
        table = MSG_TABLE_MAP[msg_id]

        detail_list = detail if isinstance(detail, list) else [detail]

        logger.debug(f"[PROCESS] msgId={msg_id}, rows={len(detail_list)}")

        for payload in detail_list:
            flat = flatten_payload(payload)  # 기존 flatten 유지
            buffer[table].append(flat)
            total += 1

    # -------------------------------------------------------
    # 3) COPY 기반 대량 insert 수행
    # -------------------------------------------------------
    for table, rows in buffer.items():
        logger.info(f"[COPY INSERT] table={table}, rows={len(rows)}")
        try:
            await save_batch_copy_preprocessed(table, rows, robot_id)
            logger.info(f"[COPY SUCCESS] table={table}, rows={len(rows)}")
        except Exception as e:
            logger.error(
                f"[COPY ERROR] table={table}, rows={len(rows)}, error={e}",
                exc_info=True
            )
            raise
        
    elapsed = time.time() - start_time
    logger.info(f"[SYNC DONE] robot_id={robot_id}, total_rows={total}, elapsed={elapsed:.2f}s")       

    return total


# ---------------------------------------------------------
# 최근 업데이트 이력 조회
# ---------------------------------------------------------
async def get_last_update_history():
    query = """
        SELECT last_from_ts, last_to_ts, rows_upserted, updated_at
        FROM shrc.telemetry_update_history
        ORDER BY updated_at DESC
        LIMIT 1
    """

    async with async_session() as session:
        result = await session.execute(text(query))
        row = result.fetchone()

        if not row:
            return None

        return {
            "last_from_ts": row[0],
            "last_to_ts": row[1],
            "rows_upserted": row[2],
            "updated_at": row[3],
        }

# ---------------------------------------------------------
# 업데이트 이력 저장
# ---------------------------------------------------------
async def save_update_history(from_ts: str, to_ts: str, rows_upserted: int):
    query = """
        INSERT INTO shrc.telemetry_update_history
        (last_from_ts, last_to_ts, rows_upserted, updated_at)
        VALUES (:from_ts, :to_ts, :rows_upserted, NOW())
    """

    async with async_session() as session:
        await session.execute(
            text(query),
            {
                "from_ts": from_ts,
                "to_ts": to_ts,
                "rows_upserted": rows_upserted,
            },
        )
        await session.commit()

async def get_robot_ids():
    """
    robots 테이블에서 모든 robot_id 리스트 조회
    """
    query = """
        SELECT robot_id
        FROM shrc.robots
    """

    async with async_session() as session:
        result = await session.execute(text(query))
        rows = result.fetchall()

        # rows = [('abc123',), ('def456',)...] 이므로 첫 번째 컬럼만 추출
        return [row[0] for row in rows]
    
# ---------------------------------------------------------
# 전체 로봇 telemetry 업데이트 실행
# ---------------------------------------------------------
async def run_full_update():
    """
    전체 로봇 업데이트 실행:
    1. 최근 기록 가져오기 (없으면 초기 from_ts 사용)
    2. 각 로봇에 대해 telemetry_sync 호출
    3. 이력 저장
    """
    robot_list = await get_robot_ids()
    logger.info(f"[UPDATE] 로봇 목록 조회: {robot_list}")

    last = await get_last_update_history()

    # 초기값: 시스템 처음 운영할 때
    from_ts = last["last_to_ts"] if last else datetime.now().strftime("%Y%m%d%H%M%S")
    to_ts = datetime.now().strftime("%Y%m%d%H%M%S")

    total_rows = 0

    for robot_id in robot_list:
        rows = await sync_recent_telemetry(robot_id, from_ts, to_ts)
        total_rows += rows

    # 저장
    await save_update_history(from_ts, to_ts, total_rows)

    return {
        "from_ts": from_ts,
        "to_ts": to_ts,
        "rows_upserted": total_rows
    }
