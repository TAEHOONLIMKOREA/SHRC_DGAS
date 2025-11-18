from fastapi import APIRouter
from .schemas import IngestRequest, IngestResponse
from .services import (
    parse_iso_utc, build_object_path,
    fetch_image_bytes, to_jpeg_bytes, put_to_minio,
)
from .telemetry_service import sync_telemetry_range,sync_recent_telemetry,get_last_update_history,run_full_update
from datetime import datetime
from .config import settings
from .redis_config import r
import json
router = APIRouter()

def producer(queue_name: str, object_path: str, image_id: str):
    r.lpush(queue_name, json.dumps({"object_path": object_path,
                                     "imageId": image_id}))
    print(f"Redis 큐에 작업 추가됨 → {queue_name}: {object_path}")

@router.post("/drone/photos", response_model=IngestResponse)
async def ingest_drone_photo(body: IngestRequest):
    try:
        p = body.data

        # 1) 시간 변환
        ts = parse_iso_utc(p.capturedAt)

        # 2) 이미지 가져오고 JPEG로 변환
        raw = await fetch_image_bytes(str(p.photo_Url))
        jpg = to_jpeg_bytes(raw)

        # 3) 파일명/경로 생성
        object_path, filename = build_object_path(p.robot_id, ts)

        # 4) 메타데이터 구성
        meta = {
            "robot_id": p.robot_id,
            "capturedAt": p.capturedAt,
            "latitude": str(p.position.latitude),
            "longitude": str(p.position.longitude),
            "altitude": "" if p.position.altitude is None else str(p.position.altitude),
        }

        # 5) MinIO 업로드
        put_to_minio(jpg, object_path, meta)

        producer("infer_job_queue", object_path, str(p.imageId))

        return IngestResponse(message="success")
    except ValueError:
        return IngestResponse(message="parameter type error")
    except Exception as e:
        print(f"❌ 서버 내부 오류 발생: {e}")
        return IngestResponse(message="server internal error")
    
@router.post("/telemetry/sync")
async def telemetry_sync(robot_id: str, from_ts: str, to_ts: str):
    """
    수동 Telemetry 동기화 API
    - robot_id: 로봇 ID
    - from_ts, to_ts: 'YYYYMMDDhhmmss' 형식
    """
    inserted = await sync_recent_telemetry(robot_id, from_ts, to_ts)
    return {
        "robot_id": robot_id,
        "from": from_ts,
        "to": to_ts,
        "rows_upserted": inserted,
    }
@router.get("/telemetry/update/last")
async def get_last_update():
    """
    가장 최근 telemetry 전체 업데이트 이력 조회
    """
    last = await get_last_update_history()

    if not last:
        return {"last_sync": None}

    return {
        "last_from_ts": last["last_from_ts"],
        "last_to_ts": last["last_to_ts"],
        "updated_at": last["updated_at"],
        "rows_upserted": last["rows_upserted"]
    }

@router.post("/telemetry/update")
async def run_update():
    """
    전체 telemetry 동기화 실행
    - robots 테이블에서 robot_id 자동 조회
    - 최근 업데이트 이력 불러옴
    - sync_recent_telemetry 반복 실행
    - 업데이트 이력 저장
    """
    result = await run_full_update()
    return result