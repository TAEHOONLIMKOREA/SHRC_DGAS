from fastapi import APIRouter
from .schemas import IngestRequest, IngestResponse
from .services import (
    parse_iso_utc, build_object_path,
    fetch_image_bytes, to_jpeg_bytes, put_to_minio,
)
from .config import settings

router = APIRouter()

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

        return IngestResponse(message="success")
    except ValueError:
        return IngestResponse(message="parameter type error")
    except Exception:
        return IngestResponse(message="server internal error")