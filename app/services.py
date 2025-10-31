import io
import hashlib
from datetime import datetime, timezone
import httpx
from fastapi import HTTPException
from PIL import Image
from minio import Minio
from minio.error import S3Error
from .config import settings

# --- MinIO 클라이언트 생성 ---
_minio: Minio | None = None

def minio_client() -> Minio:
    global _minio
    if _minio is None:
        _minio = Minio(
            settings.MINIO_ENDPOINT,
            access_key=settings.MINIO_ACCESS_KEY,
            secret_key=settings.MINIO_SECRET_KEY,
            secure=settings.MINIO_SECURE,
        )
        _ensure_bucket(settings.MINIO_BUCKET)
    return _minio

def _ensure_bucket(bucket: str):
    c = minio_client()
    if not c.bucket_exists(bucket):
        c.make_bucket(bucket)

# --- 시간 유틸 ---
def parse_iso_utc(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)

def yyyymmdd(dt: datetime) -> str:
    return dt.strftime("%Y%m%d")

def hhmmss(dt: datetime) -> str:
    return dt.strftime("%H%M%S")

# --- 파일명/경로 ---
def build_object_path(robot_id: str, dt: datetime) -> tuple[str, str]:
    date_str = yyyymmdd(dt)
    time_str = hhmmss(dt)
    filename = f"{date_str}_{time_str}.jpg"
    object_path = f"DRONE/{robot_id}/{date_str}/Image/{filename}"
    return object_path, filename

# --- 이미지 다운로드 ---
async def fetch_image_bytes(url: str) -> bytes:
    timeout = httpx.Timeout(20, connect=10)
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
        r = await client.get(url)
        if r.status_code != 200:
            raise HTTPException(400, f"Failed to fetch image: {r.status_code}")
        return r.content

# --- 이미지 JPEG 변환 ---
def to_jpeg_bytes(raw: bytes) -> bytes:
    try:
        img = Image.open(io.BytesIO(raw))
        rgb = img.convert("RGB")
        out = io.BytesIO()
        rgb.save(out, format="JPEG", quality=90, optimize=True)
        return out.getvalue()
    except Exception as e:
        raise HTTPException(400, f"Invalid image data: {e}")

# --- MinIO 업로드 ---
def put_to_minio(jpg_bytes: bytes, object_path: str, metadata: dict):
    try:
        c = minio_client()
        c.put_object(
            bucket_name=settings.MINIO_BUCKET,
            object_name=object_path,
            data=io.BytesIO(jpg_bytes),
            length=len(jpg_bytes),
            content_type="image/jpeg",
            metadata=metadata,
        )
    except S3Error as e:
        raise HTTPException(500, f"MinIO upload error: {e}")
