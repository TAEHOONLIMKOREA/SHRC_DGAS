from pydantic import BaseModel, Field, HttpUrl, field_validator
from uuid import UUID
from datetime import datetime

# ---------------------------
# 📍 요청 스키마
# ---------------------------
class Position(BaseModel):
    latitude: float = Field(..., ge=-90, le=90)
    longitude: float = Field(..., ge=-180, le=180)
    altitude: float | None = None  # Python 3.10 이상 사용 가능

class DataPayload(BaseModel):
    robot_id: str = Field(..., min_length=1)
    imageId: UUID = Field(..., description="이미지 고유 식별자(UUID)")
    photo_Url: HttpUrl
    position: Position
    capturedAt: str  # ISO8601 형식 (예: "2025-08-05T05:42:33.390Z")

    @field_validator("capturedAt")
    @classmethod
    def _validate_ts(cls, v: str) -> str:
        try:
            datetime.fromisoformat(v.replace("Z", "+00:00"))
        except ValueError:
            raise ValueError("capturedAt must be ISO 8601 (e.g., 2025-08-05T05:42:33.390Z)")
        return v

class IngestRequest(BaseModel):
    data: DataPayload

# ---------------------------
# ✅ 응답 스키마
# ---------------------------
class IngestResponse(BaseModel):
    message: str = Field(..., description="응답 메시지 (success / parameter type error / server internal error)")
