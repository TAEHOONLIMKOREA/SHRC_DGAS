import os
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

class Settings:
    def __init__(self):
        self.MINIO_ENDPOINT = os.environ["MINIO_ENDPOINT"]
        self.MINIO_ACCESS_KEY = os.environ["MINIO_ACCESS_KEY"]
        self.MINIO_SECRET_KEY = os.environ["MINIO_SECRET_KEY"]
        self.MINIO_SECURE = os.environ["MINIO_SECURE"].lower() == "true"
        self.MINIO_BUCKET = os.environ["MINIO_BUCKET"]
          # --- Redis 설정 ---
        self.REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
        self.REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
        self.REDIS_DB = int(os.getenv("REDIS_DB", 0))
        self.REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", None)

        self.time_DB_HOST = os.getenv("time_DB_HOST")
        self.time_DB_PORT = int(os.getenv("time_DB_PORT"))
        self.time_DB_USER = os.getenv("time_DB_USER")
        self.time_DB_PASSWORD = os.getenv("time_DB_PASSWORD")
        self.time_DB_NAME = os.getenv("time_DB_NAME")

        self.time_EXTERNAL_API_KEY = os.getenv("time_EXTERNAL_API_KEY")
settings = Settings()
