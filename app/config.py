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

settings = Settings()
