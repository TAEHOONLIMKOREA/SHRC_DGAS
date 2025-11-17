# app/database.py

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from .config import settings

DATABASE_URL = (
    f"postgresql+asyncpg://{settings.time_DB_USER}:{settings.time_DB_PASSWORD}"
    f"@{settings.time_DB_HOST}:{settings.time_DB_PORT}/{settings.time_DB_NAME}"
)

engine = create_async_engine(
    DATABASE_URL,
    echo=False,
    future=True,
    pool_pre_ping=True,
    pool_size=10,
    max_overflow=20,
)

async_session = sessionmaker(
    bind=engine,
    class_=AsyncSession,
    autoflush=False,
    autocommit=False,
    expire_on_commit=False,
)


async def get_session():
    async with async_session() as session:
        yield session
