import asyncio
import asyncpg
from contextlib import asynccontextmanager
import logging
from typing import Optional
from fastapi import HTTPException

from core.config import DB_NAME, DB_HOST, DB_USER, DB_PASS, DB_PORT

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

pg_pool: Optional[asyncpg.pool.Pool] = None

@asynccontextmanager
async def lifespan(app):
    global pg_pool
    dsn = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    for i in range(3):
        try:
            pg_pool = await asyncpg.create_pool(dsn, min_size=1, max_size=10)
            logger.info("Connected to Postgres")
            break
        except Exception as e:
            logger.error(f"DB connection failed: {e}")
            await asyncio.sleep(2 ** i)
    if not pg_pool:
        raise RuntimeError("Failed to connect to DB")

    yield

    if pg_pool:
        await pg_pool.close()
        logger.info("DB pool closed")

async def get_db_connection():
    if pg_pool:
        return pg_pool
    raise HTTPException(503, "Database connection not available")
