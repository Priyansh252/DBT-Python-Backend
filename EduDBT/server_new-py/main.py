from fastapi import FastAPI, HTTPException, Depends, status
import asyncpg
from core.db import lifespan, get_db_connection
from routers import students, bank_accounts

app = FastAPI(title="DBT Backend API", version="1.0.0", lifespan=lifespan)

app.include_router(students.router)
app.include_router(bank_accounts.router)

@app.get("/health")
async def health(db_pool: asyncpg.Pool = Depends(get_db_connection)):
    try:
        async with db_pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        return {"status": "ok"}
    except:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="DB not available")
