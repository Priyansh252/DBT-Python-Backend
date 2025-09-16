from fastapi import APIRouter, HTTPException, Depends, status
import asyncpg

from core.db import get_db_connection
from models.bank_account import BankAccountIn, UpdateAccountStatusIn

router = APIRouter(prefix="/bank-accounts", tags=["Bank Accounts"])

@router.post("/", status_code=status.HTTP_201_CREATED)
async def insert_bank_account(payload: BankAccountIn, db_pool: asyncpg.Pool = Depends(get_db_connection)):
    async with db_pool.acquire() as conn:
        try:
            async with conn.transaction():
                acc = await conn.fetchrow(
                    "INSERT INTO BankAccounts (student_id,account_number,bank_name) VALUES ($1,$2,$3) RETURNING account_id",
                    payload.student_id, payload.account_number, payload.bank_name
                )
                await conn.execute("INSERT INTO AccountStatus (account_id) VALUES ($1)", acc["account_id"])
            return {"account_id": acc["account_id"]}
        except asyncpg.exceptions.ForeignKeyViolationError:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Student ID does not exist")
        except asyncpg.exceptions.UniqueViolationError:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Account number already exists")

@router.get("/")
async def show_bank_accounts(db_pool: asyncpg.Pool = Depends(get_db_connection)):
    q = """
    SELECT ba.account_id, ba.account_number, ba.bank_name, s.student_id, s.name,
           COALESCE(asu.aadhaar_linked,false) AS aadhaar_linked,
           COALESCE(asu.dbt_enabled,false) AS dbt_enabled,
           asu.last_updated
    FROM BankAccounts ba
    JOIN Students s ON ba.student_id=s.student_id
    LEFT JOIN AccountStatus asu ON ba.account_id=asu.account_id
    """
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(q)
        return [dict(r) for r in rows]

@router.put("/account-status")
async def update_account_status(payload: UpdateAccountStatusIn, db_pool: asyncpg.Pool = Depends(get_db_connection)):
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow("SELECT * FROM AccountStatus WHERE account_id=$1", payload.account_id)
            if not row:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Account not found")
            new_aad = payload.aadhaar_linked if payload.aadhaar_linked is not None else row["aadhaar_linked"]
            new_dbt = payload.dbt_enabled if payload.dbt_enabled is not None else row["dbt_enabled"]
            await conn.execute(
                "INSERT INTO AccountStatusHistory (account_id,aadhaar_linked,dbt_enabled) VALUES ($1,$2,$3)",
                payload.account_id, new_aad, new_dbt
            )
            await conn.execute(
                "UPDATE AccountStatus SET aadhaar_linked=$1, dbt_enabled=$2,last_updated=CURRENT_TIMESTAMP WHERE account_id=$3",
                new_aad, new_dbt, payload.account_id
            )
    return {"status": "ok"}
