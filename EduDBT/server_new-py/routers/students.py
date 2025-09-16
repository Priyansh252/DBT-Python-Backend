from fastapi import APIRouter, HTTPException, Depends, status
import asyncpg
from typing import List

from core.db import get_db_connection
from models.student import StudentIn, StudentOut, UpdateStudentIn

router = APIRouter(prefix="/students", tags=["Students"])

@router.post("/", response_model=StudentOut, status_code=status.HTTP_201_CREATED)
async def insert_student(payload: StudentIn, db_pool: asyncpg.Pool = Depends(get_db_connection)):
    q = """INSERT INTO Students (name,email,phone,state,college)
           VALUES ($1,$2,$3,$4,$5)
           RETURNING student_id,name,email,phone,state,college"""
    try:
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(q, payload.name, payload.email, payload.phone, payload.state, payload.college)
            return dict(row)
    except asyncpg.exceptions.UniqueViolationError:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Email or phone already exists")

@router.get("/", response_model=List[StudentOut])
async def show_students(db_pool: asyncpg.Pool = Depends(get_db_connection)):
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM Students ORDER BY student_id")
        return [dict(r) for r in rows]

@router.get("/pending-dbt")
async def show_pending_dbt(db_pool: asyncpg.Pool = Depends(get_db_connection)):
    q = """
    SELECT DISTINCT s.*
    FROM Students s
    LEFT JOIN BankAccounts ba ON s.student_id=ba.student_id
    LEFT JOIN AccountStatus asu ON ba.account_id=asu.account_id
    WHERE COALESCE(asu.dbt_enabled,false)=false
    """
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(q)
        return [dict(r) for r in rows]

@router.put("/{student_id}")
async def update_student(student_id: int, payload: UpdateStudentIn, db_pool: asyncpg.Pool = Depends(get_db_connection)):
    q = """UPDATE Students SET name=$1,email=$2,phone=$3,state=$4,college=$5 WHERE student_id=$6"""
    async with db_pool.acquire() as conn:
        result = await conn.execute(q, payload.name, payload.email, payload.phone, payload.state, payload.college, student_id)
        if result == "UPDATE 0":
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Student not found")
        return {"status": "updated"}

@router.delete("/{student_id}")
async def delete_student(student_id: int, db_pool: asyncpg.Pool = Depends(get_db_connection)):
    async with db_pool.acquire() as conn:
        res = await conn.execute("DELETE FROM Students WHERE student_id=$1", student_id)
        if res == "DELETE 0":
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Student not found")
        return {"status": "deleted"}
