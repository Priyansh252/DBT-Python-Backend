import asyncio
import asyncpg
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, EmailStr
from typing import Optional, List
import logging
from contextlib import asynccontextmanager

# ----------------- CONFIG -----------------
DB_NAME = "DBTDatabase"
DB_HOST = "localhost"
DB_USER = "postgres"
DB_PASS = "root"
DB_PORT = 5432

# ----------------- LOGGING -----------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ----------------- GLOBALS -----------------
pg_pool: Optional[asyncpg.pool.Pool] = None

# ----------------- MODELS -----------------
class StudentIn(BaseModel):
    name: str
    email: Optional[EmailStr]
    phone: Optional[str]
    state: str
    college: Optional[str]

class StudentOut(StudentIn):
    student_id: int

class BankAccountIn(BaseModel):
    student_id: int
    account_number: str
    bank_name: str

class SchemeIn(BaseModel):
    scheme_name: str
    department: Optional[str]

class SchemeOut(SchemeIn):
    scheme_id: int

class UpdateStudentIn(StudentIn):
    pass

class UpdateAccountStatusIn(BaseModel):
    account_id: int
    aadhaar_linked: Optional[bool] = None
    dbt_enabled: Optional[bool] = None

class AwarenessIn(BaseModel):
    title: str
    content: Optional[str]

# ----------------- LIFESPAN -----------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    global pg_pool
    dsn = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    for i in range(3):  # retry
        try:
            pg_pool = await asyncpg.create_pool(dsn, min_size=1, max_size=10)
            logger.info("Connected to Postgres")
            break
        except Exception as e:
            logger.error(f"DB connection failed: {e}")
            await asyncio.sleep(2 ** i)
    if not pg_pool:
        raise RuntimeError("Failed to connect to DB")

    yield  # app runs here

    if pg_pool:
        await pg_pool.close()
        logger.info("DB pool closed")

# ----------------- FASTAPI APP -----------------
app = FastAPI(title="DBT Backend API", version="1.0.0", lifespan=lifespan)

# ----------------- ENDPOINTS -----------------

# 1. Insert Student
@app.post("/students", response_model=StudentOut, status_code=201)
async def insert_student(payload: StudentIn):
    q = """INSERT INTO Students (name,email,phone,state,college)
           VALUES ($1,$2,$3,$4,$5)
           RETURNING student_id,name,email,phone,state,college"""
    try:
        async with pg_pool.acquire() as conn:
            row = await conn.fetchrow(q, payload.name, payload.email, payload.phone, payload.state, payload.college)
            return dict(row)
    except asyncpg.exceptions.UniqueViolationError:
        raise HTTPException(409, "Email or phone already exists")

# 2. Insert Bank Account
@app.post("/bank-accounts", status_code=201)
async def insert_bank_account(payload: BankAccountIn):
    async with pg_pool.acquire() as conn:
        try:
            async with conn.transaction():
                acc = await conn.fetchrow(
                    "INSERT INTO BankAccounts (student_id,account_number,bank_name) VALUES ($1,$2,$3) RETURNING account_id",
                    payload.student_id, payload.account_number, payload.bank_name
                )
                await conn.execute("INSERT INTO AccountStatus (account_id) VALUES ($1)", acc["account_id"])
            return {"account_id": acc["account_id"]}
        except asyncpg.exceptions.ForeignKeyViolationError:
            raise HTTPException(400, "Student ID does not exist")
        except asyncpg.exceptions.UniqueViolationError:
            raise HTTPException(409, "Account number already exists")

# 3. Insert Scheme
@app.post("/schemes", response_model=SchemeOut, status_code=201)
async def insert_scheme(payload: SchemeIn):
    q = "INSERT INTO Schemes (scheme_name,department) VALUES ($1,$2) RETURNING scheme_id,scheme_name,department"
    async with pg_pool.acquire() as conn:
        row = await conn.fetchrow(q, payload.scheme_name, payload.department)
        return dict(row)

# 4. Show Students
@app.get("/students", response_model=List[StudentOut])
async def show_students():
    async with pg_pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM Students ORDER BY student_id")
        return [dict(r) for r in rows]

# 5. Show Bank Accounts
@app.get("/bank-accounts")
async def show_bank_accounts():
    q = """
    SELECT ba.account_id, ba.account_number, ba.bank_name, s.student_id, s.name,
           COALESCE(asu.aadhaar_linked,false) AS aadhaar_linked,
           COALESCE(asu.dbt_enabled,false) AS dbt_enabled,
           asu.last_updated
    FROM BankAccounts ba
    JOIN Students s ON ba.student_id=s.student_id
    LEFT JOIN AccountStatus asu ON ba.account_id=asu.account_id
    """
    async with pg_pool.acquire() as conn:
        rows = await conn.fetch(q)
        return [dict(r) for r in rows]

# 6. Show Students Pending DBT
@app.get("/students/pending-dbt")
async def show_pending_dbt():
    q = """
    SELECT DISTINCT s.*
    FROM Students s
    LEFT JOIN BankAccounts ba ON s.student_id=ba.student_id
    LEFT JOIN AccountStatus asu ON ba.account_id=asu.account_id
    WHERE COALESCE(asu.dbt_enabled,false)=false
    """
    async with pg_pool.acquire() as conn:
        rows = await conn.fetch(q)
        return [dict(r) for r in rows]

# 7. Update Student
@app.put("/students/{student_id}")
async def update_student(student_id: int, payload: UpdateStudentIn):
    q = """UPDATE Students SET name=$1,email=$2,phone=$3,state=$4,college=$5 WHERE student_id=$6"""
    async with pg_pool.acquire() as conn:
        result = await conn.execute(q, payload.name, payload.email, payload.phone, payload.state, payload.college, student_id)
        if result == "UPDATE 0":
            raise HTTPException(404, "Student not found")
        return {"status": "updated"}

# 8. Update Account Status
@app.put("/account-status")
async def update_account_status(payload: UpdateAccountStatusIn):
    async with pg_pool.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow("SELECT * FROM AccountStatus WHERE account_id=$1", payload.account_id)
            if not row:
                raise HTTPException(404, "Account not found")
            new_aad = payload.aadhaar_linked if payload.aadhaar_linked is not None else row["aadhaar_linked"]
            new_dbt = payload.dbt_enabled if payload.dbt_enabled is not None else row["dbt_enabled"]
            await conn.execute(
                "INSERT INTO AccountStatusHistory (account_id,aadhaar_linked,dbt_enabled) VALUES ($1,$2,$3)",
                payload.account_id, new_aad, new_dbt
            )
            await conn.execute(
                "UPDATE AccountStatus SET aadhaar_linked=$1, dbt_enabled=$2,last_updated=CURRENT_TIMESTAMP WHERE account_id=$3",
                new_aad,new_dbt,payload.account_id
            )
    return {"status":"ok"}

# 9. Update Scheme
@app.put("/schemes/{scheme_id}")
async def update_scheme(scheme_id:int,payload:SchemeIn):
    q="UPDATE Schemes SET scheme_name=$1,department=$2 WHERE scheme_id=$3"
    async with pg_pool.acquire() as conn:
        res=await conn.execute(q,payload.scheme_name,payload.department,scheme_id)
        if res=="UPDATE 0":
            raise HTTPException(404,"Scheme not found")
    return {"status":"ok"}

# 10. Update Awareness Content
@app.put("/awareness/{content_id}")
async def update_awareness(content_id:int,payload:AwarenessIn):
    q="UPDATE AwarenessContent SET title=$1,content=$2 WHERE content_id=$3"
    async with pg_pool.acquire() as conn:
        res=await conn.execute(q,payload.title,payload.content,content_id)
        if res=="UPDATE 0":
            raise HTTPException(404,"Content not found")
    return {"status":"ok"}

# 11. Delete Student
@app.delete("/students/{student_id}")
async def delete_student(student_id:int):
    async with pg_pool.acquire() as conn:
        res=await conn.execute("DELETE FROM Students WHERE student_id=$1",student_id)
        if res=="DELETE 0":
            raise HTTPException(404,"Student not found")
    return {"status":"deleted"}

# 12. Delete Scheme
@app.delete("/schemes/{scheme_id}")
async def delete_scheme(scheme_id:int):
    async with pg_pool.acquire() as conn:
        res=await conn.execute("DELETE FROM Schemes WHERE scheme_id=$1",scheme_id)
        if res=="DELETE 0":
            raise HTTPException(404,"Scheme not found")
    return {"status":"deleted"}

# 13. Delete Awareness Content
@app.delete("/awareness/{content_id}")
async def delete_awareness(content_id:int):
    async with pg_pool.acquire() as conn:
        res=await conn.execute("DELETE FROM AwarenessContent WHERE content_id=$1",content_id)
        if res=="DELETE 0":
            raise HTTPException(404,"Content not found")
    return {"status":"deleted"}

# Health
@app.get("/health")
async def health():
    try:
        async with pg_pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        return {"status":"ok"}
    except:
        raise HTTPException(503,"DB not available")
