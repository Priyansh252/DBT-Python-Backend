from pydantic import BaseModel
from typing import Optional

class BankAccountIn(BaseModel):
    student_id: int
    account_number: str
    bank_name: str

class UpdateAccountStatusIn(BaseModel):
    account_id: int
    aadhaar_linked: Optional[bool] = None
    dbt_enabled: Optional[bool] = None
