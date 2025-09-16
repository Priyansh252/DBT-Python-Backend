from pydantic import BaseModel, EmailStr
from typing import Optional

class StudentIn(BaseModel):
    name: str
    email: Optional[EmailStr]
    phone: Optional[str]
    state: str
    college: Optional[str]

class StudentOut(StudentIn):
    student_id: int

class UpdateStudentIn(StudentIn):
    pass
