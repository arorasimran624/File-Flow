from pydantic import BaseModel
from typing import Literal

class FileUploadRequest(BaseModel):
    file_id: str
    userid: int
    username: str
    role: Literal["admin", "user"]