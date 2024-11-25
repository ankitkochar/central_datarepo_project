from pydantic import BaseModel
from typing import Optional


class PromptCRUD(BaseModel):
    operation: str
    data: Optional[object] = {}
    prompt_id: Optional[str] = ""

class InstituteCrud(BaseModel):
    institute_id: int
    embedding_generated : bool
    downloaded: bool
class LatestNews(BaseModel):
    institute_ids: list