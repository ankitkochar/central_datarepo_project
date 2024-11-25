from pydantic import BaseModel
from typing import Optional


class queryResult(BaseModel):
    prompt: str
    institute_id: int
    llm: Optional[str] = "4o-mini"
    sys_prompt: Optional[str] = None
    answer_format: Optional[str] = "Concise and accurate answer"
    index: Optional[str] = "sentence"
    search_terms: Optional[str] = None
    response_type: Optional[str] = "text"


class DegreeData(BaseModel):
    institute_ids: list
