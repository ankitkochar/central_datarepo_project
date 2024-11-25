from pydantic import BaseModel
from typing import Optional


class InstituteIds(BaseModel):
    institute_ids: list
    force: Optional[bool] = False
    index: Optional[str] = "sentence"
    course: Optional[str] = None
    url: Optional[str] = None
    llm: Optional[str] = "4o-mini"
    enable_javascript: Optional[bool] = True
