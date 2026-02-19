from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class DatasetSummary(BaseModel):
    dataset_id: int
    filename: str
    total_jobs: int
    top_roles: list[dict[str, Any]]
    top_countries: list[dict[str, Any]]
    skills_source: str
    date_range: dict[str, str | None]
    mapping_confidence: float
    suggested_questions: list[str]


class AgentQueryRequest(BaseModel):
    query: str = Field(..., min_length=3)


class AgentQueryResponse(BaseModel):
    status: str
    message: str
    execution_plan: list[str]
    parsed_intent: str | None
    parsed_filters: dict[str, Any]
    result: dict[str, Any] | None
    confidence: float
    warnings: list[str]
    clarification_questions: list[str] = []


class AgentRunOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    dataset_id: int | None
    query: str
    status: str
    confidence: float
    started_at: datetime
    finished_at: datetime | None
