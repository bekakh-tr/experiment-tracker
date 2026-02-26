from datetime import date, datetime

from pydantic import BaseModel, Field, field_validator


class SearchRequest(BaseModel):
    gcid: str = Field(min_length=1, max_length=128)
    days: int = Field(default=30, ge=1, le=365)

    @field_validator("gcid")
    @classmethod
    def gcid_must_not_be_blank(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("gcid cannot be empty")
        return value


class ExperimentSummary(BaseModel):
    experiment_id: str
    experiment_name: str
    variants: list[str]


class DailyParticipation(BaseModel):
    day: date
    count: int
    experiments: list[ExperimentSummary]


class SearchResponse(BaseModel):
    gcid: str
    days: int
    generated_at: datetime
    daily: list[DailyParticipation]


class ExperimentDetailsResponse(BaseModel):
    experiment_id: str
    experiment_name: str
    start_date: date | None
    end_date: date | None
    running_days: int
    overlap_experiment_count: int
    variants: list[str]


class HealthResponse(BaseModel):
    status: str
    app_name: str


class ConnectionCheckResponse(BaseModel):
    ok: bool
    message: str
