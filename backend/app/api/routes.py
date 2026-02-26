from fastapi import APIRouter, Depends, HTTPException, Query

from app.config import Settings, get_settings
from app.models import (
    ConnectionCheckResponse,
    ExperimentDetailsResponse,
    HealthResponse,
    SearchRequest,
    SearchResponse,
)
from app.services.databricks_client import DatabricksClient
from app.services.experiment_service import ExperimentService

router = APIRouter(prefix="/api")


def get_service(settings: Settings = Depends(get_settings)) -> ExperimentService:
    return ExperimentService(settings=settings, dbx_client=DatabricksClient(settings))


@router.get("/health", response_model=HealthResponse)
def health(settings: Settings = Depends(get_settings)) -> HealthResponse:
    return HealthResponse(status="ok", app_name=settings.app_name)


@router.post("/search", response_model=SearchResponse)
def search_participation(
    payload: SearchRequest,
    service: ExperimentService = Depends(get_service),
) -> SearchResponse:
    try:
        return service.search_participation(gcid=payload.gcid, days=payload.days)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Search failed: {exc}") from exc


@router.get("/experiments/{experiment_id}", response_model=ExperimentDetailsResponse)
def experiment_details(
    experiment_id: str,
    gcid: str = Query(..., min_length=1),
    days: int = Query(default=30, ge=1, le=365),
    service: ExperimentService = Depends(get_service),
):
    try:
        return service.get_experiment_details(gcid=gcid.strip(), experiment_id=experiment_id, days=days)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Experiment details failed: {exc}") from exc


@router.get("/connection-check", response_model=ConnectionCheckResponse)
def connection_check(service: ExperimentService = Depends(get_service)) -> ConnectionCheckResponse:
    ok, message = service.check_connection()
    return ConnectionCheckResponse(ok=ok, message=message)
