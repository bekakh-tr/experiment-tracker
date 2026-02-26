from app.config import get_settings
from app.services.databricks_client import DatabricksClient
from app.services.experiment_service import ExperimentService


def main() -> None:
    settings = get_settings()
    service = ExperimentService(settings=settings, dbx_client=DatabricksClient(settings))
    ok, message = service.check_connection()
    print(f"ok={ok} message='{message}'")
    if not ok:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
