from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

from app.config import Settings
from app.models import DailyParticipation, ExperimentDetailsResponse, ExperimentSummary, SearchResponse
from app.services.databricks_client import DatabricksClient, validate_identifier, validate_table_name


class ExperimentService:
    def __init__(self, settings: Settings, dbx_client: DatabricksClient):
        self.settings = settings
        self.dbx_client = dbx_client

    def _column_map(self) -> dict[str, str]:
        return {
            "table": validate_table_name(self.settings.dbx_table),
            "gcid": validate_identifier(self.settings.dbx_gcid_column),
            "event_ts": validate_identifier(self.settings.dbx_event_ts_column),
            "exp_id": validate_identifier(self.settings.dbx_experiment_id_column),
            "exp_name": validate_identifier(self.settings.dbx_experiment_name_column),
            "variant": validate_identifier(self.settings.dbx_variant_column),
        }

    def search_participation(self, gcid: str, days: int) -> SearchResponse:
        columns = self._column_map()

        query = f"""
            SELECT
                CAST({columns["event_ts"]} AS DATE) AS event_day,
                CAST({columns["exp_id"]} AS STRING) AS experiment_id,
                CAST({columns["exp_name"]} AS STRING) AS experiment_name,
                CAST({columns["variant"]} AS STRING) AS variant
            FROM {columns["table"]}
            WHERE {columns["gcid"]} = :gcid
              AND {columns["event_ts"]} >= date_sub(current_date(), :days)
            ORDER BY event_day ASC
        """
        rows = self.dbx_client.run_query(query, {"gcid": gcid, "days": days})
        grouped: dict[Any, dict[str, Any]] = defaultdict(lambda: {"experiments": {}})

        for row in rows:
            day = row["event_day"]
            exp_key = row["experiment_id"]
            variant = row.get("variant")

            if exp_key not in grouped[day]["experiments"]:
                grouped[day]["experiments"][exp_key] = {
                    "experiment_name": row["experiment_name"],
                    "variants": set(),
                }
            if variant:
                grouped[day]["experiments"][exp_key]["variants"].add(variant)

        daily: list[DailyParticipation] = []
        for day in sorted(grouped.keys()):
            experiments: list[ExperimentSummary] = []
            for experiment_id, details in sorted(grouped[day]["experiments"].items()):
                experiments.append(
                    ExperimentSummary(
                        experiment_id=experiment_id,
                        experiment_name=details["experiment_name"],
                        variants=sorted(details["variants"]),
                    )
                )

            daily.append(
                DailyParticipation(
                    day=day,
                    count=len(experiments),
                    experiments=experiments,
                )
            )

        return SearchResponse(
            gcid=gcid,
            days=days,
            generated_at=datetime.now(timezone.utc),
            daily=daily,
        )

    def get_experiment_details(self, gcid: str, experiment_id: str, days: int) -> ExperimentDetailsResponse:
        columns = self._column_map()

        details_query = f"""
            WITH target AS (
                SELECT
                    CAST({columns["event_ts"]} AS DATE) AS event_day,
                    CAST({columns["exp_id"]} AS STRING) AS experiment_id,
                    CAST({columns["exp_name"]} AS STRING) AS experiment_name,
                    CAST({columns["variant"]} AS STRING) AS variant
                FROM {columns["table"]}
                WHERE {columns["gcid"]} = :gcid
                  AND CAST({columns["exp_id"]} AS STRING) = :experiment_id
                  AND {columns["event_ts"]} >= date_sub(current_date(), :days)
            )
            SELECT
                MIN(event_day) AS start_date,
                MAX(event_day) AS end_date,
                MIN(experiment_name) AS experiment_name
            FROM target
        """

        variants_query = f"""
            SELECT DISTINCT CAST({columns["variant"]} AS STRING) AS variant
            FROM {columns["table"]}
            WHERE {columns["gcid"]} = :gcid
              AND CAST({columns["exp_id"]} AS STRING) = :experiment_id
              AND {columns["event_ts"]} >= date_sub(current_date(), :days)
              AND {columns["variant"]} IS NOT NULL
            ORDER BY variant
        """

        overlap_query = f"""
            WITH target_days AS (
                SELECT DISTINCT CAST({columns["event_ts"]} AS DATE) AS day
                FROM {columns["table"]}
                WHERE {columns["gcid"]} = :gcid
                  AND CAST({columns["exp_id"]} AS STRING) = :experiment_id
                  AND {columns["event_ts"]} >= date_sub(current_date(), :days)
            )
            SELECT COUNT(DISTINCT CAST(src.{columns["exp_id"]} AS STRING)) - 1 AS overlap_count
            FROM {columns["table"]} src
            INNER JOIN target_days td
                ON CAST(src.{columns["event_ts"]} AS DATE) = td.day
            WHERE src.{columns["gcid"]} = :gcid
              AND src.{columns["event_ts"]} >= date_sub(current_date(), :days)
        """

        params = {"gcid": gcid, "experiment_id": experiment_id, "days": days}
        details_rows = self.dbx_client.run_query(details_query, params)
        details = details_rows[0] if details_rows else None

        if not details or details["experiment_name"] is None:
            return ExperimentDetailsResponse(
                experiment_id=experiment_id,
                experiment_name="Unknown experiment",
                start_date=None,
                end_date=None,
                running_days=0,
                overlap_experiment_count=0,
                variants=[],
            )

        start_date = details["start_date"]
        end_date = details["end_date"]
        running_days = (end_date - start_date).days + 1 if start_date and end_date else 0

        variants_rows = self.dbx_client.run_query(variants_query, params)
        variants = [row["variant"] for row in variants_rows if row.get("variant")]

        overlap_count = self.dbx_client.run_scalar(overlap_query, params)
        overlap_count = max(int(overlap_count or 0), 0)

        return ExperimentDetailsResponse(
            experiment_id=experiment_id,
            experiment_name=details["experiment_name"],
            start_date=start_date,
            end_date=end_date,
            running_days=running_days,
            overlap_experiment_count=overlap_count,
            variants=variants,
        )

    def check_connection(self) -> tuple[bool, str]:
        try:
            result = self.dbx_client.run_scalar("SELECT 1")
            if int(result) == 1:
                return True, f"Connected using profile '{self.settings.dbx_profile}'"
            return False, "Connected but unexpected response from Databricks"
        except Exception as exc:  # pragma: no cover
            return False, f"Databricks connection failed: {exc}"
