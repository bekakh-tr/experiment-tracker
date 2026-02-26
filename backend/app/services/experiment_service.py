import ast
import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone
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
            "event_name": validate_identifier(self.settings.dbx_event_name_column),
            "part_y": validate_identifier(self.settings.dbx_partition_year_column),
            "part_ym": validate_identifier(self.settings.dbx_partition_month_column),
            "part_ymd": validate_identifier(self.settings.dbx_partition_day_column),
            "gcid": validate_identifier(self.settings.dbx_gcid_column),
            "event_ts": validate_identifier(self.settings.dbx_event_ts_column),
            "exp_id": validate_identifier(self.settings.dbx_experiment_id_column),
            "exp_name": validate_identifier(self.settings.dbx_experiment_name_column),
            "variant": validate_identifier(self.settings.dbx_variant_column),
            "variant_blob": validate_identifier(self.settings.dbx_variation_blob_column),
        }

    def _window_params(self, days: int) -> dict[str, Any]:
        window_start = datetime.now(timezone.utc).date() - timedelta(days=days)
        start_year = f"{window_start.year:04d}"
        start_ym = f"{window_start.year:04d}-{window_start.month:02d}"
        start_ymd = f"{window_start.year:04d}-{window_start.month:02d}-{window_start.day:02d}"
        return {
            # Keep partition filters on raw columns (no CAST) to preserve partition pruning.
            "start_year": start_year,
            "start_ym": start_ym,
            "start_ymd": start_ymd,
        }

    @staticmethod
    def _safe_text(value: Any, default: str = "") -> str:
        if value is None:
            return default
        text = str(value).strip()
        return text if text else default

    def _parse_blob_list(self, value: Any) -> list[str]:
        text = self._safe_text(value)
        if not text:
            return []

        if text.startswith("[") and text.endswith("]"):
            parsed: Any = None
            try:
                parsed = json.loads(text)
            except Exception:
                try:
                    parsed = ast.literal_eval(text)
                except Exception:
                    parsed = None
            if isinstance(parsed, list):
                return [self._safe_text(item) for item in parsed if self._safe_text(item)]
        return [text]

    @staticmethod
    def _variant_matches_experiment(variant: str, experiment_id: str) -> bool:
        if not variant:
            return False
        variant_low = variant.lower()
        exp_low = experiment_id.lower()
        return variant_low == exp_low or variant_low.startswith(f"{exp_low}_") or exp_low in variant_low

    def search_participation(self, gcid: str, days: int) -> SearchResponse:
        columns = self._column_map()
        window = self._window_params(days)

        query = f"""
            SELECT
                CAST({columns["event_ts"]} AS DATE) AS event_day,
                CAST({columns["exp_id"]} AS STRING) AS experiment_id,
                CAST({columns["exp_name"]} AS STRING) AS experiment_name,
                CAST({columns["variant"]} AS STRING) AS variant,
                CAST({columns["variant_blob"]} AS STRING) AS variant_blob
            FROM {columns["table"]}
            WHERE {columns["gcid"]} = :gcid
              AND {columns["event_name"]} = :event_name
              AND (
                    {columns["part_y"]} > :start_year
                    OR (
                        {columns["part_y"]} = :start_year
                        AND {columns["part_ym"]} > :start_ym
                    )
                    OR (
                        {columns["part_y"]} = :start_year
                        AND {columns["part_ym"]} = :start_ym
                        AND {columns["part_ymd"]} >= :start_ymd
                    )
              )
            ORDER BY event_day ASC
        """
        rows = self.dbx_client.run_query(
            query,
            {
                "gcid": gcid,
                "event_name": self.settings.dbx_event_name_value,
                **window,
            },
        )
        grouped: dict[Any, dict[str, Any]] = defaultdict(lambda: {"experiments": {}})

        for row in rows:
            day = row["event_day"]
            exp_ids = self._parse_blob_list(row.get("experiment_id"))
            if not exp_ids:
                continue

            row_name = self._safe_text(row.get("experiment_name"), default="Unknown experiment")
            row_variant = self._safe_text(row.get("variant"))
            variant_blob_items = self._parse_blob_list(row.get("variant_blob"))

            for idx, exp_id in enumerate(exp_ids):
                if exp_id not in grouped[day]["experiments"]:
                    grouped[day]["experiments"][exp_id] = {
                        "experiment_name": row_name if len(exp_ids) == 1 else "Unknown experiment",
                        "variants": set(),
                    }

                blob_variant = variant_blob_items[idx] if idx < len(variant_blob_items) else ""
                if blob_variant:
                    if self._variant_matches_experiment(blob_variant, exp_id):
                        grouped[day]["experiments"][exp_id]["variants"].add(blob_variant)
                    elif len(exp_ids) == 1:
                        # Single experiment row: accept blob variant even without naming match.
                        grouped[day]["experiments"][exp_id]["variants"].add(blob_variant)

                # If variationid follows "<experiment>_<variant>", attach it to matching experiment.
                if row_variant and self._variant_matches_experiment(row_variant, exp_id):
                    grouped[day]["experiments"][exp_id]["variants"].add(row_variant)

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
        window = self._window_params(days)
        query = f"""
            SELECT
                CAST({columns["event_ts"]} AS DATE) AS event_day,
                CAST({columns["exp_id"]} AS STRING) AS experiment_id,
                CAST({columns["exp_name"]} AS STRING) AS experiment_name,
                CAST({columns["variant"]} AS STRING) AS variant,
                CAST({columns["variant_blob"]} AS STRING) AS variant_blob
            FROM {columns["table"]}
            WHERE {columns["gcid"]} = :gcid
              AND {columns["event_name"]} = :event_name
              AND (
                    {columns["part_y"]} > :start_year
                    OR (
                        {columns["part_y"]} = :start_year
                        AND {columns["part_ym"]} > :start_ym
                    )
                    OR (
                        {columns["part_y"]} = :start_year
                        AND {columns["part_ym"]} = :start_ym
                        AND {columns["part_ymd"]} >= :start_ymd
                    )
              )
        """

        params = {
            "gcid": gcid,
            "event_name": self.settings.dbx_event_name_value,
            **window,
        }
        rows = self.dbx_client.run_query(query, params)

        matched_days: list[Any] = []
        variants: set[str] = set()
        overlap_experiments: set[str] = set()
        resolved_name = "Unknown experiment"

        for row in rows:
            exp_ids = self._parse_blob_list(row.get("experiment_id"))
            if experiment_id not in exp_ids:
                continue
            matched_days.append(row["event_day"])
            overlap_experiments.update(exp_ids)

            row_name = self._safe_text(row.get("experiment_name"))
            if row_name and row_name != "Unknown experiment":
                resolved_name = row_name

            variant_blob_items = self._parse_blob_list(row.get("variant_blob"))
            row_variant = self._safe_text(row.get("variant"))

            for idx, exp_id in enumerate(exp_ids):
                if exp_id != experiment_id:
                    continue
                if idx < len(variant_blob_items) and variant_blob_items[idx]:
                    blob_variant = variant_blob_items[idx]
                    if self._variant_matches_experiment(blob_variant, experiment_id) or len(exp_ids) == 1:
                        variants.add(blob_variant)
                if row_variant and self._variant_matches_experiment(row_variant, experiment_id):
                    variants.add(row_variant)

            # Fallback: some rows store cross-experiment variant lists without strict index alignment.
            for blob_variant in variant_blob_items:
                if self._variant_matches_experiment(blob_variant, experiment_id):
                    variants.add(blob_variant)

        if not matched_days:
            return ExperimentDetailsResponse(
                experiment_id=experiment_id,
                experiment_name="Unknown experiment",
                start_date=None,
                end_date=None,
                running_days=0,
                overlap_experiment_count=0,
                variants=[],
            )

        start_date = min(matched_days)
        end_date = max(matched_days)
        running_days = (end_date - start_date).days + 1 if start_date and end_date else 0
        overlap_count = max(len(overlap_experiments - {experiment_id}), 0)

        return ExperimentDetailsResponse(
            experiment_id=experiment_id,
            experiment_name=resolved_name,
            start_date=start_date,
            end_date=end_date,
            running_days=running_days,
            overlap_experiment_count=overlap_count,
            variants=sorted(variants),
        )

    def check_connection(self) -> tuple[bool, str]:
        try:
            result = self.dbx_client.run_scalar("SELECT 1")
            if int(result) == 1:
                return True, f"Connected using profile '{self.settings.dbx_profile}'"
            return False, "Connected but unexpected response from Databricks"
        except Exception as exc:  # pragma: no cover
            return False, f"Databricks connection failed: {exc}"
