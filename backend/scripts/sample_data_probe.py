from datetime import datetime, timedelta, timezone

from app.config import get_settings
from app.services.databricks_client import DatabricksClient


def main() -> None:
    settings = get_settings()
    dbx = DatabricksClient(settings)

    start_day = datetime.now(timezone.utc).date() - timedelta(days=14)
    start_year = f"{start_day.year:04d}"
    start_ymd = f"{start_day.year:04d}-{start_day.month:02d}-{start_day.day:02d}"

    query = f"""
        SELECT
            CAST({settings.dbx_gcid_column} AS STRING) AS gcid,
            CAST({settings.dbx_experiment_id_column} AS STRING) AS experiment_id_blob,
            CAST({settings.dbx_variant_column} AS STRING) AS variation_id,
            CAST({settings.dbx_variation_blob_column} AS STRING) AS variation_blob,
            CAST({settings.dbx_event_ts_column} AS STRING) AS event_ts,
            CAST({settings.dbx_partition_year_column} AS STRING) AS etr_y,
            CAST({settings.dbx_partition_month_column} AS STRING) AS etr_ym,
            CAST({settings.dbx_partition_day_column} AS STRING) AS etr_ymd
        FROM {settings.dbx_table}
        WHERE {settings.dbx_event_name_column} = :event_name
          AND {settings.dbx_partition_year_column} >= :start_year
          AND {settings.dbx_partition_day_column} >= :start_ymd
          AND {settings.dbx_gcid_column} IS NOT NULL
        ORDER BY {settings.dbx_partition_day_column} DESC
        LIMIT 5
    """
    rows = dbx.run_query(
        query,
        {
            "event_name": settings.dbx_event_name_value,
            "start_year": start_year,
            "start_ymd": start_ymd,
        },
    )
    print("SAMPLE_EXPERIMENT_STARTED_ROWS_LAST_14_DAYS")
    for row in rows:
        print(row)


if __name__ == "__main__":
    main()
