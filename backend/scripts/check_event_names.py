from datetime import datetime, timedelta, timezone

from profile_silver_table import get_connection


def main() -> None:
    start_day = datetime.now(timezone.utc).date() - timedelta(days=14)
    start_ymd = f"{start_day.year:04d}-{start_day.month:02d}-{start_day.day:02d}"

    query = """
        SELECT mp_event_name, COUNT(*) AS cnt
        FROM main.mixpanel.silver
        WHERE mp_event_name IS NOT NULL
          AND etr_ymd >= :start_ymd
          AND LOWER(mp_event_name) LIKE '%experiment%'
        GROUP BY mp_event_name
        ORDER BY cnt DESC
        LIMIT 20
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, {"start_ymd": start_ymd})
            rows = cur.fetchall()
    print("EVENT_NAMES_WITH_EXPERIMENT_LAST_14_DAYS")
    for row in rows:
        print(f"{row[0]}|{row[1]}")


if __name__ == "__main__":
    main()
