import re

from profile_silver_table import get_connection


KEYWORDS = [
    "gcid",
    "customer",
    "client",
    "user",
    "userid",
    "uid",
    "distinct",
    "experiment",
    "variant",
    "optimizely",
    "event",
    "timestamp",
    "time",
    "date",
    "created",
    "ingest",
]


def main():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DESCRIBE TABLE main.mixpanel.silver")
            rows = cur.fetchall()

    print("MATCHES")
    total = 0
    for col_name, data_type, *_ in rows:
        if not col_name:
            continue
        low = col_name.lower()
        if any(k in low for k in KEYWORDS):
            print(f"{col_name}|{data_type}")
            total += 1
    print(f"TOTAL_MATCHES={total}")

    # Strong candidates
    patterns = {
        "id_candidates": re.compile(r"(gcid|customer.*id|client.*id|(^|_)user(id)?($|_)|distinct_id)"),
        "event_time_candidates": re.compile(r"(event.*(time|ts|date)|timestamp|created_at|time_stamp|datetime)"),
        "experiment_candidates": re.compile(r"(experiment|optimizely)"),
        "variant_candidates": re.compile(r"variant"),
    }

    print("\nSTRONG_CANDIDATES")
    for label, pattern in patterns.items():
        print(f"[{label}]")
        for col_name, data_type, *_ in rows:
            name = (col_name or "").lower()
            if pattern.search(name):
                print(f"  {col_name}|{data_type}")


if __name__ == "__main__":
    main()
