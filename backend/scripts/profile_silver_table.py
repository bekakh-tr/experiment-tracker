import configparser
import json
import subprocess

from databricks import sql


def get_connection():
    cfg = configparser.ConfigParser()
    cfg.read(r"C:\Users\bekakh\.databrickscfg")
    host = cfg["Beka Databricks BI"]["host"].replace("https://", "")
    http_path = cfg["BekaLocal"]["http_path"]
    token_payload = subprocess.check_output(
        ["databricks", "auth", "token", "--profile", "Beka Databricks BI"],
        text=True,
    )
    access_token = json.loads(token_payload)["access_token"]
    return sql.connect(server_hostname=host, http_path=http_path, access_token=access_token)


def main():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DESCRIBE TABLE main.mixpanel.silver")
            rows = cur.fetchall()
            print(f"COLUMN_COUNT={len(rows)}")
            for row in rows[:300]:
                # DESCRIBE TABLE returns col_name, data_type, comment
                print(f"{row[0]}|{row[1]}")


if __name__ == "__main__":
    main()
