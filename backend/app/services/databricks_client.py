import configparser
import json
import re
import subprocess
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from databricks import sql

from app.config import Settings


IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
TABLE_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*){0,2}$")


def validate_identifier(identifier: str) -> str:
    if not IDENTIFIER_PATTERN.match(identifier):
        raise ValueError(f"Invalid SQL identifier: {identifier}")
    return identifier


def validate_table_name(table_name: str) -> str:
    if not TABLE_PATTERN.match(table_name):
        raise ValueError(f"Invalid table name: {table_name}")
    return table_name


class DatabricksClient:
    def __init__(self, settings: Settings):
        self.settings = settings
        self._access_token: str | None = None
        self._token_expiry_epoch: float = 0.0

    def _read_profiles(self) -> configparser.ConfigParser:
        config_path = Path(self.settings.dbx_config_file).expanduser()
        parser = configparser.ConfigParser()
        read_paths = parser.read(config_path, encoding="utf-8")
        if not read_paths:
            raise ValueError(f"Databricks config file not found: {config_path}")
        return parser

    def _resolve_connection_settings(self) -> tuple[str, str]:
        parser = self._read_profiles()
        profile = self.settings.dbx_profile
        fallback_profile = self.settings.dbx_http_path_profile

        if profile not in parser:
            raise ValueError(f"Databricks profile '{profile}' not found in config")
        if fallback_profile not in parser:
            raise ValueError(f"Databricks HTTP path profile '{fallback_profile}' not found in config")

        host = parser[profile].get("host")
        if not host:
            raise ValueError(f"Profile '{profile}' does not contain host")

        http_path = parser[profile].get("http_path") or parser[profile].get("sql_http_path")
        if not http_path:
            # Some OAuth profiles only carry host+auth_type and rely on another profile for SQL warehouse path.
            http_path = parser[fallback_profile].get("http_path") or parser[fallback_profile].get("sql_http_path")
        if not http_path:
            raise ValueError(
                f"Could not resolve SQL warehouse http_path from profile '{profile}' "
                f"or fallback profile '{fallback_profile}'"
            )

        return host.replace("https://", ""), http_path

    def _refresh_access_token(self) -> str:
        # Reuse token if it is still valid with a small expiry buffer.
        if self._access_token and time.time() < self._token_expiry_epoch - 30:
            return self._access_token

        command = ["databricks", "auth", "token", "--profile", self.settings.dbx_profile]
        try:
            result = subprocess.run(command, capture_output=True, text=True, check=True)
        except subprocess.CalledProcessError as exc:
            message = (exc.stderr or exc.stdout or str(exc)).strip()
            raise ValueError(f"Failed to retrieve Databricks OAuth token: {message}") from exc
        payload = json.loads(result.stdout)
        access_token = payload.get("access_token")
        expires_in = int(payload.get("expires_in", 3600))
        if not access_token:
            raise ValueError("Databricks OAuth token response did not include access_token")

        self._access_token = access_token
        self._token_expiry_epoch = time.time() + max(expires_in, 60)
        return access_token

    @contextmanager
    def connect(self) -> Iterator[Any]:
        host, http_path = self._resolve_connection_settings()
        access_token = self._refresh_access_token()
        connection = sql.connect(
            server_hostname=host,
            http_path=http_path,
            access_token=access_token,
        )
        try:
            yield connection
        finally:
            connection.close()

    def run_query(self, query: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        with self.connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, params or {})
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                return [dict(zip(columns, row)) for row in rows]

    def run_scalar(self, query: str, params: dict[str, Any] | None = None) -> Any:
        with self.connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, params or {})
                row = cursor.fetchone()
                if row is None:
                    return None
                return row[0]
