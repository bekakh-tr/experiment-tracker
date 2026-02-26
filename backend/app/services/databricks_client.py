import re
from contextlib import contextmanager
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

    @contextmanager
    def connect(self) -> Iterator[Any]:
        connection = sql.connect(profile=self.settings.dbx_profile)
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
