from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    app_name: str = "Experiment Tracker"
    app_env: str = "local"
    allowed_origins: str = "http://localhost:8080,http://127.0.0.1:8080"

    dbx_profile: str = Field(default="Beka Databricks BI", alias="DBX_PROFILE")
    dbx_http_path_profile: str = Field(default="BekaLocal", alias="DBX_HTTP_PATH_PROFILE")
    dbx_config_file: str = Field(default="~/.databrickscfg", alias="DBX_CONFIG_FILE")
    dbx_table: str = Field(default="main.mixpanel.silver", alias="DBX_TABLE")
    dbx_event_name_column: str = Field(default="mp_event_name", alias="DBX_EVENT_NAME_COLUMN")
    dbx_event_name_value: str = Field(default="$experiment_started", alias="DBX_EVENT_NAME_VALUE")
    dbx_partition_year_column: str = Field(default="etr_y", alias="DBX_PARTITION_YEAR_COLUMN")
    dbx_partition_month_column: str = Field(default="etr_ym", alias="DBX_PARTITION_MONTH_COLUMN")
    dbx_partition_day_column: str = Field(default="etr_ymd", alias="DBX_PARTITION_DAY_COLUMN")
    dbx_gcid_column: str = Field(default="mp_user_id", alias="DBX_GCID_COLUMN")
    dbx_event_ts_column: str = Field(default="UpdateDate", alias="DBX_EVENT_TS_COLUMN")
    dbx_experiment_id_column: str = Field(default="x_experiment_id_blob", alias="DBX_EXPERIMENT_ID_COLUMN")
    dbx_experiment_name_column: str = Field(default="experiment_name", alias="DBX_EXPERIMENT_NAME_COLUMN")
    dbx_variant_column: str = Field(default="variationid", alias="DBX_VARIANT_COLUMN")
    dbx_variation_blob_column: str = Field(default="x_variation_id_blob", alias="DBX_VARIATION_BLOB_COLUMN")

    default_days: int = 30
    max_days: int = 365

    @property
    def cors_origins(self) -> list[str]:
        return [origin.strip() for origin in self.allowed_origins.split(",") if origin.strip()]


@lru_cache
def get_settings() -> Settings:
    return Settings()
