from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
    )

    DUCKDB_PATH: str = Field(default="data/duckdb/mental_health_pulse.duckdb")
    BLUESKY_HANDLE: str = Field(...)
    BLUESKY_APP_PASSWORD: str = Field(...)
    NOAA_API_TOKEN: str = Field(...)
    BLS_API_KEY: str = Field(...)
    LOG_LEVEL: str = Field(default="INFO")