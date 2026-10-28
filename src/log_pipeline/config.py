"""Configuration management for LogAnalyticsPipeline.

Handles loading settings from environment variables and `.env` file,
constructing SQLAlchemy database URLs securely using URL.create(),
and preventing secret leakage in representations and logs.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv
from pydantic import BaseModel, Field
from sqlalchemy.engine import URL, make_url

# Load environment variables from .env if present
load_dotenv(override=False)


class DatabaseConfig(BaseModel):
    """Database connectivity configuration with safe URL handling."""

    url: Optional[str] = Field(
        default=None,
        description="Optional full SQLAlchemy database URL override",
    )
    host: str = Field(default="localhost", description="Database host")
    port: int = Field(default=3306, description="Database port")
    user: str = Field(default="log_user", description="Database username")
    password: str = Field(default="log_pass", description="Database password")
    name: str = Field(default="log_analytics", description="Database schema name")
    echo: bool = Field(default=False, description="Whether to echo SQL queries")
    pool_size: int = Field(default=5, description="Connection pool size")
    max_overflow: int = Field(default=10, description="Max overflow connections")

    def __repr__(self) -> str:
        return (
            f"DatabaseConfig(host='{self.host}', port={self.port}, "
            f"user='{self.user}', password='***', name='{self.name}')"
        )

    def get_url_object(self) -> URL:
        """Construct a SQLAlchemy URL object using URL.create() for robust encoding."""
        # If an explicit URL override is provided (e.g., sqlite in tests), honor it
        if self.url:
            return make_url(self.url)

        # Otherwise safely construct with URL.create to properly escape special characters
        return URL.create(
            drivername="mysql+pymysql",
            username=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
            database=self.name,
            query={"charset": "utf8mb4"},
        )

    def get_connection_url(self) -> str:
        """Return the connection URL string suitable for create_engine."""
        return self.get_url_object().render_as_string(hide_password=False)

    def get_masked_url(self) -> str:
        """Return safe, credential-masked URL for logging and error reporting."""
        return self.get_url_object().render_as_string(hide_password=True)


class PipelineConfig(BaseModel):
    """Top-level pipeline configuration."""

    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    input_dir: Path = Field(
        default=Path("data/sample"),
        description="Directory containing raw log files to ingest",
    )
    quarantine_dir: Path = Field(
        default=Path("output/quarantine"),
        description="Directory for writing quarantined invalid records",
    )
    reports_dir: Path = Field(
        default=Path("output/reports"),
        description="Directory for writing summary CSV reports",
    )
    batch_size: int = Field(
        default=5000,
        description="Number of records to batch insert per transaction",
    )
    log_level: str = Field(default="INFO", description="Application log level")

    def ensure_directories(self) -> None:
        """Create output directories if they do not already exist."""
        self.quarantine_dir.mkdir(parents=True, exist_ok=True)
        self.reports_dir.mkdir(parents=True, exist_ok=True)


def load_config(
    database_url: Optional[str] = None,
    input_dir: Optional[str | Path] = None,
    quarantine_dir: Optional[str | Path] = None,
    reports_dir: Optional[str | Path] = None,
    batch_size: Optional[int] = None,
) -> PipelineConfig:
    """Load configuration from environment variables with optional explicit overrides."""
    env_db_host = os.getenv("DB_HOST", "localhost")
    env_db_port = int(os.getenv("DB_PORT", "3306"))
    env_db_user = os.getenv("DB_USER", "log_user")
    env_db_password = os.getenv("DB_PASSWORD", "log_pass")
    env_db_name = os.getenv("DB_NAME", "log_analytics")

    # If an explicit database_url is provided (e.g. in test suites), use it
    explicit_url = database_url
    if not explicit_url and not os.getenv("DB_PASSWORD") and os.getenv("DATABASE_URL"):
        explicit_url = os.getenv("DATABASE_URL")

    db_config = DatabaseConfig(
        url=explicit_url,
        host=env_db_host,
        port=env_db_port,
        user=env_db_user,
        password=env_db_password,
        name=env_db_name,
    )

    batch_val = batch_size
    if batch_val is None:
        env_batch = os.getenv("BATCH_SIZE")
        batch_val = int(env_batch) if env_batch and env_batch.isdigit() else 5000

    cfg = PipelineConfig(
        database=db_config,
        input_dir=Path(input_dir or os.getenv("LOG_INPUT_DIR", "data/sample")),
        quarantine_dir=Path(quarantine_dir or os.getenv("QUARANTINE_DIR", "output/quarantine")),
        reports_dir=Path(reports_dir or os.getenv("REPORTS_DIR", "output/reports")),
        batch_size=batch_val,
        log_level=os.getenv("LOG_LEVEL", "INFO"),
    )
    return cfg
