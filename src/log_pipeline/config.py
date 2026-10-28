"""Configuration management for LogAnalyticsPipeline.

Handles loading settings from environment variables and `.env` file,
with sensible defaults and path resolution.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv
from pydantic import BaseModel, Field

# Load environment variables from .env if present
load_dotenv(override=False)


class DatabaseConfig(BaseModel):
    """Database connectivity configuration."""

    url: Optional[str] = Field(
        default=None,
        description="Full SQLAlchemy database URL (e.g., mysql+pymysql://user:pass@host:port/db)",
    )
    host: str = Field(default="localhost", description="Database host")
    port: int = Field(default=3306, description="Database port")
    user: str = Field(default="log_user", description="Database username")
    password: str = Field(default="log_pass", description="Database password")
    name: str = Field(default="log_analytics", description="Database schema name")
    echo: bool = Field(default=False, description="Whether to echo SQL queries")
    pool_size: int = Field(default=5, description="Connection pool size")
    max_overflow: int = Field(default=10, description="Max overflow connections")

    def get_connection_url(self) -> str:
        """Construct or return the SQLAlchemy connection URL."""
        if self.url:
            return self.url
        # Build standard MySQL connection URL using PyMySQL driver
        return (
            f"mysql+pymysql://{self.user}:{self.password}@"
            f"{self.host}:{self.port}/{self.name}?charset=utf8mb4"
        )


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
    """Load configuration from environment variables with optional explicit overrides.

    Args:
        database_url: Optional override for database connection URL.
        input_dir: Optional override for log input directory.
        quarantine_dir: Optional override for quarantine output directory.
        reports_dir: Optional override for reports output directory.
        batch_size: Optional override for batch insert size.

    Returns:
        Configured PipelineConfig instance.
    """
    env_db_url = database_url or os.getenv("DATABASE_URL")
    env_db_host = os.getenv("DB_HOST", "localhost")
    env_db_port = int(os.getenv("DB_PORT", "3306"))
    env_db_user = os.getenv("DB_USER", "log_user")
    env_db_password = os.getenv("DB_PASSWORD", "log_pass")
    env_db_name = os.getenv("DB_NAME", "log_analytics")

    db_config = DatabaseConfig(
        url=env_db_url,
        host=env_db_host,
        port=env_db_port,
        user=env_db_user,
        password=env_db_password,
        name=env_db_name,
    )

    cfg = PipelineConfig(
        database=db_config,
        input_dir=Path(input_dir or os.getenv("LOG_INPUT_DIR", "data/sample")),
        quarantine_dir=Path(quarantine_dir or os.getenv("QUARANTINE_DIR", "output/quarantine")),
        reports_dir=Path(reports_dir or os.getenv("REPORTS_DIR", "output/reports")),
        batch_size=batch_size or int(os.getenv("BATCH_SIZE", "5000")),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
    )
    return cfg
