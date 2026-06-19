import json
import logging
import os
from dataclasses import dataclass, field
from functools import lru_cache

logger = logging.getLogger(__name__)


@dataclass
class Config:
    project_id: str
    bq_dataset: str
    bq_table: str
    hubspot_token: str
    window_minutes: int = 10
    batch_size: int = 100
    max_rows_per_run: int = 5000
    max_concurrent_requests: int = 5
    max_requests_per_minute: int = 100
    mappings: dict = field(default_factory=dict)


@lru_cache(maxsize=1)
def load_config() -> Config:
    project_id = os.environ["GCP_PROJECT_ID"]

    # Allow direct token override for local dev / testing
    hubspot_token = os.environ.get("HUBSPOT_TOKEN_OVERRIDE")
    if not hubspot_token:
        hubspot_token = _fetch_secret(project_id, os.environ["HUBSPOT_TOKEN_SECRET"])

    mappings_path = os.environ.get("MAPPINGS_PATH", "mappings.json")
    with open(mappings_path) as f:
        mappings = json.load(f)

    return Config(
        project_id=project_id,
        bq_dataset=os.environ["BQ_DATASET"],
        bq_table=os.environ.get("BQ_TABLE", "hubspot_updates"),
        hubspot_token=hubspot_token,
        window_minutes=int(os.environ.get("WINDOW_MINUTES", "10")),
        batch_size=int(os.environ.get("BATCH_SIZE", "100")),
        max_rows_per_run=int(os.environ.get("MAX_ROWS_PER_RUN", "5000")),
        max_concurrent_requests=int(os.environ.get("MAX_CONCURRENT_REQUESTS", "5")),
        max_requests_per_minute=int(os.environ.get("MAX_REQUESTS_PER_MINUTE", "100")),
        mappings=mappings,
    )


def _fetch_secret(project_id: str, secret_name: str) -> str:
    from google.cloud import secretmanager  # import here so tests can run without GCP creds

    client = secretmanager.SecretManagerServiceClient()
    secret_path = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    logger.info("Fetching secret %s", secret_path)
    return client.access_secret_version(name=secret_path).payload.data.decode("utf-8").strip()
