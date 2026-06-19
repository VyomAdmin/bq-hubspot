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

    # Invoice transition sync settings
    invoice_project: str = "psychic-lens-456414-e4"
    invoice_dataset: str = "omega_stg"
    invoice_view: str = "invoices_unique_view"
    invoice_state_table: str = "invoice_sync_state"
    hs_deal_pipeline: str = "691581097"
    hs_won_stage: str = "1013210905"
    hs_omega_job_property: str = "omega_job__c"
    hs_install_completed_property: str = "status_code__c"


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
        invoice_project=os.environ.get("INVOICE_PROJECT", "psychic-lens-456414-e4"),
        invoice_dataset=os.environ.get("INVOICE_DATASET", "omega_stg"),
        invoice_view=os.environ.get("INVOICE_VIEW", "invoices_unique_view"),
        invoice_state_table=os.environ.get("INVOICE_STATE_TABLE", "invoice_sync_state"),
        hs_deal_pipeline=os.environ.get("HS_DEAL_PIPELINE", "691581097"),
        hs_won_stage=os.environ.get("HS_WON_STAGE", "1013210905"),
        hs_omega_job_property=os.environ.get("HS_OMEGA_JOB_PROPERTY", "omega_job__c"),
        hs_install_completed_property=os.environ.get(
            "HS_INSTALL_COMPLETED_PROPERTY", "status_code__c"
        ),
    )


def _fetch_secret(project_id: str, secret_name: str) -> str:
    from google.cloud import secretmanager  # import here so tests can run without GCP creds

    client = secretmanager.SecretManagerServiceClient()
    secret_path = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    logger.info("Fetching secret %s", secret_path)
    return client.access_secret_version(name=secret_path).payload.data.decode("utf-8").strip()
