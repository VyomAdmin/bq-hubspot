#!/usr/bin/env bash
# deploy.sh — builds and deploys the bq-hubspot-sync service to Cloud Run
# and wires up Cloud Scheduler.
#
# Usage:
#   export PROJECT=your-gcp-project-id
#   export REGION=us-central1
#   bash deploy.sh

set -euo pipefail

: "${PROJECT:?Set PROJECT env var}"
: "${REGION:=us-central1}"

IMAGE="gcr.io/${PROJECT}/bq-hubspot-sync:latest"
SERVICE_NAME="bq-hubspot-sync"
SYNC_SA="${SERVICE_NAME}-sa@${PROJECT}.iam.gserviceaccount.com"
SCHEDULER_SA="cloud-scheduler-invoker@${PROJECT}.iam.gserviceaccount.com"
SECRET_NAME="hubspot-private-app-token"
BQ_DATASET="crm_integration"

echo "==> [1/7] Creating service accounts (idempotent)"
gcloud iam service-accounts create "${SERVICE_NAME}-sa" \
  --display-name="BQ→HubSpot sync runner" \
  --project="${PROJECT}" 2>/dev/null || true

gcloud iam service-accounts create "cloud-scheduler-invoker" \
  --display-name="Cloud Scheduler → Cloud Run invoker" \
  --project="${PROJECT}" 2>/dev/null || true

echo "==> [2/7] Granting BigQuery roles to sync SA (dataset-level)"
gcloud projects add-iam-policy-binding "${PROJECT}" \
  --member="serviceAccount:${SYNC_SA}" \
  --role="roles/bigquery.jobUser" \
  --condition=None

# Dataset-level editor (read + update rows)
bq add-iam-policy-binding \
  --member="serviceAccount:${SYNC_SA}" \
  --role="roles/bigquery.dataEditor" \
  "${BQ_DATASET}"

echo "==> [3/7] Granting Secret Manager accessor"
gcloud secrets add-iam-policy-binding "${SECRET_NAME}" \
  --project="${PROJECT}" \
  --member="serviceAccount:${SYNC_SA}" \
  --role="roles/secretmanager.secretAccessor"

echo "==> [4/7] Building and pushing Docker image"
gcloud builds submit --tag "${IMAGE}" --project="${PROJECT}"

echo "==> [5/7] Deploying Cloud Run service"
gcloud run deploy "${SERVICE_NAME}" \
  --image="${IMAGE}" \
  --region="${REGION}" \
  --platform="managed" \
  --no-allow-unauthenticated \
  --service-account="${SYNC_SA}" \
  --memory="512Mi" \
  --timeout="540s" \
  --concurrency="1" \
  --set-env-vars="\
GCP_PROJECT_ID=${PROJECT},\
BQ_DATASET=${BQ_DATASET},\
BQ_TABLE=hubspot_updates,\
HUBSPOT_TOKEN_SECRET=${SECRET_NAME},\
WINDOW_MINUTES=10,\
BATCH_SIZE=100,\
MAX_ROWS_PER_RUN=5000,\
MAX_CONCURRENT_REQUESTS=5,\
MAX_REQUESTS_PER_MINUTE=100,\
MAPPINGS_PATH=mappings.json" \
  --project="${PROJECT}"

echo "==> [6/7] Granting Scheduler SA permission to invoke Cloud Run"
gcloud run services add-iam-policy-binding "${SERVICE_NAME}" \
  --region="${REGION}" \
  --member="serviceAccount:${SCHEDULER_SA}" \
  --role="roles/run.invoker" \
  --project="${PROJECT}"

echo "==> [7/7] Creating Cloud Scheduler job (every 5 minutes)"
SERVICE_URL=$(gcloud run services describe "${SERVICE_NAME}" \
  --region="${REGION}" \
  --project="${PROJECT}" \
  --format="value(status.url)")

gcloud scheduler jobs create http "${SERVICE_NAME}-every-5m" \
  --location="${REGION}" \
  --schedule="*/5 * * * *" \
  --uri="${SERVICE_URL}/sync" \
  --http-method="POST" \
  --oidc-service-account-email="${SCHEDULER_SA}" \
  --oidc-token-audience="${SERVICE_URL}" \
  --attempt-deadline="570s" \
  --project="${PROJECT}" 2>/dev/null || \
gcloud scheduler jobs update http "${SERVICE_NAME}-every-5m" \
  --location="${REGION}" \
  --schedule="*/5 * * * *" \
  --uri="${SERVICE_URL}/sync" \
  --http-method="POST" \
  --oidc-service-account-email="${SCHEDULER_SA}" \
  --oidc-token-audience="${SERVICE_URL}" \
  --attempt-deadline="570s" \
  --project="${PROJECT}"

echo ""
echo "Deployment complete."
echo "Service URL : ${SERVICE_URL}"
echo "Scheduler   : */5 * * * *  POST ${SERVICE_URL}/sync"
