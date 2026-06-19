import logging
import os

import structlog
from flask import Flask, jsonify, request

from sync.config import load_config
from sync.runner import run_sync

# ---------------------------------------------------------------------------
# Logging — structured JSON for Cloud Logging ingestion
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
structlog.configure(
    processors=[
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
    logger_factory=structlog.PrintLoggerFactory(),
)

app = Flask(__name__)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/sync", methods=["POST"])
def sync_handler():
    """
    Triggered by Cloud Scheduler every 5 minutes via authenticated HTTPS POST.
    Cloud Run is configured with --no-allow-unauthenticated; the Scheduler SA
    holds roles/run.invoker, so no manual token validation is needed here.
    """
    cfg = load_config()
    try:
        stats = run_sync(cfg)
        return jsonify({"status": "ok", **stats}), 200
    except Exception as exc:
        logging.exception("sync_run_failed")
        return jsonify({"status": "error", "error": str(exc)}), 500


@app.route("/healthz", methods=["GET"])
def health():
    """Liveness probe for Cloud Run."""
    return "ok", 200


# ---------------------------------------------------------------------------
# Local entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
