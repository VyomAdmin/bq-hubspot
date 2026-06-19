import logging
import time
from typing import Any

import requests
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)

HUBSPOT_BASE = "https://api.hubapi.com"

# Object types that support idProperty-based batch updates
ID_PROPERTY_SUPPORTED = {"contacts", "companies"}


class RateLimitError(Exception):
    def __init__(self, retry_after: int = 10):
        self.retry_after = retry_after
        super().__init__(f"HubSpot rate limited; retry after {retry_after}s")


class HubSpotClient:
    def __init__(self, token: str, max_requests_per_minute: int = 100):
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            }
        )
        self._rpm_limit = max_requests_per_minute
        self._request_times: list[float] = []

    def _throttle(self) -> None:
        """Sliding-window rate limiter — blocks until under the RPM cap."""
        now = time.monotonic()
        self._request_times = [t for t in self._request_times if now - t < 60.0]
        if len(self._request_times) >= self._rpm_limit:
            sleep_for = 60.0 - (now - self._request_times[0]) + 0.1
            logger.info("Throttling: sleeping %.1fs to respect RPM limit", sleep_for)
            time.sleep(max(sleep_for, 0))
        self._request_times.append(time.monotonic())

    def batch_update(
        self, object_type: str, inputs: list[dict[str, Any]]
    ) -> dict[str, Any]:
        """
        POST /crm/v3/objects/{object_type}/batch/update

        Returns the parsed JSON response.
        Raises RateLimitError on 429, requests.HTTPError on 5xx.
        """
        self._throttle()
        url = f"{HUBSPOT_BASE}/crm/v3/objects/{object_type}/batch/update"
        resp = self.session.post(url, json={"inputs": inputs}, timeout=30)

        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", 10))
            logger.warning(
                "HubSpot 429 on %s; Retry-After=%s", object_type, retry_after
            )
            raise RateLimitError(retry_after=retry_after)

        if resp.status_code >= 500:
            logger.error(
                "HubSpot 5xx on %s: %s %s", object_type, resp.status_code, resp.text
            )
            resp.raise_for_status()

        # 200 = full success, 207 = partial (results + errors arrays both present)
        return resp.json()


    def search_deals_by_property(
        self, property_name: str, values: list[str]
    ) -> dict[str, str]:
        """
        Searches for deals where `property_name` is in `values`.
        Returns {property_value: hubspot_deal_id}.
        Uses CRM search API with an IN filter (up to 100 values per call).
        """
        self._throttle()
        url = f"{HUBSPOT_BASE}/crm/v3/objects/deals/search"
        payload = {
            "filterGroups": [
                {
                    "filters": [
                        {
                            "propertyName": property_name,
                            "operator": "IN",
                            "values": values[:100],
                        }
                    ]
                }
            ],
            "properties": [property_name],
            "limit": 100,
        }
        resp = self.session.post(url, json=payload, timeout=30)

        if resp.status_code == 429:
            raise RateLimitError(retry_after=int(resp.headers.get("Retry-After", 10)))
        if resp.status_code >= 500:
            resp.raise_for_status()

        result: dict[str, str] = {}
        for item in resp.json().get("results", []):
            prop_val = item.get("properties", {}).get(property_name)
            if prop_val:
                result[prop_val] = item["id"]
        return result

    def update_deal(self, deal_id: str, properties: dict[str, Any]) -> dict[str, Any]:
        """
        PATCH /crm/v3/objects/deals/{deal_id}
        Updates a single deal. Raises RateLimitError on 429, HTTPError on 5xx.
        """
        self._throttle()
        url = f"{HUBSPOT_BASE}/crm/v3/objects/deals/{deal_id}"
        resp = self.session.patch(url, json={"properties": properties}, timeout=30)

        if resp.status_code == 429:
            raise RateLimitError(retry_after=int(resp.headers.get("Retry-After", 10)))
        if resp.status_code >= 500:
            resp.raise_for_status()

        return resp.json()


def make_retrying_batch_update(client: HubSpotClient):
    """
    Returns a callable with the same signature as HubSpotClient.batch_update
    but wrapped with tenacity exponential backoff for RateLimitError and 5xx.
    """

    @retry(
        retry=retry_if_exception_type((RateLimitError, requests.HTTPError)),
        wait=wait_exponential(multiplier=2, min=2, max=60),
        stop=stop_after_attempt(4),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    def _call(object_type: str, inputs: list[dict[str, Any]]) -> dict[str, Any]:
        try:
            return client.batch_update(object_type, inputs)
        except RateLimitError as exc:
            time.sleep(exc.retry_after)
            raise

    return _call


def make_retrying_update_deal(client: HubSpotClient):
    """Returns client.update_deal wrapped with tenacity retry."""

    @retry(
        retry=retry_if_exception_type((RateLimitError, requests.HTTPError)),
        wait=wait_exponential(multiplier=2, min=2, max=60),
        stop=stop_after_attempt(4),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    def _call(deal_id: str, properties: dict[str, Any]) -> dict[str, Any]:
        try:
            return client.update_deal(deal_id, properties)
        except RateLimitError as exc:
            time.sleep(exc.retry_after)
            raise

    return _call
