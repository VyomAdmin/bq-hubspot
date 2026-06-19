from unittest.mock import MagicMock, patch

import pytest
import requests

from sync.hubspot_client import HubSpotClient, RateLimitError


def _client():
    return HubSpotClient("fake-token", max_requests_per_minute=1000)


def _mock_response(status_code, json_body=None, headers=None):
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_body or {}
    resp.headers = headers or {}
    resp.raise_for_status = MagicMock(
        side_effect=requests.HTTPError(response=resp) if status_code >= 400 else None
    )
    return resp


# ---------------------------------------------------------------------------
# 429 handling
# ---------------------------------------------------------------------------

def test_429_raises_rate_limit_error():
    client = _client()
    mock_resp = _mock_response(429, headers={"Retry-After": "15"})
    with patch.object(client.session, "post", return_value=mock_resp):
        with pytest.raises(RateLimitError) as exc_info:
            client.batch_update("contacts", [{"id": "1", "properties": {}}])
    assert exc_info.value.retry_after == 15


def test_429_default_retry_after_when_header_missing():
    client = _client()
    mock_resp = _mock_response(429, headers={})
    with patch.object(client.session, "post", return_value=mock_resp):
        with pytest.raises(RateLimitError) as exc_info:
            client.batch_update("contacts", [])
    assert exc_info.value.retry_after == 10  # default


# ---------------------------------------------------------------------------
# 5xx handling
# ---------------------------------------------------------------------------

def test_500_raises_http_error():
    client = _client()
    mock_resp = _mock_response(500)
    with patch.object(client.session, "post", return_value=mock_resp):
        with pytest.raises(requests.HTTPError):
            client.batch_update("contacts", [])


# ---------------------------------------------------------------------------
# Success responses
# ---------------------------------------------------------------------------

def test_200_returns_json():
    client = _client()
    body = {"results": [{"id": "42", "properties": {"email": "a@b.com"}}]}
    mock_resp = _mock_response(200, json_body=body)
    with patch.object(client.session, "post", return_value=mock_resp):
        result = client.batch_update("contacts", [{"id": "42", "properties": {}}])
    assert result == body


def test_207_partial_returned_as_is():
    client = _client()
    body = {
        "results": [{"id": "1"}],
        "errors": [{"id": "2", "message": "property not found"}],
    }
    mock_resp = _mock_response(207, json_body=body)
    with patch.object(client.session, "post", return_value=mock_resp):
        result = client.batch_update("contacts", [])
    assert "errors" in result
    assert len(result["errors"]) == 1


# ---------------------------------------------------------------------------
# _parse_response (via runner)
# ---------------------------------------------------------------------------

def test_parse_response_splits_successes_and_failures():
    from sync.runner import _parse_response

    response = {
        "results": [{"id": "111"}, {"id": "222"}],
        "errors": [{"id": "333", "message": "bad property"}],
    }
    id_to_row = {"111": "row-a", "222": "row-b", "333": "row-c"}
    successes, failures = [], []
    _parse_response(response, id_to_row, successes, failures)

    assert set(successes) == {"row-a", "row-b"}
    assert failures == [("row-c", "bad property")]


def test_parse_response_ignores_unmapped_ids():
    from sync.runner import _parse_response

    response = {"results": [{"id": "unknown-id"}], "errors": []}
    successes, failures = [], []
    _parse_response(response, {}, successes, failures)
    assert successes == []
    assert failures == []
