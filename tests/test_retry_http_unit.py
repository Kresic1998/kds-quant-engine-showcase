from unittest.mock import MagicMock

import pytest

from engine.retry_http import requests_get_retry, yfinance_download_retry


def test_requests_get_retry_returns_none_when_all_fail(monkeypatch):
    monkeypatch.setattr("engine.retry_util.time.sleep", lambda s: None)
    mock_get = MagicMock(side_effect=ConnectionError("503"))
    monkeypatch.setattr("engine.retry_http.requests.get", mock_get)
    assert requests_get_retry("https://example.test/x", attempts=2, base_seconds=0.01) is None
    assert mock_get.call_count == 2


def test_yfinance_download_retry_default(monkeypatch):
    monkeypatch.setattr("engine.retry_util.time.sleep", lambda s: None)

    def boom():
        raise RuntimeError("fail")

    assert yfinance_download_retry(boom, attempts=2, base_seconds=0.01, default="x") == "x"
