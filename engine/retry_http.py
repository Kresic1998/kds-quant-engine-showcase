"""HTTP / Yahoo pozivi sa eksponencijalnim backoff-om (503, privremene mrežne greške)."""
from __future__ import annotations

from typing import Any

import requests

from .retry_util import run_with_retries


def requests_get_retry(
    url: str,
    *,
    attempts: int = 4,
    base_seconds: float = 1.5,
    max_sleep: float = 45.0,
    default: Any = None,
    **kwargs,
):
    """
    requests.get sa retry; vraća Response ili `default` (npr. None) ako svi pokušaji padnu.
    """
    kwargs.setdefault("timeout", kwargs.get("timeout", 15))

    def call():
        r = requests.get(url, **kwargs)
        r.raise_for_status()
        return r

    return run_with_retries(
        call,
        attempts=attempts,
        base_seconds=base_seconds,
        max_sleep=max_sleep,
        exponential=True,
        default=default,
    )


def yfinance_download_retry(download_fn, *, attempts: int = 4, base_seconds: float = 2.0, default: Any = None):
    """
    Wrapper oko `yfinance.download` ili sličnog callable-a koji vraća DataFrame.
    `download_fn` je npr. lambda: yf.download(...).
    """
    return run_with_retries(
        download_fn,
        attempts=attempts,
        base_seconds=base_seconds,
        max_sleep=60.0,
        exponential=True,
        default=default,
    )
