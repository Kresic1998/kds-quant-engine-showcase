"""_resolve_ticker_for_audit must not invent SILVER=X from commodity code SILVER."""
from engine.audit_engine import _resolve_ticker_for_audit


def test_silver_not_fx_pair():
    # Without alias: must not split SIL+VER into fake FX ticker
    assert _resolve_ticker_for_audit("SILVER", {}) is None


def test_silver_with_alias():
    m = {"SILVER": "SI=F"}
    assert _resolve_ticker_for_audit("SILVER", m) == "SI=F"


def test_six_letter_fx_pair():
    m = {}
    assert _resolve_ticker_for_audit("EURGBP", m) == "EURGBP=X"
