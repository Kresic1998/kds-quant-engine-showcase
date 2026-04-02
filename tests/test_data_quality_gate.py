"""Data quality pack + PROVISIONAL flag."""
from __future__ import annotations

from datetime import date, timedelta

import pandas as pd

from engine.data_quality_gate import build_data_quality_pack, eia_relevant_for_short_name, merge_hard_gate_into_dq_pack


def test_eia_relevant_for_short_name():
    assert eia_relevant_for_short_name("WTI") is True
    assert eia_relevant_for_short_name("EUR") is False


def test_provisional_when_tier_low():
    old = date.today() - timedelta(days=30)
    dq = build_data_quality_pack(
        old,
        0.2,
        False,
        False,
        True,
        False,
    )
    assert dq["tier"] == "LOW"
    assert dq["provisional"] is True
    assert "composite_tier_low" in dq["provisional_reasons"]


def test_provisional_macro_stale_fallback():
    today = date.today()
    dq = build_data_quality_pack(
        today,
        1.0,
        True,
        True,
        False,
        True,
        macro_stale_fallback=True,
    )
    assert dq["provisional"] is True
    assert "macro_stale_fallback" in dq["provisional_reasons"]


def test_provisional_price_missing():
    today = date.today()
    dq = build_data_quality_pack(
        today,
        1.0,
        True,
        True,
        False,
        True,
        price_data_ok=False,
    )
    assert dq["provisional"] is True
    assert "price_data_missing" in dq["provisional_reasons"]


def test_merge_hard_gate_sets_fatal_tier():
    today = date.today()
    dq = build_data_quality_pack(today, 1.0, True, True, False, True)
    hard = {"ok": False, "tier": "FATAL", "reasons": ["critical_missing:HG=F"]}
    merged = merge_hard_gate_into_dq_pack(dq, hard)
    assert merged["data_quality_tier"] == "FATAL"
    assert merged["tier"] == "FATAL"
    assert merged["overall"] == 0.0
    assert any(str(x).startswith("hard_gate:") for x in merged["provisional_reasons"])


def test_high_tier_not_provisional_when_inputs_clean():
    today = date.today()
    dq = build_data_quality_pack(
        today,
        1.0,
        True,
        True,
        False,
        True,
        macro_stale_fallback=False,
        price_data_ok=True,
    )
    assert dq["tier"] == "HIGH"
    assert dq["provisional"] is False


def test_nan_cot_date_scores_low():
    dq = build_data_quality_pack(
        pd.NaT,
        1.0,
        True,
        True,
        False,
        True,
    )
    assert dq["cot_score"] < 50

