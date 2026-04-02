"""
Composite data-quality index (COT age, macro coverage, live feeds) + PROVISIONAL flag.

Used by Streamlit, score decomposition audit, and Telegram panel summaries.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pandas as pd


def eia_relevant_for_short_name(short_name: str | None) -> bool:
    """Heuristic: energy/commodity FX exposure where EIA context matters."""
    if not short_name:
        return False
    s = str(short_name).upper()
    keys = ("WTI", "USOIL", "BRENT", "CAD", "COPPER", "HG", "NAT", "GAS", "CL")
    return any(k in s for k in keys)


def _clip(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, float(v)))


def build_data_quality_pack(
    cot_date: Any,
    macro_cov_ratio: float,
    yield_ok: bool,
    shield_ok: bool,
    eia_relevant: bool,
    eia_ok: bool,
    *,
    macro_stale_fallback: bool = False,
    price_data_ok: bool | None = None,
) -> dict[str, Any]:
    """
    Returns overall 0-100, tier HIGH/MEDIUM/LOW, component scores, and provisional flag.

    PROVISIONAL when: tier LOW, overall < 70, macro stale fallback was used,
    or price_data_ok is explicitly False.
    """
    now = datetime.now(timezone.utc)
    if pd.isna(cot_date):
        cot_score = 35.0
    else:
        age_days = max(0, (now.date() - pd.to_datetime(cot_date).date()).days)
        if age_days <= 10:
            cot_score = 100.0
        elif age_days <= 17:
            cot_score = 75.0
        elif age_days <= 24:
            cot_score = 55.0
        else:
            cot_score = 35.0

    macro_score = _clip(float(macro_cov_ratio) * 100.0, 0.0, 100.0)
    yield_score = 100.0 if yield_ok else 45.0
    shield_score = 100.0 if shield_ok else 55.0
    eia_score = 100.0 if (not eia_relevant or eia_ok) else 50.0
    overall = (0.35 * cot_score) + (0.25 * macro_score) + (0.20 * yield_score) + (0.10 * shield_score) + (0.10 * eia_score)
    if overall >= 85:
        tier = "HIGH"
    elif overall >= 70:
        tier = "MEDIUM"
    else:
        tier = "LOW"

    reasons: list[str] = []
    if tier == "LOW":
        reasons.append("composite_tier_low")
    if overall < 70.0:
        reasons.append("overall_below_70")
    if macro_stale_fallback:
        reasons.append("macro_stale_fallback")
    if price_data_ok is False:
        reasons.append("price_data_missing")
    provisional = bool(reasons)

    return {
        "overall": float(overall),
        "tier": tier,
        "cot_score": float(cot_score),
        "macro_score": float(macro_score),
        "yield_score": float(yield_score),
        "shield_score": float(shield_score),
        "eia_score": float(eia_score),
        "macro_stale_fallback_used": bool(macro_stale_fallback),
        "price_data_ok": price_data_ok,
        "provisional": provisional,
        "provisional_reasons": reasons,
        "hard_data_gate": None,
        "data_quality_tier": tier,
    }


def merge_hard_gate_into_dq_pack(
    dq: dict[str, Any],
    hard: dict[str, Any] | None,
) -> dict[str, Any]:
    """
    Attach Hard Data Gate result and promote composite tier to FATAL / CRITICAL_STALE when gate fails.
    """
    out = dict(dq)
    out["hard_data_gate"] = hard
    if not hard or hard.get("ok"):
        out["data_quality_tier"] = str(out.get("tier", "LOW"))
        return out
    ht = str(hard.get("tier") or "CRITICAL_STALE")
    out["data_quality_tier"] = ht
    out["tier"] = ht
    out["overall"] = 0.0
    out["provisional"] = True
    merged = list(out.get("provisional_reasons") or [])
    for r in hard.get("reasons") or []:
        key = f"hard_gate:{r}"
        if key not in merged:
            merged.append(key)
    out["provisional_reasons"] = merged
    return out
