import math
import os
from pathlib import Path
from typing import Optional

EPSILON = 1e-9

_DB_FILENAME = os.environ.get("COT_QUANT_DB_FILENAME", "cot_quant_master.db")
_DB_PATH_RESOLVED: Optional[str] = None


def get_sqlite_db_path() -> str:
    """
    Resolved absolute path to the SQLite file (signals, ledger, COT tables).

    Priority:
    1) Env `COT_QUANT_DB_PATH` (full path)
    2) Streamlit secrets `COT_QUANT_DB_PATH` or `SQLITE_DB_PATH` (lazy, if app runs under Streamlit)
    3) Env `COT_QUANT_DATA_DIR` or `STREAMLIT_DATA_DIR` + filename
    4) Repo root / `cot_quant_master.db` (default for local / git checkout)
    """
    global _DB_PATH_RESOLVED
    if _DB_PATH_RESOLVED is not None:
        return _DB_PATH_RESOLVED

    env_full = os.environ.get("COT_QUANT_DB_PATH", "").strip()
    if env_full:
        _DB_PATH_RESOLVED = os.path.abspath(env_full)
        return _DB_PATH_RESOLVED

    try:
        import streamlit as st  # type: ignore

        sec = getattr(st, "secrets", {})
        if sec is not None:
            for k in ("COT_QUANT_DB_PATH", "SQLITE_DB_PATH"):
                if k in sec and str(sec[k]).strip():
                    _DB_PATH_RESOLVED = os.path.abspath(str(sec[k]).strip())
                    return _DB_PATH_RESOLVED
            for k in ("COT_QUANT_DATA_DIR", "STREAMLIT_DATA_DIR"):
                if k in sec and str(sec[k]).strip():
                    _DB_PATH_RESOLVED = os.path.join(
                        os.path.abspath(str(sec[k]).strip()), _DB_FILENAME
                    )
                    return _DB_PATH_RESOLVED
    except Exception:
        pass

    data_dir = (
        os.environ.get("COT_QUANT_DATA_DIR", "").strip()
        or os.environ.get("STREAMLIT_DATA_DIR", "").strip()
    )
    if data_dir:
        _DB_PATH_RESOLVED = os.path.join(os.path.abspath(data_dir), _DB_FILENAME)
        return _DB_PATH_RESOLVED

    root = Path(__file__).resolve().parent.parent
    _DB_PATH_RESOLVED = str(root / _DB_FILENAME)
    return _DB_PATH_RESOLVED


def reset_sqlite_db_path_cache() -> None:
    """Tests / rare hot-reload: force re-read env and secrets."""
    global _DB_PATH_RESOLVED
    _DB_PATH_RESOLVED = None
    g = globals()
    if "DB_NAME" in g:
        del g["DB_NAME"]


def is_default_ephemeral_sqlite_path(path: Optional[str] = None) -> bool:
    """
    True when DB lives under Streamlit Cloud app checkout (/mount/src/...).
    That copy is reset on redeploy — use Secrets persistent path for production clients.
    """
    p = (path or get_sqlite_db_path()).replace("\\", "/")
    return "/mount/src/" in p


def __getattr__(name: str):
    if name == "DB_NAME":
        p = get_sqlite_db_path()
        globals()["DB_NAME"] = p
        return p
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

# DISPLAY_MAP kodovi za disaggregated robu (nema TFF Asset Manager u COT obrascu).
COT_DISAGG_SHORT_NAMES = frozenset({"XAU", "XAG", "WTI", "COPPER", "USOIL", "HG"})


def cot_disaggregated_short_name(short_name: str | None) -> bool:
    return str(short_name or "").upper().strip() in COT_DISAGG_SHORT_NAMES


def cot_field_float(row, key: str, default: float) -> float:
    """
    Čita numeričko COT polje iz reda (dict/Series): 0.0 je validno.
    `x or default` je zabranjeno — gazi legitiman indeks 0.
    """
    if row is None:
        return float(default)
    try:
        v = row.get(key) if hasattr(row, "get") else row[key]
    except (KeyError, IndexError, TypeError):
        return float(default)
    if v is None:
        return float(default)
    try:
        x = float(v)
    except (TypeError, ValueError):
        return float(default)
    if math.isnan(x):
        return float(default)
    return x

TICKER_MAP = {
    "USD INDEX - ICE FUTURES U.S.": "DX-Y.NYB",
    "EURO FX - CHICAGO MERCANTILE EXCHANGE": "EURUSD=X",
    "BRITISH POUND - CHICAGO MERCANTILE EXCHANGE": "GBPUSD=X",
    # Explicit USD-quoted majors (same series as JPY=X/CAD=X aliases; clearer for charts).
    "JAPANESE YEN - CHICAGO MERCANTILE EXCHANGE": "USDJPY=X",
    "CANADIAN DOLLAR - CHICAGO MERCANTILE EXCHANGE": "USDCAD=X",
    "SWISS FRANC - CHICAGO MERCANTILE EXCHANGE": "USDCHF=X",
    "AUSTRALIAN DOLLAR - CHICAGO MERCANTILE EXCHANGE": "AUDUSD=X",
    "NZ DOLLAR - CHICAGO MERCANTILE EXCHANGE": "NZDUSD=X",
    "BITCOIN - CHICAGO MERCANTILE EXCHANGE": "BTC-USD",
    "ETHER CASH SETTLED - CHICAGO MERCANTILE EXCHANGE": "ETH-USD",
    "E-MINI S&P 500 - CHICAGO MERCANTILE EXCHANGE": "^GSPC",
    # Nasdaq-100 index (not broad NASDAQ Composite).
    "NASDAQ MINI - CHICAGO MERCANTILE EXCHANGE": "^NDX",
    "RUSSELL E-MINI - CHICAGO MERCANTILE EXCHANGE": "^RUT",
    "NIKKEI STOCK AVERAGE - CHICAGO MERCANTILE EXCHANGE": "^N225",
    # Yields (%): 10Y and 30Y cash Treasury proxies on Yahoo.
    "UST 10Y NOTE - CHICAGO BOARD OF TRADE": "^TNX",
    "UST BOND - CHICAGO BOARD OF TRADE": "^TYX",
    # Futures price (not yield %): 2Y note + Fed Funds.
    "UST 2Y NOTE - CHICAGO BOARD OF TRADE": "ZT=F",
    "FED FUNDS - CHICAGO BOARD OF TRADE": "ZQ=F",
    # Liquid vol proxy for “VIX” context (CME VIX futures track similarly).
    "VIX FUTURES - CBOE FUTURES EXCHANGE": "^VIX",
    "GOLD - COMMODITY EXCHANGE INC.": "GC=F",
    "SILVER - COMMODITY EXCHANGE INC.": "SI=F",
    "WTI-PHYSICAL - NEW YORK MERCANTILE EXCHANGE": "CL=F",
    "COPPER- #1 - COMMODITY EXCHANGE INC.": "HG=F",
}

INVERTED_PAIRS = [
    'JAPANESE YEN - CHICAGO MERCANTILE EXCHANGE', 
    'CANADIAN DOLLAR - CHICAGO MERCANTILE EXCHANGE', 
    'SWISS FRANC - CHICAGO MERCANTILE EXCHANGE'
]

FX_HIERARCHY = {
    "EURO FX - CHICAGO MERCANTILE EXCHANGE": 1,
    "BRITISH POUND - CHICAGO MERCANTILE EXCHANGE": 2,
    "AUSTRALIAN DOLLAR - CHICAGO MERCANTILE EXCHANGE": 3,
    "NZ DOLLAR - CHICAGO MERCANTILE EXCHANGE": 4,
    "GOLD - COMMODITY EXCHANGE INC.": 4.1,
    "SILVER - COMMODITY EXCHANGE INC.": 4.2,
    "COPPER- #1 - COMMODITY EXCHANGE INC.": 4.3,
    "WTI-PHYSICAL - NEW YORK MERCANTILE EXCHANGE": 4.4,
    "E-MINI S&P 500 - CHICAGO MERCANTILE EXCHANGE": 4.5,
    "NASDAQ MINI - CHICAGO MERCANTILE EXCHANGE": 4.6,
    "RUSSELL E-MINI - CHICAGO MERCANTILE EXCHANGE": 4.65,
    "NIKKEI STOCK AVERAGE - CHICAGO MERCANTILE EXCHANGE": 4.66,
    "USD INDEX - ICE FUTURES U.S.": 5,
    "CANADIAN DOLLAR - CHICAGO MERCANTILE EXCHANGE": 6,
    "SWISS FRANC - CHICAGO MERCANTILE EXCHANGE": 7,
    "JAPANESE YEN - CHICAGO MERCANTILE EXCHANGE": 8,
}

DISPLAY_MAP = {
    "EURO FX - CHICAGO MERCANTILE EXCHANGE": "EUR",
    "BRITISH POUND - CHICAGO MERCANTILE EXCHANGE": "GBP",
    "AUSTRALIAN DOLLAR - CHICAGO MERCANTILE EXCHANGE": "AUD",
    "NZ DOLLAR - CHICAGO MERCANTILE EXCHANGE": "NZD",
    "USD INDEX - ICE FUTURES U.S.": "USD",
    "CANADIAN DOLLAR - CHICAGO MERCANTILE EXCHANGE": "CAD",
    "SWISS FRANC - CHICAGO MERCANTILE EXCHANGE": "CHF",
    "JAPANESE YEN - CHICAGO MERCANTILE EXCHANGE": "JPY",
    "GOLD - COMMODITY EXCHANGE INC.": "XAU",
    "SILVER - COMMODITY EXCHANGE INC.": "XAG",
    "COPPER- #1 - COMMODITY EXCHANGE INC.": "COPPER",
    "WTI-PHYSICAL - NEW YORK MERCANTILE EXCHANGE": "WTI",
    "E-MINI S&P 500 - CHICAGO MERCANTILE EXCHANGE": "SPX",
    "NASDAQ MINI - CHICAGO MERCANTILE EXCHANGE": "NDX",
    "RUSSELL E-MINI - CHICAGO MERCANTILE EXCHANGE": "RUT",
    "NIKKEI STOCK AVERAGE - CHICAGO MERCANTILE EXCHANGE": "N225",
    "UST 2Y NOTE - CHICAGO BOARD OF TRADE": "US2Y",
    "UST 10Y NOTE - CHICAGO BOARD OF TRADE": "US10Y",
    "UST BOND - CHICAGO BOARD OF TRADE": "US30Y",
    "FED FUNDS - CHICAGO BOARD OF TRADE": "FED",
    "VIX FUTURES - CBOE FUTURES EXCHANGE": "VIX",
    "BITCOIN - CHICAGO MERCANTILE EXCHANGE": "BTC",
    "ETHER CASH SETTLED - CHICAGO MERCANTILE EXCHANGE": "ETH",
}

REVERSE_DISPLAY_MAP = {v: k for k, v in DISPLAY_MAP.items()}

FRED_SERIES = {
    "USD": {"RATE": "FEDFUNDS", "GDP": "GDPC1", "CPI": "CPIAUCSL", "UNRATE": "UNRATE", "CLI": "CSCICP03USM665S"},
    "EUR": {"RATE": "IR3TIB01EZM156N", "GDP": "CLVMNACSCAB1GQEA19", "CPI": "CPHPTT01EZM659N", "UNRATE": "LRHUTTTTEZM156S", "CLI": "CSCICP02EZM460S"},
    "GBP": {"RATE": "IR3TIB01GBM156N", "GDP": "GBRGDPRQPSMEI", "CPI": "GBRCPIALLMINMEI", "UNRATE": "LRHUTTTTGBM156S", "CLI": "CSCICP02GBM460S"},
    "JPY": {"RATE": "IRSTCB01JPM156N", "GDP": "JPNPROINDMISMEI", "CPI": "JPNCPIALLMINMEI", "UNRATE": "LRUN64TTJPM156S", "CLI": "JPNLORSGPTDSTSAM"},
    "CAD": {"RATE": "IRSTCB01CAM156N", "GDP": "CANGDPRQPSMEI", "CPI": "CANCPIALLMINMEI", "UNRATE": "LRHUTTTTCAM156S", "CLI": "CANLORSGPTDSTSAM"},
    "AUD": {"RATE": "IRSTCI01AUM156N", "GDP": "AUSGDPRQPSMEI", "CPI": "AUSCPIALLQINMEI", "UNRATE": "LRUNTTTTAUM156S", "CLI": "CSCICP02AUM460S"},
    "CHF": {"RATE": "IRSTCI01CHM156N", "GDP": "CHEGDPRQPSMEI", "CPI": "CHECPIALLMINMEI", "UNRATE": "LRUN64TTCHQ156S", "CLI": "CHELOLITONOSTSAM"},
    "NZD": {"RATE": "IRSTCI01NZM156N", "GDP": "NZLGDPRQPSMEI", "CPI": "NZLCPIALLQINMEI", "UNRATE": "LRUNTTTTNZQ156S", "CLI": "NZLLOLITONOSTSAM"},
    "CHN": {"RATE": "IR3TIB01CNM156N", "GDP": "CHNGDPNQDSMEI", "CPI": "CPALTT01CNM657N", "EXPORT": "XTEXVA01CNM667S", "CLI": "CSCICP02CNM460S"}
}


def normalize_price_return(instrument_code: str, raw_return: float) -> float:
    try:
        ret = float(raw_return)
    except Exception:
        return float(raw_return)
    code = str(instrument_code or "").strip()
    return -ret if code in INVERTED_PAIRS else ret


def get_asset_class(name):
    name = name.upper().strip()
    if any(x in name for x in ['BOND', 'NOTE', 'TREASURY', 'FED FUNDS', 'YIELD', 'T-BILL', '2-YEAR', '10-YEAR']): return "OBVEZNICE"
    if any(x in name for x in ['INDEX', 'S&P', '500', 'SPX', 'NDX', 'NASDAQ', 'RUSSELL', 'NIKKEI', 'DOW JONES', 'STOCK', 'EQUITY']):
        if 'USD INDEX' not in name: return "INDEKSI"
    if any(x in name for x in ['USD INDEX', 'CURRENCY', 'EURO', 'YEN', 'POUND', 'DOLLAR', 'FRANC', 'PESO', 'FX', 'SWISS']): return "VALUTE"
    if any(x in name for x in ['BITCOIN', 'ETHER', 'CASH SETTLED', 'CRYPTO']): return "KRIPTO"
    if any(x in name for x in ['WTI', 'BRENT', 'GOLD', 'SILVER', 'COPPER', 'CORN', 'WHEAT', 'SOYBEANS', 'GAS', 'CRUDE', 'COCOA', 'COFFEE', 'SUGAR']): return "ROBE"
    return "OSTALO"


def format_instrument_name(name):
    nm = str(name or "").strip()
    if nm in DISPLAY_MAP:
        return DISPLAY_MAP[nm]
    # Deterministic fallback: trim exchange suffix/details, avoid fuzzy substring matches.
    return nm.split(" - ")[0].split(" (")[0].strip()
