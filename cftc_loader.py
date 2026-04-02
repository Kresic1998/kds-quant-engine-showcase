import io
import logging
import os
import sqlite3
import zipfile

import pandas as pd
import requests
from datetime import datetime
from engine.config import TICKER_MAP, get_sqlite_db_path, normalize_price_return
from engine.cot_cftc_constants import DIS_STRICT as _DIS_STRICT, FIN_STRICT as _FIN_STRICT
from engine.db_backend import sqlalchemy_engine, use_postgresql
from engine.db_engine import ensure_sqlite_database_file_ready
from engine.pg_schema import ensure_postgres_schema
from engine.yahoo_single_history import history_close_series

# --- LOGGING KONFIGURACIJA ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# --- GLOBALNI PARAMETRI ---
def _db_path() -> str:
    return get_sqlite_db_path()

# Dinamičko generisanje godina do tekuće
CURRENT_YEAR = datetime.now().year
YEARS = [str(y) for y in range(2022, CURRENT_YEAR + 1)]

# INSTRUMENTI ZA FINANSIJSKI / DISAGG — jedan izvor: engine/cot_cftc_constants.py
FIN_STRICT = list(_FIN_STRICT)
DIS_STRICT = list(_DIS_STRICT)

def fetch_and_filter(url_template: str, target_list: list, report_type: str) -> pd.DataFrame:
    all_data = []
    
    with requests.Session() as session:
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })

        for year in YEARS:
            url = url_template.format(year)
            logging.info(f"Skeniranje {report_type} arhive za godinu: {year}")
            
            try:
                resp = session.get(url, timeout=45)
                if resp.status_code == 404:
                    logging.warning(f"Arhiva za {year} još uvek nije generisana na CFTC serveru.")
                    continue
                resp.raise_for_status()
                
                with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
                    files = [n for n in z.namelist() if n.endswith(('.txt', '.csv'))]
                    if not files:
                        continue
                        
                    with z.open(files[0]) as f:
                        df = pd.read_csv(f, low_memory=False)
                        df['Market_and_Exchange_Names'] = df['Market_and_Exchange_Names'].str.strip()
                        filtered = df[df['Market_and_Exchange_Names'].isin(target_list)].copy()
                        
                        if not filtered.empty:
                            date_cols = [c for c in filtered.columns if 'Report_Date' in c]
                            if date_cols:
                                date_col = date_cols[0]
                                filtered[date_col] = pd.to_datetime(filtered[date_col], errors='coerce')
                                all_data.append(filtered)
                                logging.info(f"Uspešno povučeno {len(filtered)} nedelja podataka iz {year}.")
                            else:
                                logging.error(f"Kolona sa datumom nije pronađena u {year} za {report_type}.")
            
            except requests.exceptions.RequestException as e:
                logging.error(f"Mrežna greška pri preuzimanju {year}: {e}")
            except zipfile.BadZipFile:
                logging.error(f"Oštećen ZIP fajl za godinu {year}.")
            except Exception as e:
                logging.error(f"Neočekivana greška u obradi godine {year}: {str(e)}")

    if all_data:
        return pd.concat(all_data, ignore_index=True)
    return pd.DataFrame()

def build_price_seasonality_db(db_name: str, years=10):
    logging.info(f">>> INICIJALIZACIJA PRICE SEASONALITY MODULA ({years}Y LOOKBACK) <<<")
    results = []
    
    for name, ticker in TICKER_MAP.items():
        try:
            cp = history_close_series(str(ticker), period=f"{int(years)}y")
            if cp.empty:
                continue

            m_prices = cp.resample("ME").last()
            m_returns = m_prices.pct_change() * 100
            
            m_returns = m_returns.apply(lambda v: normalize_price_return(name, v))
            
            r_df = pd.DataFrame({'Ret': m_returns.values}, index=m_prices.index)
            r_df['Month'] = r_df.index.month
            r_df['Year'] = r_df.index.year
            
            grp = r_df.groupby('Month')['Ret']
            avg_ret = grp.mean()
            med_ret = grp.median()
            hit_ratio = grp.apply(lambda s: float((s > 0).mean() * 100.0))
            std_dev = grp.std()
            q25 = grp.quantile(0.25)
            q75 = grp.quantile(0.75)
            sharpe_seasonal = (med_ret / std_dev.replace(0, pd.NA)).fillna(0.0)
            # Distinct-year sample depth per month (handles gaps; avoids survivorship illusion).
            sample_size = r_df.groupby('Month')['Year'].nunique()

            for month, val in avg_ret.items():
                if pd.notna(val):
                    results.append({
                        'Instrument': name, 
                        'Month': int(month), 
                        'Avg_Return': float(val),
                        'Median_Return': float(med_ret.get(month, 0.0)),
                        'Hit_Ratio': float(hit_ratio.get(month, 0.0)),
                        'Std_Dev': float(0.0 if pd.isna(std_dev.get(month, 0.0)) else std_dev.get(month, 0.0)),
                        'Q25_Return': float(q25.get(month, 0.0)),
                        'Q75_Return': float(q75.get(month, 0.0)),
                        'Sharpe_Seasonal': float(sharpe_seasonal.get(month, 0.0)),
                        'sample_size': int(sample_size.get(month, 0)),
                        'Sample_Years': int(sample_size.get(month, 0)),
                    })
        except Exception as e:
            logging.error(f"Greška pri obradi {ticker}: {e}")
            
    if results:
        df_res = pd.DataFrame(results)
        try:
            if use_postgresql():
                eng = sqlalchemy_engine()
                if eng is None:
                    raise RuntimeError("No SQLAlchemy engine")
                df_res.to_sql("price_seasonality", eng, if_exists="replace", index=False)
            else:
                with sqlite3.connect(db_name) as conn:
                    df_res.to_sql("price_seasonality", conn, if_exists="replace", index=False)
            logging.info("Price Seasonality tabela uspešno zapisana u bazu.")
        except Exception as e:
            logging.error(f"Greška pri upisu price_seasonality u bazu: {e}")


def _merge_cot_table_sqlite(conn: sqlite3.Connection, df: pd.DataFrame, table: str, date_col: str) -> None:
    if df.empty or not date_col:
        return
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df = df.dropna(subset=[date_col])
    if df.empty:
        return
    try:
        existing = pd.read_sql_query(f"SELECT * FROM {table}", conn)
    except Exception:
        existing = pd.DataFrame()
    if existing.empty:
        df2 = df.sort_values(["Market_and_Exchange_Names", date_col])
        df2.to_sql(table, conn, if_exists="replace", index=False)
        logging.info(f"{table}: created with {len(df2)} rows.")
        return
    if date_col not in existing.columns:
        df.sort_values(["Market_and_Exchange_Names", date_col]).to_sql(table, conn, if_exists="replace", index=False)
        logging.warning(f"{table}: rebuilt (missing date column in existing).")
        return
    existing[date_col] = pd.to_datetime(existing[date_col], errors="coerce")
    merged = pd.concat([existing, df], ignore_index=True)
    merged = merged.dropna(subset=[date_col])
    merged = merged.drop_duplicates(subset=["Market_and_Exchange_Names", date_col], keep="last")
    merged = merged.sort_values(["Market_and_Exchange_Names", date_col])
    merged.to_sql(table, conn, if_exists="replace", index=False)
    logging.info(f"{table}: merged to {len(merged)} rows (history preserved).")


def _merge_cot_table_postgres(df: pd.DataFrame, table: str, date_col: str) -> None:
    """Same merge logic for Supabase / PostgreSQL via SQLAlchemy."""
    if df.empty or not date_col:
        return
    eng = sqlalchemy_engine()
    if eng is None:
        raise RuntimeError("DATABASE_URL required for PostgreSQL merge")
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df = df.dropna(subset=[date_col])
    if df.empty:
        return
    try:
        existing = pd.read_sql_query(f'SELECT * FROM "{table}"', eng)
    except Exception:
        existing = pd.DataFrame()
    if existing.empty:
        df2 = df.sort_values(["Market_and_Exchange_Names", date_col])
        df2.to_sql(table, eng, if_exists="replace", index=False, method="multi")
        logging.info(f"{table}: created with {len(df2)} rows (Postgres).")
        return
    if date_col not in existing.columns:
        df.sort_values(["Market_and_Exchange_Names", date_col]).to_sql(table, eng, if_exists="replace", index=False)
        logging.warning(f"{table}: rebuilt (Postgres, missing date column).")
        return
    existing[date_col] = pd.to_datetime(existing[date_col], errors="coerce")
    merged = pd.concat([existing, df], ignore_index=True)
    merged = merged.dropna(subset=[date_col])
    merged = merged.drop_duplicates(subset=["Market_and_Exchange_Names", date_col], keep="last")
    merged = merged.sort_values(["Market_and_Exchange_Names", date_col])
    merged.to_sql(table, eng, if_exists="replace", index=False, method="multi", chunksize=500)
    logging.info(f"{table}: merged to {len(merged)} rows (Postgres).")


def build_database():
    start_time = datetime.now()
    db_name = _db_path()
    logging.info(">>> INICIJALIZACIJA GLOBALNOG QUANT LOADER-A (v12.6 supabase-ready) <<<")
    if use_postgresql():
        ensure_postgres_schema()
        logging.info("PostgreSQL (Supabase) — COT tabele se kreiraju pri prvom merge-u.")
    else:
        ensure_sqlite_database_file_ready()
        logging.info(f"SQLite target: {db_name}")

    # 1. OBRADA FINANSIJA (TFF Report)
    tff_url = "https://www.cftc.gov/files/dea/history/com_fin_txt_{}.zip"
    df_fin = fetch_and_filter(tff_url, FIN_STRICT, "FINANSIJE (TFF)")

    # 2. OBRADA ROBA (Disaggregated Report)
    disagg_url = "https://www.cftc.gov/files/dea/history/com_disagg_txt_{}.zip"
    df_robe = fetch_and_filter(disagg_url, DIS_STRICT, "ROBE (DISAGG)")

    try:
        if use_postgresql():
            if not df_fin.empty:
                date_cols_fin = [c for c in df_fin.columns if "Report_Date" in c]
                if date_cols_fin:
                    _merge_cot_table_postgres(df_fin, "tff_finansije", date_cols_fin[0])
                    logging.info(
                        f"Finansije: {df_fin['Market_and_Exchange_Names'].nunique()} instrumenata u poslednjem uvozu."
                    )
            if not df_robe.empty:
                date_cols_robe = [c for c in df_robe.columns if "Report_Date" in c]
                if date_cols_robe:
                    _merge_cot_table_postgres(df_robe, "disagg_robe", date_cols_robe[0])
                    logging.info(
                        f"Robe: {df_robe['Market_and_Exchange_Names'].nunique()} instrumenata u poslednjem uvozu."
                    )
        else:
            _parent = os.path.dirname(os.path.abspath(db_name))
            if _parent:
                os.makedirs(_parent, exist_ok=True)
            with sqlite3.connect(db_name) as conn:
                if not df_fin.empty:
                    date_cols_fin = [c for c in df_fin.columns if "Report_Date" in c]
                    if date_cols_fin:
                        _merge_cot_table_sqlite(conn, df_fin, "tff_finansije", date_cols_fin[0])
                        logging.info(
                            f"Finansije: {df_fin['Market_and_Exchange_Names'].nunique()} instrumenata u poslednjem uvozu."
                        )
                if not df_robe.empty:
                    date_cols_robe = [c for c in df_robe.columns if "Report_Date" in c]
                    if date_cols_robe:
                        _merge_cot_table_sqlite(conn, df_robe, "disagg_robe", date_cols_robe[0])
                        logging.info(
                            f"Robe: {df_robe['Market_and_Exchange_Names'].nunique()} instrumenata u poslednjem uvozu."
                        )
    except Exception as e:
        logging.error(f"Kritična greška pri upisu u bazu: {e}")

    # 3. OBRADA CENOVNE SEZONALNOSTI
    build_price_seasonality_db(db_name)
    
    duration = datetime.now() - start_time
    logging.info(f">>> PROTOKOL ZAVRŠEN: Baza '{db_name}' je sinhronizovana. Trajanje: {duration} <<<")

if __name__ == "__main__":
    build_database()