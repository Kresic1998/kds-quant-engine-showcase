# Quant System Blueprint

**Purpose:** Authoritative technical reference for engineers and quant analysts operating this repository. The document traces **architecture, data plumbing, persistence, and risk-management mechanics** to the current Python implementation (`engine/` + selected repo roots). It describes **how** indicators are computed and combined—not **why** any asset is favored in production, and not proprietary execution alpha.

**Public showcase:** Numeric parameters in sections 2–4 are redacted as [PARAM] where they encode strategy or calibration detail. Infrastructure facts (e.g. Supabase pooler port) stay literal. For authoritative values, use the private monorepo.

**Guardrail (showcase):** Numerical transforms (Z-scores, caps, clamps) are fair game. Do not treat this file as investment advice or as a full disclosure of business strategy.

---

## 1. System hierarchy and data pipeline

### 1.1 End-to-end flow (conceptual)

1. **Ingestion & persistence**
   - CFTC-style positioning data is loaded into a **SQLite file** (default path from `engine/config.py` → `get_sqlite_db_path()`) **or** into **PostgreSQL (e.g. Supabase)** when `DATABASE_URL` / related DSN env vars or Streamlit secrets are set (`engine/db_backend.py`).
   - Loaders: repo root `cftc_loader.py` (and app subprocess); reads/writes go through `engine/db_engine.py`, `engine/cot_engine.py`, and SQL helpers `engine/cot_sql.py` / `engine/cot_postgres_columns.py` (quoted identifiers for Postgres case-folding).
2. **Feature engineering (PIT-aware)**
   - `engine/cot_engine.py`: rolling windows per `Market_and_Exchange_Names`, `Effective_Date` filtering in `load_and_process_all_asof`, outputs `Spec_Z`, `Comm_COT_Idx`, `OI_Streak`, Williams-style long-window commercial index, etc.
3. **Live market layer**
   - Yahoo proxies: `engine/market_macro_engine.py`, `engine/shield_engine.py`, `engine/yield_engine.py`, `engine/yahoo_series.py`, `engine/yahoo_single_history.py`, `engine/yahoo_download_frame.py`, `engine/price_engine.py`, `engine/shock_detector.py` (multi-ticker history/download for velocity and stress).
   - FRED / macro: `engine/macro_engine.py` (observation dates, real-rate context for lineage).
   - EIA: `engine/eia_engine.py` where energy context is required.
4. **Regime & overlays**
   - Shock classification: `engine/shock_detector.py` → bucket scores, `scenario` label, `supply_buy_block`, `ignore_seasonality`, `BOND_LIQUIDITY_SHOCK`, `INVERTED_10Y_3M`.
   - Risk parity stress: `engine/risk_parity_engine.py`.
   - Rates COT composite: `engine/rates_overlay.py` (TFF / CBOT names via `cot_sql`).
   - Intermarket pairs / matrix: `engine/intermarket_engine.py`, `engine/intermarket_narrative.py`, `engine/intermarket_correlation_matrix.py`.
   - Dollar smile heuristic: `engine/dollar_smile_engine.py`.
5. **Scoring**
   - `engine/score_engine.py` → `calculate_master_score` (also exposed as `calculate_final_quant_score`). Post-step in UI/panel: `engine/rates_overlay.apply_rates_overlay` clamps displayed composite to a fixed **1–10** band where integrated.
6. **Outputs**
   - Web UI: **UI implementation handled by the decoupled Next.js frontend** (private SaaS terminal; scanner, deep dive, decomposition, lineage, Macro Swing embed from `engine/swing_ta_engine.py` in the full monorepo). The legacy Streamlit prototype is not part of the public showcase.
   - Telegram / headless: `quant_notify_cli.py` → `engine/quant_notify_panel.py` → `engine/telegram_engine.py`.
   - Optional LLM narrative: `engine/quant_llm_context.py` + `engine/gemini_quant.py` / `engine/ai_engine.py` (structured context must not contradict numeric engine output).
   - **No X/Twitter** integration in this repository.

### 1.2 Persistence & configuration (dual backend)

| Mechanism | Role |
|-----------|------|
| `engine/config.py` | `get_sqlite_db_path()`, `DB_NAME` cache, ticker maps, FRED series lists, paths. |
| `engine/db_backend.py` | Chooses PostgreSQL vs SQLite; normalizes DSN for `psycopg2` / SQLAlchemy; `get_connection()`, `read_sql_pandas()`, `sqlalchemy_engine()`. Reads `DATABASE_URL` from **environment first**; Streamlit TOML secrets for DSN are only applied **inside an active Streamlit script context** (`get_script_run_ctx()`), so pytest/CI do not pick up committed local secrets files. |
| `engine/db_engine.py` | Schema init / migrations for SQLite; delegates Postgres DDL to `pg_schema.py` + column patches. |
| `engine/pg_schema.py` | `ensure_postgres_schema()`, `postgres_add_missing_columns()` for core app tables. |
| `engine/cot_sql.py` / `cot_postgres_columns.py` | Portable SQL quoting and dataframe column normalization for COT tables. |

**Operational note:** Serverless deploys should use Supabase **transaction pooler** (port **6543**); direct `db.*.supabase.co:5432` often fails on IPv6-restricted hosts. See `docs/SUPABASE_SETUP.md`.

### 1.3 `engine/` module map (full product; not all files exist in the public showcase)

The table below describes the **complete** private monorepo. This **showcase** repository includes only the infrastructure slice listed in `README.md` (DB, gates, retry, audit/ledger helpers, CFTC loader support). Scoring, shock, notify, and most macro modules are omitted on purpose.

Grouped by responsibility; every `*.py` in `engine/` appears exactly once.

| Group | Modules | Role |
|-------|---------|------|
| **Config & persistence** | `config.py`, `db_backend.py`, `db_engine.py`, `pg_schema.py`, `cot_sql.py`, `cot_postgres_columns.py`, `secrets_util.py` | Paths, DSN routing, SQLite/PG connections, DDL, portable COT SQL. |
| **COT core** | `cot_engine.py`, `cot_dates.py`, `cot_cftc_constants.py`, `replay_loader.py` | Rolling COT metrics, PIT loads, constants shared with loaders. |
| **Yahoo / HTTP** | `yahoo_series.py`, `yahoo_single_history.py`, `yahoo_download_frame.py`, `yahoo_refresh_session.py`, `retry_http.py`, `retry_util.py`, `live_config.py` | Market data fetch, session/cache fences, resilient HTTP. |
| **Macro bundle** | `market_macro_engine.py` | RMS, copper/gold, growth bias, MOVE in bundle; pillar decomposition. |
| **Shield** | `shield_engine.py` | VIX, DXY, OVX, MOVE; `Shield_Active`, `yahoo_last_bar_ts` for hard gate. |
| **Yields** | `yield_engine.py` | FRED + Yahoo merge, curve labels, `INVERTED_10Y_3M`, lineage fields. |
| **FRED detail** | `macro_engine.py` | Macro series helpers / lineage. |
| **Prices & seasonality** | `price_engine.py`, `lookback_engine.py` | Prices, seasonality, correlation lookback resolution. |
| **Correlation stats** | `correlation_utils.py` | Quality-weighted correlation, Z-spread on log prices (used in RV / scanner flows). |
| **Shock / smile / intermarket** | `shock_detector.py`, `dollar_smile_engine.py`, `intermarket_engine.py`, `intermarket_narrative.py`, `intermarket_correlation_matrix.py` | Regime scoring with per-channel caps; narratives; rolling-window return correlation matrix (window `[PARAM]` in code). |
| **Risk** | `risk_parity_engine.py` | Cross-asset stress score and regime string feeding shock + score guardrails. |
| **Scoring** | `score_engine.py`, `score_guardrails.py`, `score_invoke.py`, `vix_semantics.py` | Master score, guardrail catalog, backward-compatible invoke wrapper, shared VIX thresholds. |
| **Data quality** | `data_quality_gate.py`, `data_freshness.py`, `hard_data_gate.py` | Composite DQ pack + PROVISIONAL; freshness helpers; critical-ticker staleness gate. |
| **Rates overlay** | `rates_overlay.py` | COT-based rates composite → per-asset score adjustment. |
| **Energy** | `eia_engine.py` | EIA fetch/cache when relevant. |
| **Performance / audit** | `performance_ledger_engine.py`, `bias_horizon_engine.py`, `audit_engine.py`, `audit_metrics.py`, `daily_snapshots_engine.py`, `ledger_scenario_presets.py` | Ledger, MFE/MAE metrics, audits, daily snapshot rows, scenario presets. |
| **AI / notify** | `gemini_quant.py`, `ai_engine.py`, `quant_llm_context.py`, `quant_notify_panel.py`, `quant_notify_constants.py`, `quant_notify_session.py`, `telegram_engine.py`, `resend_engine.py` | Gemini client, prompt context envelopes, panel assembly, Telegram HTML. |
| **Automation** | `live_refresh.py`, `backfill_engine.py`, `backfill_schedule.py`, `quant_automation_util.py` | Headless refresh orchestration, backfills, schedules. |
| **Presentation & TA UI** | `display_format.py`, `heatmap_engine.py`, `swing_engine.py`, `swing_ta_engine.py`, `timing_engine.py`, `cross_engine.py`, `pdf_engine.py`, `win_rate_report.py` | Formatting, charts, optional SMC/swing **UI** (`swing_engine`), **manual** Macro Swing plan widget (`swing_ta_engine`); **UI implementation handled by the decoupled Next.js frontend**. PDF / win-rate reporting. |
| **Package** | `__init__.py` | Package marker. |

**Repo root (not inside `engine/` but part of the system):** `cftc_loader.py`, `live_refresh.py` (thin CLI over `engine.live_refresh.run_live_refresh`), `quant_notify_cli.py`, `audit_score_engine.py` (standalone CLI). Legacy Streamlit `app.py` was removed from the public showcase; **UI implementation handled by the decoupled Next.js frontend**.

---

## 2. Quantitative mathematics and indicators

### 2.1 COT rolling indices and Z-scores (`engine/cot_engine.py`)

Per instrument group, window `window`, `dyn_min` uses a capped fraction of `window` (see code), `EPSILON` from `config`, `ROLLING_STD_FLOOR = [PARAM]`, `SPEC_Z_CLIP = [PARAM]`:

**Commercial index (0–100)** — rolling min/max normalization of `Comm_Net` (same structural formula as in code).

**Speculator Z (net contracts)** — rolling mean/std of `Spec_Net`; denominator floored at `ROLLING_STD_FLOOR`; `Spec_Z = clip(z_raw, -SPEC_Z_CLIP, SPEC_Z_CLIP)`.

**Speculator Z on % of OI** — same on `Spec_Net_Pct`; clipped to a symmetric bound `[±PARAM]`.

**Speculator index (0–100)** — min/max on `Spec_Net` over the window.

**Asset manager Z** — when `AM_Net` exists with usable mass: same Z structure, clipped; else neutral placeholders in code path.

**Williams 156W commercial index** — `window = [PARAM]`, `min_periods = [PARAM]`.

**Changes:** `OI_Change_Custom`, `Net_Change_Custom`, `Comm_Net_Change_Custom` via `diff` with configured periods.

**OI_Streak:** sequential scan over `OI.diff()` sign.

### 2.2 Institutional bias from COT indices (`engine/score_engine.py` — `get_institutional_bias`)

- **TFF path (`include_am=True`):** blended commercial / asset-manager indices with **weight pair `[PARAM]`**; tiered thresholds on index levels drive **step impacts `[PARAM]`**; crossover terms use **scale `[PARAM]`**; tail coupling vs speculator index uses **±`[PARAM]`**.

- **Disaggregated path (`include_am=False`):** `weighted_smart = comm_idx`; high/low index bands map to **±`[PARAM]`** impacts (no AM crossover).

Exact numeric cutoffs and multipliers: **private monorepo only** (`[REDACTED]`).

### 2.3 Flow impact from Williams health (`engine/score_engine.py`)

| `h_status` | Rule (coefficients `[PARAM]` in code) |
|------------|------|
| `STRONG ACCUMULATION` | additive impact `[PARAM]` |
| `INSTITUTIONAL ABSORPTION` | streak clamped to `[PARAM]` range; capped log-shaped boost `[PARAM]` |
| `AGGRESSIVE SELLING` | additive impact `[PARAM]` |

### 2.4 Relative monetary strength and growth (`engine/market_macro_engine.py`)

- **`ROLL_DAYS = [PARAM]`** for return momentum pairs.
- **RMS:** per-currency return vs USD anchor (implementation uses configured proxy chains).
- **Copper/Gold ratio** and **slope over a configured lookback** (linear regression on last ≤**`[PARAM]`** points).
- **Lookback % change on ratio** vs a rolling baseline (see code).
- **`COPPER_GOLD_BEARISH`:** `cg_20 <= COPPER_GOLD_BEARISH_20D_PCT_THRESHOLD` with **`COPPER_GOLD_BEARISH_20D_PCT_THRESHOLD = [PARAM]`**.
- **`growth_risk_on_bias`:** `tanh` composition plus additive adjustment **`[PARAM]`** if copper/gold bearish flag set.
- **`growth_impact_for_asset`:** `tanh_norm` pieces clipped to **`[±PARAM]`** by asset class.
- **`rms_impact_for_asset`:** `tanh_norm(rms, RMS_TANH_SCALE)` scaled by **`[PARAM]`**, clipped **`[±PARAM]`**; listed risk assets use USD RMS with extra scale **`[PARAM]`**. **`RMS_TANH_SCALE = [PARAM]`**.
- **`risk_sentiment_impact`:** VIX shock, OVX shock, DXY trend buckets → clipped **`[±PARAM]`** (see code for asset sets).

### 2.5 VIX macro retention (`engine/vix_semantics.py`)

- `VIX_MACRO_RETENTION_LOW = [PARAM]`, `VIX_MACRO_RETENTION_HIGH = [PARAM]`, `VIX_MACRO_RETENTION_FLOOR = [PARAM]` — linear multiplier on macro pillar weight via `macro_retention_multiplier_from_shield`.
- **Shield panic (curr/prev):** `vix_shock_from_curr_prev`: level/spike/ratio thresholds are **`VIX_SHIELD_LEVEL = [PARAM]`**, **`[PARAM]`** on absolute level, **`VIX_SHIELD_SPIKE_BASE = [PARAM]`**, **`VIX_SHIELD_SPIKE_RATIO = [PARAM]`** (see private code).
- **Live risk-on penalty threshold:** `VIX_PENALTY_RISK_ON_THRESHOLD = [PARAM]` (used in live penalty path in `score_engine`).

### 2.6 Shield: DXY, OVX, MOVE (`engine/shield_engine.py`)

- **Data path:** `history_close_series` per `^VIX`, `DX-Y.NYB`, `^OVX`, `^MOVE` (not raw `yf.download` in shield core).
- **DXY trend:** threshold `max([PARAM], [PARAM] * σ)` where `σ` is std of last ≤**`[PARAM]`** **daily differences** of DXY (if ≥**`[PARAM]`** points). `BULLISH` / `BEARISH` vs prior close ± threshold.
- **OVX shock:** level and ratio tests with thresholds **`[PARAM]`** / **`[PARAM]`**.
- **MOVE / bond liquidity:** `BOND_LIQUIDITY_MOVE_THRESHOLD = [PARAM]`; `BOND_LIQUIDITY_SHOCK` when last MOVE > threshold.
- **`Shield_Active`:** any of `VIX_Shock`, `OVX_Shock`, `DXY_Trend == "BULLISH"`, `BOND_LIQUIDITY_SHOCK`.

### 2.7 Yields and 10Y−3M (`engine/yield_engine.py`)

- **Yahoo 10Y − 3M:** from aligned as-of slices of 10Y proxy and `IRX`: spread sign drives `INVERTED_10Y_3M` and `Spread_10Y_3M_Yahoo`.
- **FRED `T10Y3M` slice:** negative spread sets inversion flag and may override curve narrative.
- **10Y−2Y:** quantiles, short-horizon deltas (`[PARAM]` days in code), `Curve_Signal` / `Steepening_Type` branches as implemented.
- **Real 10Y expected:** `10Y - T10YIE` when both exist (feeds metal penalty path in score engine).

### 2.8 Shock detector (`engine/shock_detector.py`)

**Channel caps (`_CHANNEL_CAPS`) before winner selection:** per-channel integer caps are **`[PARAM]`** each (see private `shock_detector.py`).

**Representative point rules:** each family maps market evidence strings to **FC / DEM / SUP** point grants; all level cutoffs, point sizes, and MOVE tie-ins are **`[PARAM]`** in source. (This showcase doc omits the numeric scoring table.)

**Great Unwind:** conditional scale factor **`[PARAM]`** on `FINANCIAL_CRASH` with bonus cap **`_GREAT_UNWIND_FC_BONUS_CAP = [PARAM]`**.

**Stagflation:** combines `SUPPLY_SHOCK` and `FINANCIAL_CRASH` when both exceed **`[PARAM]`**; exact formula in code.

**Winner:** `threshold_critical = [PARAM]`, `threshold_elevated = [PARAM]` on max bucket.

**Meta flags:** `ignore_seasonality` / `supply_buy_block` compare bucket scores to **`[PARAM]`**; `BOND_LIQUIDITY_SHOCK`, `INVERTED_10Y_3M` forwarded in return dict.

### 2.9 Intermarket correlation matrix (`engine/intermarket_correlation_matrix.py`)

- **`ROLL_WINDOW = [PARAM]`** trading days on log returns; **`MIN_PAIRS_OBS = [PARAM]`**.
- Fixed `PAIR_SPECS` (GC/DXY, SPX/VIX, Copper/SPX, TNX/Gold, etc.) — see file for exact tickers.

### 2.10 Rates positioning composite (`engine/rates_overlay.py`)

- Component weights are **`[PARAM]`** each (UST tenors + SOFR TFF; SOFR name from `cot_cftc_constants`).
- Per component: AM/Comm signals combined; disagreement and `|spec_z|` beyond **`[PARAM]`** dampen; composite clipped **`[±PARAM]`** → normalized **`[±PARAM]`**.
- Regime labels from curve steepness vs composite thresholds.
- **Score overlay:** `adjustment = sensitivity[short_code] * normalized * [PARAM]`; `final = clip(base_score + adjustment, [PARAM], [PARAM])` (`RATES_OVERLAY_ASSET_SENSITIVITY` in private code).

### 2.11 Data quality composite (`engine/data_quality_gate.py`)

- COT age buckets (days): stepped scores **`[PARAM]`** per band (see `data_quality_gate.py` in this repo for structure; numeric cutoffs may be tuned privately).
- `macro_score`: maps `macro_cov_ratio` to a bounded score via `clip(macro_cov_ratio * [PARAM], [PARAM], [PARAM])` (exact scale in code).  
- Component scores for yield/shield/EIA use **good vs weak constants `[PARAM]`**  
- **`overall`** is a **weighted sum** with coefficients **`[PARAM]`** each term  
- Tier cutoffs **`[PARAM]`** for HIGH/MEDIUM/LOW.  
- **PROVISIONAL** if: tier LOW, overall below **`[PARAM]`**, macro stale fallback, or `price_data_ok is False`.  
- `merge_hard_gate_into_dq_pack` promotes tier to **`FATAL` / `CRITICAL_STALE`**, zeros overall, when hard gate fails.

### 2.12 Hard Data Gate (`engine/hard_data_gate.py`)

- **Critical tickers:** `CRITICAL_MACRO_TICKERS = ("^VIX", "^MOVE", "HG=F", "DX-Y.NYB")` — staleness vs `market_bundle` / shield `yahoo_last_bar_ts`.
- **`MAX_BUSINESS_DAY_GAP = [PARAM]`**, **`SAME_DAY_MAX_HOURS = [PARAM]`**.
- Business-day gap via **`numpy.busday_count`** (not pandas `bdate_range(freq="C")`).
- **`calculate_master_score(..., hard_data_gate_check=True)`:** on failure can return a **neutral sentinel score pair** `( [PARAM], [PARAM] )` with decomposition `trading_halted` / tier **`FATAL`** or **`CRITICAL_STALE`**. Default **`hard_data_gate_check=False`** for headless/tests.  
- **`score_invoke.invoke_calculate_master_score`:** drops `hard_data_gate_check` if target `score_engine` lacks the parameter.

### 2.13 BTC dominance & SMC (scope boundary)

- No separate **BTC.D** dominance series in `calculate_master_score`; BTC is treated like other risk-on legs in RMS/shock/overlay tables.
- **`dollar_smile_engine.py`:** `dominance_score` names a **USD regime** heuristic, not crypto market-cap dominance.
- **`swing_engine.py`:** optional SMC-style zones for **TA UI** — not part of the master score pipeline.
- **`swing_ta_engine.py`:** **manual** Long/Short/Neutral Macro Swing plan for DB/Telegram text — does not feed `calculate_master_score`.

---

## 3. Guardrails and shock application (`engine/score_engine.py`)

### 3.1 Shock regime overlay

Payloads from `_shock_payload` (asset lists and `bull_boost` / `bear_penalty` per `SUPPLY_SHOCK`, `DEMAND_SHOCK`, `FINANCIAL_CRASH` — exact lists in source).

- Impact scaled by **`_shock_decay_multiplier`:** weekly base **`[PARAM] ** Shock_Weeks`** times **VIX linear factor** with breakpoints **`[PARAM]`** / **`[PARAM]`** and endpoint gains **`[PARAM]`** / **`[PARAM]`**.
- Optional mean-reversion disable when price vs SMA**`[PARAM]`** fields say so.
- Added via **`tanh(shock_impact / [PARAM]) * [PARAM]`**, then clamp to configured **1–10** band.

### 3.2 Live penalties (`_apply_live_score_penalties`)

| Rule ID | Condition | Effect (numeric detail `[PARAM]` in code) |
|---------|-----------|--------|
| `LIVE_VIX_RISK_ON_SCALE` | VIX above **`[PARAM]`** and asset in `RISK_ON_ASSETS` | shrink toward mid via factor **`[PARAM]`** |
| `BOND_MOVE_RISK_ON_SCALE` | `BOND_LIQUIDITY_SHOCK` and risk-on | same pattern toward **`[PARAM]`** |
| `MACRO_CURVE_COPPER_STACK` | `INVERTED_10Y_3M` and `COPPER_GOLD_BEARISH` and risk-on | additive **`[PARAM]`** |
| `LIVE_REAL_YIELD_METAL` | real yield above **`[PARAM]`** and metals | additive **`[PARAM]`** |

`RISK_ON_ASSETS` list: see private code. Then clamp to **1–10**.

### 3.3 Seasonality tilt (`_seasonality_quant_adjustment`)

- Price bias and alignment deltas use **`[PARAM]`** each; conflicting adjustment **`[PARAM]`**; clamp **`[±PARAM]`** additive.

### 3.4 Risk parity override (`_risk_regime_override`)

- Stress tiers: breakpoints **`[PARAM]`** with per-tier `max_score` **`[PARAM]`**.  
- Recovery momentum: `max_score += [PARAM]` (cap **`[PARAM]`**).  
- USD stress path and liquidity-crisis position flags as in code (`atr_stop_mult`, `position_size_mult`, `allow_limit_entries`).

### 3.5 Shock meta caps / floors

- USD floor when high stress; supply-asset cap with `supply_buy_block`; risk-asset caps by stress tier; final clamp to **1–10** (see implementation for exact asset name matching).

### 3.6 Data quality provisional shrink

- **`_DATA_QUALITY_PROVISIONAL_SHRINK = [PARAM]`:** `score = mid + (score - mid) * [PARAM]`, clamp to **1–10** (`mid` is **`[PARAM]`** in code).

### 3.7 Guardrail catalog

- `GUARDRAIL_CATALOG` in `engine/score_guardrails.py` documents IDs such as `MACRO_STALE_FALLBACK`, `DATA_QUALITY_*`, `HARD_DATA_GATE_HALT`, `LIVE_VIX_*`, `SHOCK_REGIME_TANH`, `RP_*`, `SHOCK_META_*`, `SCORE_CLAMP_1_10`.

---

## 4. Master score decomposition (`engine/market_macro_engine.py`)

### 4.1 Four-pillar blend (`decompose_quant_master_score`)

Let `f = macro_retention_multiplier_from_shield(shield)`.

- Pillar weights: `w_rms = [PARAM] * f`, `w_gr = [PARAM] * f`, `w_cot = [PARAM]`, `w_rk = [PARAM] + [PARAM] * (1 - f)`

Normalized terms use `_tanh_norm` with per-pillar scales **`[PARAM]`** (see private code).

\[
\text{blend} = w_{\text{cot}} \cdot \text{cot} + w_{\text{rms}} \cdot \text{rms\_n} + w_{\text{gr}} \cdot \text{gr\_n} + w_{\text{rk}} \cdot \text{rk\_n}
\]

\[
\text{base\_score\_1\_10} = a + b \cdot \tanh(c \cdot \text{blend}) \quad (a,b,c = \texttt{[PARAM]})
\]

`combine_quant_master_score` returns the same `base_score_1_10` scalar.

### 4.2 Failure mode

- Missing core COT fields (`Spec_Z`, `Comm_COT_Idx`, or `Spec_COT_Idx`): returns the same **neutral sentinel pair** `( [PARAM], [PARAM] )` and decomposition may be absent.

### 4.3 Bias and verdict strings (`engine/score_engine.py`)

- `get_bias_text` / `get_verdict`: score bands and shock-specific branches as implemented (labels are **UI semantics**, not trading instructions).

---

## 5. Output channels

### 5.1 Web UI (Next.js SaaS terminal)

- **UI implementation handled by the decoupled Next.js frontend** (private): builds bundles, calls `calculate_master_score` with optional `extras_out` for decomposition JSON.  
- Lineage / as-of: `data_freshness.py` and yield helpers.  
- Rates overlay applied where the UI integrates `apply_rates_overlay`.

### 5.2 Telegram / CLI

- `quant_notify_cli.py` schedules; `quant_notify_panel.build_quant_panel` assembles rows; `telegram_engine` sends HTML with escaping.

### 5.3 LLM (`engine/quant_llm_context.py`, `engine/gemini_quant.py`)

- Structured `QUANT_SYNTH_*` blocks bind narrative to numeric outputs.  
- Default Gemini model name resolved from env / secrets (`DEFAULT_GEMINI_MODEL` in `gemini_quant.py`); retries on timeout/504-class errors.

### 5.4 X / Twitter

**Not implemented.**

---

## Appendix: primary file index

| Topic | Files |
|-------|--------|
| Master score & guardrails | `score_engine.py`, `score_guardrails.py`, `score_invoke.py` |
| Pillar blend | `market_macro_engine.py` |
| COT features | `cot_engine.py`, `cot_cftc_constants.py` |
| Shield + MOVE | `shield_engine.py` |
| Yields | `yield_engine.py` |
| VIX thresholds | `vix_semantics.py` |
| Shock caps | `shock_detector.py` |
| Rates overlay | `rates_overlay.py` |
| DQ + hard gate | `data_quality_gate.py`, `hard_data_gate.py` |
| Persistence | `db_backend.py`, `db_engine.py`, `pg_schema.py` |
| Portable SQL | `cot_sql.py`, `cot_postgres_columns.py` |
| Correlation / Z-spread | `correlation_utils.py`, `lookback_engine.py` |
| Macro Swing UI | `swing_ta_engine.py` (**UI implementation handled by the decoupled Next.js frontend**) |

---

*In the **private** monorepo, update this document when changing referenced modules or thresholds. In **this showcase** tree, run `pytest tests/` (subset of infrastructure tests only).*
