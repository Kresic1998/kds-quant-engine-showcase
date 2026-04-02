from engine.audit_metrics import compute_directional_excursions


def test_long_win_on_mfe_only():
    entry = 100.0
    p_min = 99.0
    p_max = 103.0  # MFE 3% > 2%
    p_close = 100.5  # return small positive actually 0.5% > 0 -> win anyway
    ret, mfe, mae, er, win = compute_directional_excursions(entry, p_min, p_max, p_close, "LONG")
    assert win == 1
    assert mfe >= 2.0


def test_long_efficiency_win_mfe_despite_flat_close():
    entry = 100.0
    p_min = 99.0
    p_max = 102.5  # MFE 2.5% > 2%
    p_close = 99.8  # negative return
    ret, mfe, mae, er, win = compute_directional_excursions(entry, p_min, p_max, p_close, "LONG")
    assert win == 1
    assert er == mfe / max(mae, 0.001)
