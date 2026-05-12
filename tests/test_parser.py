"""Parser tests against real signals from the user's channel sample."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from parser import parse, SignalKind  # noqa: E402


# ---------- ENTRY signals ----------

def test_pha_entry_cmp():
    p = parse("$PHA ENTRY - CMP TP -0.42 -0.46 SL-0.3440")
    assert p.kind == SignalKind.ENTRY
    assert p.ticker == "PHA"
    assert p.entry_is_market is True
    assert p.take_profits[0] == 0.42
    assert p.stop_loss == 0.3440


def test_opg_entry_with_hint_price():
    p = parse("$OPG ENTRY - CMP -0.29 TP -0.365 -0.46 -0.0523 - 0.061 SL - 0.24")
    assert p.kind == SignalKind.ENTRY
    assert p.ticker == "OPG"
    assert p.entry_is_market is True
    assert p.take_profits[0] == 0.365
    assert p.stop_loss == 0.24


def test_bard_entry():
    p = parse("$BARD Entry - CMP TP - 0.295 - 0.31 -0.327 SL - 0.27")
    assert p.kind == SignalKind.ENTRY
    assert p.ticker == "BARD"
    assert p.take_profits[0] == 0.295
    assert p.stop_loss == 0.27


def test_bb_limit_entry_with_target_keyword():
    p = parse("$BB Could make a good move within few days !! Entry - 0.034 Target - 0.04 - 0.042++ Stoploss - 0.027")
    assert p.kind == SignalKind.ENTRY
    assert p.ticker == "BB"
    assert p.entry_is_market is False
    assert p.entry_price == 0.034
    assert p.take_profits[0] == 0.04
    assert p.stop_loss == 0.027


def test_giggle_is_risky():
    p = parse(
        "$GIGGLE Might have a possible outcome !! Entry - cmp Target - 44++ Stoploss - 33.20"
    )
    assert p.kind == SignalKind.ENTRY
    # 'cmp' is lowercase but matches \bcmp\b case-insensitively
    assert p.entry_is_market is True


def test_giggle_hype_call_marked_risky():
    p = parse(
        "$GIGGLE Entry - CMP Target - 44 Stoploss - 33.20\n"
        "This call is not based on TA , it's just based on Hype so invest lowest amount"
    )
    assert p.kind == SignalKind.ENTRY
    assert p.is_risky is True


def test_dash_with_parens():
    p = parse("$DASH Entry ~ (CMP - 36) TP~ 43 - 47 SL ~ 33")
    assert p.kind == SignalKind.ENTRY
    assert p.ticker == "DASH"
    assert p.entry_is_market is True
    assert p.take_profits[0] == 43
    assert p.stop_loss == 33


def test_dollar_4_ticker():
    p = parse("$4 Entry - CMP - 0.012257 Target - 0.016230 - 0.023213++ Stoploss - 0.010803")
    assert p.kind == SignalKind.ENTRY
    assert p.ticker == "4"
    assert p.take_profits[0] == 0.01623


def test_form_pct_targets():
    p = parse("$FORM SPOT CALL ENTRY ~CMP TP~ 100-1400%++ SL ~ 0.18")
    assert p.kind == SignalKind.ENTRY
    assert p.ticker == "FORM"
    assert p.take_profits_are_pct is True
    assert p.take_profits[0] == 100
    assert p.stop_loss == 0.18


def test_aigensyn_multiple_tickers_is_ambiguous():
    p = parse(
        "$AIGENSYN Next $SKYAI SPOT (ALPHA) ENTRY ~ (CMP - 0.33) TP- 60%- 1000%+ SL~ 0.028"
    )
    assert p.kind == SignalKind.AMBIGUOUS
    assert "Multiple tickers" in "; ".join(p.notes)


def test_tilde_separators_zora():
    p = parse("$ZORA Entry ~ CMP TP ~0.0155 - 0.0176 - 0.0197 SL ~ 0.012")
    assert p.kind == SignalKind.ENTRY
    assert p.take_profits[0] == 0.0155
    assert p.stop_loss == 0.012


def test_grass_no_decimal_in_sl():
    p = parse("$GRASS ENTRY ~ CMP TP ~ 0.41 -0.46 SL ~0.3528")
    assert p.kind == SignalKind.ENTRY
    assert p.take_profits[0] == 0.41
    assert p.stop_loss == 0.3528


def test_eth_dollar_suffix():
    p = parse("$ETC Entry - CMP - 9$ Target - 10.5 - 11.5 - 12++ Stoploss - 8.5")
    assert p.kind == SignalKind.ENTRY
    assert p.ticker == "ETC"
    assert p.take_profits[0] == 10.5
    assert p.stop_loss == 8.5


# ---------- SHORT and other rejects ----------

def test_short_rejected():
    p = parse("$BTC SHORT (Might hunting liquidity)")
    assert p.kind == SignalKind.IGNORE
    # Even if it had Entry/TP/SL, short -> IGNORE
    p2 = parse("$BTC SHORT Entry - CMP TP - 60000 SL - 65000")
    assert p2.kind == SignalKind.IGNORE


def test_lone_ticker_is_ignore():
    assert parse("$DOGS").kind == SignalKind.IGNORE
    assert parse("$ONDO").kind == SignalKind.IGNORE


def test_commentary_is_ignore():
    assert parse("should run").kind == SignalKind.IGNORE
    assert parse("Volume increasing").kind == SignalKind.IGNORE
    assert parse("first target crushed").kind == SignalKind.TP_HIT_INFO


# ---------- Management messages ----------

def test_close_with_ticker():
    p = parse("close $ENJ")
    assert p.kind == SignalKind.CLOSE
    assert p.ticker == "ENJ"


def test_close_without_ticker_is_ambiguous():
    p = parse("Close it")
    assert p.kind == SignalKind.AMBIGUOUS


def test_dont_close_isnt_close():
    p = parse("TP 1 hit , don't close all the profits we may go more higher probably $80")
    # "don't close" should not trip; this should look like TP hit info or ignore.
    # The TP-hit detector matches "TP 1 hit" pattern.
    assert p.kind == SignalKind.TP_HIT_INFO


def test_put_sl_with_ticker():
    p = parse("$SXT Put SL -0.01388")
    assert p.kind == SignalKind.UPDATE_SL
    assert p.ticker == "SXT"
    assert p.stop_loss == 0.01388


def test_set_sl_without_ticker_is_ambiguous():
    p = parse("set your stoploss at entry zone now")
    assert p.kind == SignalKind.AMBIGUOUS


if __name__ == "__main__":
    import sys
    import traceback

    tests = [v for k, v in globals().items() if k.startswith("test_") and callable(v)]
    failed = 0
    for t in tests:
        try:
            t()
            print(f"PASS  {t.__name__}")
        except AssertionError as e:
            failed += 1
            print(f"FAIL  {t.__name__}: {e}")
        except Exception:
            failed += 1
            print(f"ERROR {t.__name__}")
            traceback.print_exc()
    total = len(tests)
    print(f"\n{total - failed}/{total} passing")
    sys.exit(1 if failed else 0)
