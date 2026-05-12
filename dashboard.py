"""Streamlit dashboard - strategy-comparison edition.

Pages (sidebar radio):
  Live      - reads bot.sqlite3
  Backtest  - reads backtest_results.csv + backtest_metrics.json
              Shows all strategies side-by-side, then drill-down for one.
  Cache     - signals_cache.jsonl viewer

Launch:  streamlit run dashboard.py
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parent
DB_PATH = PROJECT_ROOT / "bot.sqlite3"
BACKTEST_RESULTS = PROJECT_ROOT / "backtest_results.csv"
BACKTEST_METRICS = PROJECT_ROOT / "backtest_metrics.json"
SIGNALS_CACHE = PROJECT_ROOT / "signals_cache.jsonl"

st.set_page_config(page_title="Signal Bot", layout="wide",
                    page_icon=":chart_with_upwards_trend:")
page = st.sidebar.radio("View", ["Live", "Backtest", "Cache"])
st.sidebar.markdown("---")
st.sidebar.caption("Refresh: Ctrl-R")


# =============================================================================
# Helpers
# =============================================================================

def _read_db(query: str, params=()) -> pd.DataFrame:
    if not DB_PATH.exists():
        return pd.DataFrame()
    conn = sqlite3.connect(DB_PATH)
    try:
        return pd.read_sql_query(query, conn, params=params)
    finally:
        conn.close()


def fmt_pair(usdt, aud_rate):
    if usdt is None:
        return "n/a"
    if aud_rate:
        return f"{usdt:+.2f} USDT  |  {usdt * aud_rate:+.2f} AUD"
    return f"{usdt:+.2f} USDT"


# =============================================================================
# Live page
# =============================================================================

def page_live():
    st.title("Live Bot Status")
    if not DB_PATH.exists():
        st.warning("No bot.sqlite3 yet - run the bot at least once.")
        return
    open_pos = _read_db("SELECT * FROM positions WHERE status='OPEN' ORDER BY opened_ts DESC")
    closed_pos = _read_db("SELECT * FROM positions WHERE status='CLOSED' ORDER BY closed_ts DESC")
    error_pos = _read_db("SELECT * FROM positions WHERE status='ERROR' ORDER BY opened_ts DESC")
    signals = _read_db("SELECT * FROM signals ORDER BY ts DESC LIMIT 200")

    total_pnl = float(closed_pos["pnl_quote"].sum()) if not closed_pos.empty else 0.0
    win = int((closed_pos["pnl_quote"] > 0).sum()) if not closed_pos.empty else 0
    loss = int((closed_pos["pnl_quote"] < 0).sum()) if not closed_pos.empty else 0
    total_closed = win + loss

    aud_rate = None
    if BACKTEST_METRICS.exists():
        try:
            aud_rate = json.loads(BACKTEST_METRICS.read_text()).get("usdt_to_aud")
        except Exception:
            pass

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Realized PnL", fmt_pair(total_pnl, aud_rate))
    c2.metric("Open positions", len(open_pos))
    c3.metric("Closed trades", len(closed_pos))
    c4.metric("Win rate", f"{(win/total_closed*100):.1f}%" if total_closed else "n/a")
    c5.metric("Errored", len(error_pos))
    st.markdown("---")

    if not closed_pos.empty:
        eq = closed_pos.copy()
        eq["closed_dt"] = pd.to_datetime(eq["closed_ts"], unit="s", utc=True)
        eq = eq.sort_values("closed_dt")
        eq["cum_pnl"] = eq["pnl_quote"].cumsum()
        fig = go.Figure(go.Scatter(x=eq["closed_dt"], y=eq["cum_pnl"],
                                     mode="lines+markers", name="Cum PnL"))
        fig.update_layout(height=320, title="Cumulative realized PnL (USDT)",
                          margin=dict(l=10, r=10, t=30, b=10))
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Open positions")
    if open_pos.empty:
        st.caption("None.")
    else:
        df = open_pos.copy()
        df["opened"] = pd.to_datetime(df["opened_ts"], unit="s", utc=True)
        st.dataframe(df[["opened", "ticker", "symbol", "qty", "avg_entry",
                          "tp_price", "sl_price", "notes"]],
                      use_container_width=True, hide_index=True)

    st.subheader("Closed trades")
    if closed_pos.empty:
        st.caption("None yet.")
    else:
        df = closed_pos.copy()
        df["opened"] = pd.to_datetime(df["opened_ts"], unit="s", utc=True)
        df["closed"] = pd.to_datetime(df["closed_ts"], unit="s", utc=True)
        df["duration_h"] = (df["closed"] - df["opened"]).dt.total_seconds() / 3600
        st.dataframe(df[["opened", "closed", "ticker", "qty", "avg_entry",
                          "tp_price", "sl_price", "pnl_quote", "duration_h"]],
                      use_container_width=True, hide_index=True)

    if not error_pos.empty:
        st.subheader(":warning: Errored positions")
        df = error_pos.copy()
        df["opened"] = pd.to_datetime(df["opened_ts"], unit="s", utc=True)
        st.dataframe(df[["opened", "ticker", "qty", "avg_entry", "notes"]],
                      use_container_width=True, hide_index=True)

    st.subheader("Recent signals (last 100)")
    if signals.empty:
        st.caption("None.")
    else:
        df = signals.copy()
        df["received"] = pd.to_datetime(df["ts"], unit="s", utc=True)
        st.dataframe(df[["received", "kind", "ticker", "raw_text"]].head(100),
                      use_container_width=True, hide_index=True)


# =============================================================================
# Backtest page - strategy comparison + drill-down
# =============================================================================

def page_backtest():
    st.title("Backtest - Strategy Comparison")
    if not BACKTEST_METRICS.exists() or not BACKTEST_RESULTS.exists():
        st.warning("No backtest data yet. Run backfill_signals.py then backtest.py.")
        return

    metrics = json.loads(BACKTEST_METRICS.read_text())
    trades = pd.read_csv(BACKTEST_RESULTS, parse_dates=[
        "signal_time", "entry_time", "exit_time"
    ])

    strategies_dict = metrics.get("strategies", {})
    if not strategies_dict:
        # Backward compatibility: an older single-strategy metrics file
        st.error("This metrics file isn't multi-strategy. Re-run backtest.py.")
        st.json(metrics)
        return

    aud_rate = metrics.get("usdt_to_aud")
    init_cash = metrics.get("init_cash", 100.0)
    pos_size = metrics.get("position_size_usdt", 0)
    fee_bps = metrics.get("fee_bps", 20)

    cap = (f"Position size: {pos_size} USDT  |  fees={fee_bps} bps  |  "
           f"init={init_cash} USDT")
    if aud_rate:
        cap += f"  |  1 USDT = {aud_rate:.4f} AUD"
    src_counts = metrics.get("data_source_counts", {})
    if src_counts:
        src_str = ", ".join(f"{k}={v}" for k, v in src_counts.items())
        cap += f"  |  Data: {src_str}"
    st.caption(cap)

    # --- Strategy summary table ---
    st.subheader("Strategy summary")
    summary_rows = []
    for name, m in strategies_dict.items():
        pf = m.get("profit_factor")
        summary_rows.append({
            "Strategy": name,
            "Label": m.get("label", ""),
            "Trades": m.get("total_trades", 0),
            "Win rate %": round(m.get("win_rate_pct", 0), 1),
            "Profit factor": round(pf, 3) if pf is not None else "inf",
            "Net PnL (USDT)": round(m.get("net_pnl_usdt", 0), 2),
            "Net PnL (AUD)": (round(m.get("net_pnl_usdt", 0) * aud_rate, 2)
                                if aud_rate else None),
            "Max DD %": round(m.get("max_drawdown_pct", 0), 2),
            "Sharpe": round(m.get("sharpe_annualized", 0), 2),
            "Avg dur (h)": round(m.get("avg_duration_hours", 0), 1),
            "Fees (USDT)": round(m.get("total_fees_usdt", 0), 2),
        })
    summary_df = pd.DataFrame(summary_rows)
    st.dataframe(summary_df, use_container_width=True, hide_index=True)

    # --- Best-of badges ---
    best = metrics.get("best_by", {})
    if best:
        cols = st.columns(4)
        cols[0].metric("Best Net PnL", best.get("net_pnl_usdt", "n/a"))
        cols[1].metric("Best Profit Factor", best.get("profit_factor", "n/a"))
        cols[2].metric("Best Sharpe", best.get("sharpe_annualized", "n/a"))
        cols[3].metric("Smallest drawdown", best.get("max_drawdown_pct", "n/a"))

    st.markdown("---")

    # --- Overlaid equity curves ---
    st.subheader("Equity curves (all strategies)")
    fig_eq = go.Figure()
    for name in strategies_dict.keys():
        sub = trades[trades["strategy"] == name].sort_values("exit_time").copy()
        if sub.empty:
            continue
        sub["cum_pnl"] = sub["pnl_usdt"].cumsum()
        sub["equity"] = init_cash + sub["cum_pnl"]
        fig_eq.add_trace(go.Scatter(x=sub["exit_time"], y=sub["equity"],
                                      mode="lines", name=name))
    fig_eq.update_layout(height=360,
                          title="Equity over time (USDT, starting from init cash)",
                          margin=dict(l=10, r=10, t=40, b=10),
                          yaxis_title="Equity (USDT)")
    st.plotly_chart(fig_eq, use_container_width=True)

    # --- Overlaid drawdowns ---
    st.subheader("Drawdown comparison")
    fig_dd = go.Figure()
    for name in strategies_dict.keys():
        sub = trades[trades["strategy"] == name].sort_values("exit_time").copy()
        if sub.empty:
            continue
        sub["cum_pnl"] = sub["pnl_usdt"].cumsum()
        sub["equity"] = init_cash + sub["cum_pnl"]
        running_max = sub["equity"].cummax()
        sub["dd_pct"] = (sub["equity"] - running_max) / running_max * 100
        fig_dd.add_trace(go.Scatter(x=sub["exit_time"], y=sub["dd_pct"],
                                      mode="lines", name=name, fill="tonexty"))
    fig_dd.update_layout(height=300, title="Drawdown over time (%)",
                          margin=dict(l=10, r=10, t=40, b=10),
                          yaxis_title="Drawdown %")
    st.plotly_chart(fig_dd, use_container_width=True)

    # --- Net PnL bar chart ---
    st.subheader("Per-strategy bar comparison")
    c1, c2 = st.columns(2)
    with c1:
        fig_b = go.Figure(go.Bar(
            x=summary_df["Strategy"], y=summary_df["Net PnL (USDT)"],
            marker_color=["green" if v >= 0 else "red"
                            for v in summary_df["Net PnL (USDT)"]],
            text=summary_df["Net PnL (USDT)"], textposition="outside",
        ))
        fig_b.update_layout(height=300, title="Net PnL (USDT)",
                              margin=dict(l=10, r=10, t=40, b=10))
        st.plotly_chart(fig_b, use_container_width=True)
    with c2:
        fig_w = go.Figure(go.Bar(
            x=summary_df["Strategy"], y=summary_df["Win rate %"],
            marker_color="steelblue",
            text=summary_df["Win rate %"], textposition="outside",
        ))
        fig_w.update_layout(height=300, title="Win rate %",
                              margin=dict(l=10, r=10, t=40, b=10),
                              yaxis_title="%")
        st.plotly_chart(fig_w, use_container_width=True)

    st.markdown("---")

    # --- Drill-down for one strategy ---
    st.subheader("Drill-down: one strategy")
    picked = st.selectbox("Pick a strategy to inspect",
                           list(strategies_dict.keys()),
                           index=0)
    sub = trades[trades["strategy"] == picked].copy()
    if sub.empty:
        st.info("No trades for this strategy.")
        return
    m = strategies_dict[picked]
    st.caption(m.get("label", ""))

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Win rate", f"{m.get('win_rate_pct', 0):.1f}%")
    pf = m.get("profit_factor")
    c2.metric("Profit factor",
                f"{pf:.2f}" if pf is not None else "inf")
    c3.metric("Max DD", f"{m.get('max_drawdown_pct', 0):.2f}%")
    c4.metric("Sharpe", f"{m.get('sharpe_annualized', 0):.2f}")
    c5.metric("Net PnL", fmt_pair(m.get("net_pnl_usdt", 0), aud_rate))

    # Outcome pie
    c1, c2 = st.columns([1, 2])
    with c1:
        oc = sub["outcome"].value_counts().reset_index()
        oc.columns = ["outcome", "count"]
        fig = px.pie(oc, names="outcome", values="count", hole=0.4,
                       title="Outcome distribution")
        fig.update_layout(height=320, margin=dict(l=10, r=10, t=40, b=10))
        st.plotly_chart(fig, use_container_width=True)
    with c2:
        # Performance by call type
        rows = []
        for label, mask in [
            ("Risky calls", sub["is_risky"] == True),
            ("Non-risky", sub["is_risky"] == False),
        ]:
            seg = sub[mask]
            if seg.empty:
                rows.append((label, 0, "n/a", "n/a", "n/a"))
                continue
            seg_w = seg[seg["pnl_usdt"] > 0]
            rows.append((
                label, len(seg),
                f"{len(seg_w)/len(seg)*100:.1f}%",
                f"{seg['pnl_usdt'].sum():+.2f} USDT",
                f"{seg['duration_h'].mean():.1f} h",
            ))
        st.markdown("**Performance by call type**")
        st.dataframe(pd.DataFrame(rows, columns=[
            "Segment", "N", "Win rate", "Net PnL", "Avg duration"
        ]), use_container_width=True, hide_index=True)

    # Duration histogram
    st.markdown("**Duration: winners vs losers**")
    wins_df = sub[sub["pnl_usdt"] > 0]
    losses_df = sub[sub["pnl_usdt"] < 0]
    fig_h = go.Figure()
    if not wins_df.empty:
        fig_h.add_trace(go.Histogram(x=wins_df["duration_h"], name="Wins",
                                        opacity=0.7, nbinsx=20))
    if not losses_df.empty:
        fig_h.add_trace(go.Histogram(x=losses_df["duration_h"], name="Losses",
                                        opacity=0.7, nbinsx=20))
    fig_h.update_layout(height=300, barmode="overlay",
                         xaxis_title="Hours held",
                         yaxis_title="Number of trades",
                         margin=dict(l=10, r=10, t=10, b=10))
    st.plotly_chart(fig_h, use_container_width=True)

    if not wins_df.empty:
        p50 = float(wins_df["duration_h"].quantile(0.5))
        p90 = float(wins_df["duration_h"].quantile(0.9))
        p95 = float(wins_df["duration_h"].quantile(0.95))
        cur = metrics.get("max_hold_days", 14)
        st.markdown(
            f"**Winning-trade duration**: 50th pct = {p50:.1f}h, "
            f"90th pct = {p90:.1f}h, 95th pct = {p95:.1f}h "
            f"(current MAX_HOLD_DAYS = {cur} days = {cur*24}h)"
        )
        if p95 / 24 < cur * 0.4:
            st.info(
                f"Suggestion: 95% of wins finish in {p95/24:.1f} days; "
                f"MAX_HOLD_DAYS={cur} is much larger - shortening could free up "
                f"capital without losing many wins."
            )

    # PnL by ticker
    st.markdown("**PnL by ticker (top 8 + bottom 8)**")
    by_t = (sub.groupby("ticker")["pnl_usdt"]
              .agg(["sum", "count"])
              .reset_index()
              .rename(columns={"sum": "net_pnl", "count": "n"})
              .sort_values("net_pnl", ascending=False))
    top = pd.concat([by_t.head(8), by_t.tail(8)]).drop_duplicates()
    fig_t = go.Figure(go.Bar(
        x=top["ticker"], y=top["net_pnl"],
        marker_color=["green" if v >= 0 else "red" for v in top["net_pnl"]],
        text=top["n"].apply(lambda n: f"n={n}"), textposition="outside",
    ))
    fig_t.update_layout(height=320, margin=dict(l=10, r=10, t=10, b=10),
                          yaxis_title="USDT")
    st.plotly_chart(fig_t, use_container_width=True)

    # Day of week
    st.markdown("**Day-of-week breakdown**")
    dow = sub.copy()
    dow["dow"] = pd.to_datetime(dow["signal_time"]).dt.day_name()
    dsum = (dow.groupby("dow")["pnl_usdt"]
              .agg(["sum", "count"])
              .reset_index()
              .rename(columns={"sum": "net", "count": "n"}))
    order = ["Monday", "Tuesday", "Wednesday", "Thursday",
             "Friday", "Saturday", "Sunday"]
    dsum["dow"] = pd.Categorical(dsum["dow"], categories=order, ordered=True)
    dsum = dsum.sort_values("dow")
    fig_d = go.Figure(go.Bar(
        x=dsum["dow"], y=dsum["net"],
        marker_color=["green" if v >= 0 else "red" for v in dsum["net"]],
        text=dsum["n"].apply(lambda n: f"n={n}"), textposition="outside",
    ))
    fig_d.update_layout(height=300, margin=dict(l=10, r=10, t=10, b=10),
                         yaxis_title="USDT")
    st.plotly_chart(fig_d, use_container_width=True)

    # Top winners and losers
    c1, c2 = st.columns(2)
    show_cols = ["ticker", "outcome", "entry_price", "first_tp_price",
                 "sl_price", "tps_hit", "pnl_usdt", "duration_h"]
    with c1:
        st.markdown("**Top 5 winners**")
        wd = sub.nlargest(5, "pnl_usdt")[show_cols].copy()
        wd["pnl_usdt"] = wd["pnl_usdt"].round(3)
        wd["duration_h"] = wd["duration_h"].round(1)
        st.dataframe(wd, use_container_width=True, hide_index=True)
    with c2:
        st.markdown("**Top 5 losers**")
        ld = sub.nsmallest(5, "pnl_usdt")[show_cols].copy()
        ld["pnl_usdt"] = ld["pnl_usdt"].round(3)
        ld["duration_h"] = ld["duration_h"].round(1)
        st.dataframe(ld, use_container_width=True, hide_index=True)

    # All trades
    st.markdown("**All trades for this strategy**")
    show = sub.copy()
    show["pnl_pct"] = (show["pnl_pct"] * 100).round(2)
    show["pnl_usdt"] = show["pnl_usdt"].round(4)
    if aud_rate:
        show["pnl_aud"] = (show["pnl_usdt"] * aud_rate).round(4)
    show["duration_h"] = show["duration_h"].round(1)
    cols = ["signal_time", "ticker", "symbol", "data_source", "outcome",
            "is_risky", "entry_price", "first_tp_price", "sl_price",
            "tps_hit", "tps_in_signal", "pnl_pct", "pnl_usdt"]
    if aud_rate:
        cols.append("pnl_aud")
    cols += ["duration_h"]
    cols = [c for c in cols if c in show.columns]
    st.dataframe(show[cols], use_container_width=True, hide_index=True)

    with st.expander("Raw metrics JSON (full)"):
        st.json(metrics)


# =============================================================================
# Cache page
# =============================================================================

def page_cache():
    st.title("Signals Cache")
    if not SIGNALS_CACHE.exists():
        st.warning("No signals_cache.jsonl yet.")
        return
    rows = []
    with SIGNALS_CACHE.open(encoding="utf-8") as f:
        for line in f:
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    if not rows:
        st.info("Cache empty.")
        return
    df = pd.DataFrame(rows)
    if "time" in df.columns:
        df["time"] = pd.to_datetime(df["time"], utc=True, errors="coerce")
    st.caption(f"{len(df)} cached messages. "
                f"Range: {df['time'].min()} to {df['time'].max()}")
    counts = df["kind"].value_counts() if "kind" in df.columns else {}
    c1, c2, c3 = st.columns(3)
    c1.metric("Total", len(df))
    c2.metric("ENTRY", int(counts.get("SignalKind.ENTRY", 0)))
    c3.metric("Other", len(df) - int(counts.get("SignalKind.ENTRY", 0)))
    show = df.sort_values("time", ascending=False).head(200)
    cols = [c for c in ["time", "kind", "ticker", "text"] if c in show.columns]
    st.dataframe(show[cols], use_container_width=True, hide_index=True)


# =============================================================================
# Router
# =============================================================================

if page == "Live":
    page_live()
elif page == "Backtest":
    page_backtest()
else:
    page_cache()
