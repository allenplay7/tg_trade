"""Streamlit dashboard with two pages:

  Live      - reads bot.sqlite3 to show what the running bot is doing
              (open positions, recent signals, realized PnL, equity curve)
  Backtest  - reads backtest_results.csv + backtest_metrics.json from a
              previous backtest run

Launch:
    streamlit run dashboard.py
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parent
DB_PATH = PROJECT_ROOT / "bot.sqlite3"
BACKTEST_RESULTS = PROJECT_ROOT / "backtest_results.csv"
BACKTEST_METRICS = PROJECT_ROOT / "backtest_metrics.json"

st.set_page_config(page_title="Signal Bot", layout="wide", page_icon=":chart_with_upwards_trend:")

page = st.sidebar.radio("View", ["Live", "Backtest"])
st.sidebar.markdown("---")
st.sidebar.caption("Refresh: ⌘R / Ctrl-R")


# =============================================================================
# Live
# =============================================================================

def _read_db(query: str, params=()) -> pd.DataFrame:
    if not DB_PATH.exists():
        return pd.DataFrame()
    conn = sqlite3.connect(DB_PATH)
    try:
        return pd.read_sql_query(query, conn, params=params)
    finally:
        conn.close()


def page_live() -> None:
    st.title("Live Bot Status")

    if not DB_PATH.exists():
        st.warning("No bot.sqlite3 yet - run the bot at least once.")
        return

    # Top-line metrics
    open_pos = _read_db(
        "SELECT * FROM positions WHERE status='OPEN' ORDER BY opened_ts DESC"
    )
    closed_pos = _read_db(
        "SELECT * FROM positions WHERE status='CLOSED' ORDER BY closed_ts DESC"
    )
    error_pos = _read_db(
        "SELECT * FROM positions WHERE status='ERROR' ORDER BY opened_ts DESC"
    )
    signals = _read_db(
        "SELECT * FROM signals ORDER BY ts DESC LIMIT 100"
    )

    total_pnl = float(closed_pos["pnl_quote"].sum()) if not closed_pos.empty else 0.0
    win_count = int((closed_pos["pnl_quote"] > 0).sum()) if not closed_pos.empty else 0
    loss_count = int((closed_pos["pnl_quote"] < 0).sum()) if not closed_pos.empty else 0
    total_closed = win_count + loss_count

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Realized PnL (USDT)", f"{total_pnl:+.4f}")
    c2.metric("Open positions", len(open_pos))
    c3.metric("Closed trades", len(closed_pos))
    c4.metric(
        "Win rate",
        f"{(win_count / total_closed * 100):.1f}%" if total_closed > 0 else "n/a",
    )
    c5.metric("Errored trades", len(error_pos))

    st.markdown("---")

    # Equity curve from closed positions
    if not closed_pos.empty:
        eq = closed_pos.copy()
        eq["closed_dt"] = pd.to_datetime(eq["closed_ts"], unit="s", utc=True)
        eq = eq.sort_values("closed_dt")
        eq["cum_pnl"] = eq["pnl_quote"].cumsum()

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=eq["closed_dt"], y=eq["cum_pnl"],
            mode="lines+markers", name="Cumulative PnL",
            line=dict(width=2),
        ))
        fig.update_layout(
            height=320,
            margin=dict(l=10, r=10, t=30, b=10),
            title="Cumulative realized PnL (USDT)",
            xaxis_title="", yaxis_title="USDT",
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No closed trades yet. PnL chart will appear once trades complete.")

    # Open positions
    st.subheader("Open positions")
    if open_pos.empty:
        st.caption("None.")
    else:
        df = open_pos.copy()
        df["opened"] = pd.to_datetime(df["opened_ts"], unit="s", utc=True)
        st.dataframe(
            df[["opened", "ticker", "symbol", "qty", "avg_entry", "tp_price",
                "sl_price", "oco_order_list_id", "notes"]],
            use_container_width=True,
            hide_index=True,
        )

    # Recent closed trades
    st.subheader("Closed trades")
    if closed_pos.empty:
        st.caption("None yet.")
    else:
        df = closed_pos.copy()
        df["opened"] = pd.to_datetime(df["opened_ts"], unit="s", utc=True)
        df["closed"] = pd.to_datetime(df["closed_ts"], unit="s", utc=True)
        df["duration_h"] = (df["closed"] - df["opened"]).dt.total_seconds() / 3600
        df = df[["opened", "closed", "ticker", "symbol", "qty", "avg_entry",
                 "tp_price", "sl_price", "pnl_quote", "duration_h", "notes"]]
        st.dataframe(df, use_container_width=True, hide_index=True)

    # Errored positions - these need manual review
    if not error_pos.empty:
        st.subheader(":warning: Errored positions (need manual review)")
        df = error_pos.copy()
        df["opened"] = pd.to_datetime(df["opened_ts"], unit="s", utc=True)
        st.dataframe(
            df[["opened", "ticker", "symbol", "qty", "avg_entry", "notes"]],
            use_container_width=True,
            hide_index=True,
        )

    # Recent signal feed
    st.subheader("Recent signals (last 100)")
    if signals.empty:
        st.caption("None.")
    else:
        df = signals.copy()
        df["received"] = pd.to_datetime(df["ts"], unit="s", utc=True)
        df = df[["received", "kind", "ticker", "raw_text"]].head(100)
        st.dataframe(df, use_container_width=True, hide_index=True)


# =============================================================================
# Backtest
# =============================================================================

def page_backtest() -> None:
    st.title("Backtest Results")

    if not BACKTEST_METRICS.exists() or not BACKTEST_RESULTS.exists():
        st.warning(
            "No backtest data yet. Run:\n\n"
            "1. `python backfill_signals.py --days 30` (or use `run_backtest.bat`)\n"
            "2. `python backtest.py`\n\n"
            "Then refresh this page."
        )
        return

    metrics = json.loads(BACKTEST_METRICS.read_text())
    trades = pd.read_csv(BACKTEST_RESULTS, parse_dates=[
        "signal_time", "entry_time", "exit_time"
    ])

    # Headline metric cards
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Win rate", f"{metrics.get('win_rate_pct', 0):.1f}%")
    pf = metrics.get("profit_factor")
    c2.metric("Profit factor",
              f"{pf:.2f}" if pf is not None else ">>",
              help="Gross profit / Gross loss. Anything < 1 means losing.")
    c3.metric("Max drawdown",
              f"{metrics.get('max_drawdown_pct', 0):.2f}%",
              delta=f"{metrics.get('max_drawdown_usdt', 0):.2f} USDT",
              delta_color="inverse")
    c4.metric("Sharpe (annualized)",
              f"{metrics.get('sharpe_annualized', 0):.2f}")

    c5, c6, c7, c8 = st.columns(4)
    c5.metric("Total trades", metrics.get("total_trades", 0))
    c6.metric("Wins / Losses",
              f"{metrics.get('wins', 0)} / {metrics.get('losses', 0)}")
    c7.metric("Net PnL", f"{metrics.get('net_pnl_usdt', 0):+.2f} USDT")
    c8.metric("Avg duration", f"{metrics.get('avg_duration_hours', 0):.1f} h")

    st.markdown("---")

    # Equity curve
    if not trades.empty:
        eq = trades.sort_values("exit_time").copy()
        eq["cum_pnl"] = eq["pnl_usdt"].cumsum()
        eq["equity"] = metrics.get("init_cash", 100.0) + eq["cum_pnl"]
        running_max = eq["equity"].cummax()
        eq["drawdown_pct"] = (eq["equity"] - running_max) / running_max * 100

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=eq["exit_time"], y=eq["equity"],
            mode="lines", name="Equity",
            line=dict(width=2),
        ))
        fig.add_trace(go.Scatter(
            x=eq["exit_time"], y=running_max,
            mode="lines", name="High-water mark",
            line=dict(width=1, dash="dot"),
        ))
        fig.update_layout(
            height=350, title="Equity curve (USDT)",
            margin=dict(l=10, r=10, t=40, b=10),
            xaxis_title="", yaxis_title="Equity",
        )
        st.plotly_chart(fig, use_container_width=True)

        # Drawdown chart
        fig_dd = go.Figure()
        fig_dd.add_trace(go.Scatter(
            x=eq["exit_time"], y=eq["drawdown_pct"],
            mode="lines", fill="tozeroy", name="Drawdown",
            line=dict(width=1, color="red"),
        ))
        fig_dd.update_layout(
            height=220, title="Drawdown (%)",
            margin=dict(l=10, r=10, t=40, b=10),
            xaxis_title="", yaxis_title="%",
        )
        st.plotly_chart(fig_dd, use_container_width=True)

        # Per-outcome breakdown
        c1, c2 = st.columns(2)
        with c1:
            outcome_counts = trades["outcome"].value_counts().reset_index()
            outcome_counts.columns = ["outcome", "count"]
            fig_outcome = px.pie(outcome_counts, names="outcome", values="count",
                                 title="Outcome distribution")
            st.plotly_chart(fig_outcome, use_container_width=True)
        with c2:
            top_winners = trades.nlargest(10, "pnl_usdt")[
                ["ticker", "outcome", "pnl_usdt", "duration_h"]
            ]
            top_losers = trades.nsmallest(10, "pnl_usdt")[
                ["ticker", "outcome", "pnl_usdt", "duration_h"]
            ]
            st.markdown("**Top 5 winners**")
            st.dataframe(top_winners.head(5), use_container_width=True, hide_index=True)
            st.markdown("**Top 5 losers**")
            st.dataframe(top_losers.head(5), use_container_width=True, hide_index=True)

    # Full trade table
    st.subheader("All trades")
    show = trades.copy()
    show["pnl_usdt"] = show["pnl_usdt"].round(4)
    show["pnl_pct"] = (show["pnl_pct"] * 100).round(2)
    show["duration_h"] = show["duration_h"].round(1)
    cols = ["signal_time", "ticker", "symbol", "outcome", "entry_price",
            "exit_price", "tp_price", "sl_price", "pnl_pct", "pnl_usdt",
            "duration_h", "is_risky"]
    st.dataframe(show[cols], use_container_width=True, hide_index=True)

    # Raw metrics dump
    with st.expander("Raw metrics JSON"):
        st.json(metrics)


# =============================================================================
# Router
# =============================================================================

if page == "Live":
    page_live()
else:
    page_backtest()
