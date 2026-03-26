"""
punish_weights.py

Calculates daily equal-weight long positions for 處置股 strategy.

Strategy rules:
  - LONG all 處置股 from announce_date through (start_date + 6 calendar days) inclusive
  - Equal weight across all active positions, rebalanced daily
  - Weight = 1 / N  (N = number of active stocks on that day)

Usage:
    from punish_weights import get_weights_for_date, get_weights_range

    # Single day
    weights = get_weights_for_date(date(2026, 3, 26))
    # → {'6515': 0.1, '2337': 0.1, ...}

    # Range of dates (returns DataFrame)
    df = get_weights_range(date(2026, 1, 1), date(2026, 3, 26))
"""

from __future__ import annotations

import os
from datetime import date, timedelta

import pandas as pd
import psycopg2
import psycopg2.extras

# ── DB config ─────────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host":     os.environ.get("DB_HOST", "quant-db.cluster-c1igmy0yu89z.ap-northeast-1.rds.amazonaws.com"),
    "port":     int(os.environ.get("DB_PORT", "5432")),
    "dbname":   os.environ.get("DB_NAME", "quant_data"),
    "user":     os.environ.get("DB_USER", "quant_master"),
    "password": os.environ.get("DB_PASSWORD", "e74G2UWuxTDYr1j5Mtf7"),
}

HOLD_DAYS_AFTER_START = 6  # hold through start_date + N calendar days


def _get_conn():
    return psycopg2.connect(**DB_CONFIG, connect_timeout=10)


def _load_punish_records(start_window: date, end_window: date) -> pd.DataFrame:
    """
    Load 處置股 records whose holding window overlaps [start_window, end_window].

    Holding window for each record:
        entry = announce_date
        exit  = start_date + HOLD_DAYS_AFTER_START
    """
    conn = _get_conn()
    try:
        query = """
            SELECT
                announce_date,
                stock_code,
                stock_name,
                start_date,
                (start_date + %(hold_days)s * INTERVAL '1 day')::date AS exit_date
            FROM tw_punish_stocks
            WHERE
                start_date IS NOT NULL
                AND announce_date IS NOT NULL
                -- record is active on at least one day in our window
                AND announce_date                                          <= %(end_window)s
                AND (start_date + %(hold_days)s * INTERVAL '1 day')::date >= %(start_window)s
        """
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, {
                "hold_days":    HOLD_DAYS_AFTER_START,
                "start_window": start_window,
                "end_window":   end_window,
            })
            rows = cur.fetchall()

        df = pd.DataFrame(rows)
        if df.empty:
            return df
        for col in ["announce_date", "start_date", "exit_date"]:
            df[col] = pd.to_datetime(df[col]).dt.date
        return df
    finally:
        conn.close()


def get_weights_for_date(target_date: date) -> dict[str, float]:
    """
    Return equal-weight long positions for a single date.

    A stock is active on `target_date` if:
        announce_date <= target_date <= start_date + HOLD_DAYS_AFTER_START

    Returns:
        dict mapping stock_code → weight (sums to 1.0, or empty dict if no positions)
    """
    records = _load_punish_records(target_date, target_date)

    active = records[
        (records["announce_date"] <= target_date) &
        (records["exit_date"]     >= target_date)
    ]["stock_code"].unique().tolist()

    if not active:
        return {}

    weight = 1.0 / len(active)
    return {code: round(weight, 6) for code in sorted(active)}


def get_weights_range(
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    """
    Return a DataFrame of daily equal-weight positions for a date range.

    Columns: date | stock_code | stock_name | weight | announce_date | start_date | exit_date

    Rebalanced daily — weight = 1/N where N = active stocks on that day.
    """
    records = _load_punish_records(start_date, end_date)

    if records.empty:
        return pd.DataFrame(columns=["date", "stock_code", "stock_name", "weight",
                                     "announce_date", "start_date", "exit_date"])

    rows = []
    current = start_date
    while current <= end_date:
        active = records[
            (records["announce_date"] <= current) &
            (records["exit_date"]     >= current)
        ].copy()

        if not active.empty:
            n = len(active)
            active["date"]   = current
            active["weight"] = round(1.0 / n, 6)
            rows.append(active[["date", "stock_code", "stock_name", "weight",
                                 "announce_date", "start_date", "exit_date"]])

        current += timedelta(days=1)

    if not rows:
        return pd.DataFrame(columns=["date", "stock_code", "stock_name", "weight",
                                     "announce_date", "start_date", "exit_date"])

    df = pd.concat(rows, ignore_index=True)
    df = df.sort_values(["date", "stock_code"]).reset_index(drop=True)
    return df


# ── Quick sanity check ────────────────────────────────────────────────────────
if __name__ == "__main__":
    from datetime import date

    print("=" * 60)
    print(f"Hold period: announce_date → start_date + {HOLD_DAYS_AFTER_START} days")
    print("=" * 60)

    # Single-day weights
    today = date(2026, 3, 26)
    weights = get_weights_for_date(today)
    print(f"\n📅 Weights for {today}:  ({len(weights)} positions, each = {next(iter(weights.values()), 0):.4f})")
    for code, w in sorted(weights.items()):
        print(f"   {code}  {w:.4f}")

    # Range view — last 7 days
    df = get_weights_range(date(2026, 3, 20), date(2026, 3, 26))
    print(f"\n📊 Range 2026-03-20 ~ 2026-03-26:  {len(df)} position-days")
    print(df.groupby("date")["stock_code"].count().rename("n_positions").to_string())
