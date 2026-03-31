#!/usr/bin/env python3
"""
Ingest TPEX disposal data into the disposal table.

Covers two TPEX sources:
  - TPEX-OTC  (上櫃): https://www.tpex.org.tw/zh-tw/announce/market/disposal.html
  - TPEX-ESB  (興櫃): https://www.tpex.org.tw/zh-tw/announce/market/esb-disposal.html

Usage:
    # Full historical backfill (2011-04-01 to today)
    python3 ingest_tpex_disposal.py

    # Specific date range
    python3 ingest_tpex_disposal.py --start 20260101 --end 20260331

    # Single source
    python3 ingest_tpex_disposal.py --source TPEX-OTC
    python3 ingest_tpex_disposal.py --source TPEX-ESB
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
import urllib.request
import urllib.parse
from datetime import datetime, timedelta, timezone, date
from typing import Optional

import psycopg2
import psycopg2.extras
import pandas_market_calendars as mcal

# ── Config ────────────────────────────────────────────────────────────────────
TW_TZ   = timezone(timedelta(hours=8))
TW_CAL  = mcal.get_calendar('XTAI')

DB_CONFIG = dict(
    host="quant-db.cluster-c1igmy0yu89z.ap-northeast-1.rds.amazonaws.com",
    port=5432, dbname="quant_data", user="quant_master", password="e74G2UWuxTDYr1j5Mtf7",
)

TPEX_SOURCES = {
    "TPEX-OTC": "bulletin/disposal",       # 上櫃
    "TPEX-ESB": "bulletin/disposalEsb",    # 興櫃
}
API_BASE = "https://www.tpex.org.tw/www/zh-tw"
HEADERS  = {
    "Content-Type":    "application/x-www-form-urlencoded; charset=UTF-8",
    "Accept":          "application/json, text/javascript, */*; q=0.01",
    "User-Agent":      "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Referer":         "https://www.tpex.org.tw/zh-tw/announce/market/disposal.html",
    "X-Requested-With":"XMLHttpRequest",
}

# TPEX data starts from 2011-04-01
TPEX_START = "20110401"


# ── Date helpers ──────────────────────────────────────────────────────────────
def tw_date_to_iso(s: str) -> Optional[date]:
    try:
        parts = s.strip().split("/")
        return date(int(parts[0]) + 1911, int(parts[1]), int(parts[2]))
    except Exception:
        return None


def parse_period(s: str) -> tuple[Optional[date], Optional[date]]:
    try:
        parts = re.split(r"[～~]", s.strip())
        return tw_date_to_iso(parts[0]), tw_date_to_iso(parts[1]) if len(parts) > 1 else None
    except Exception:
        return None, None


def trading_exit_date(start: date, n: int = 6) -> Optional[date]:
    if not start:
        return None
    try:
        sessions = TW_CAL.valid_days(start_date=start, end_date=start + timedelta(days=60))
        dates = [s.date() for s in sessions]
        idx = dates.index(start) if start in dates else next(i for i, d in enumerate(dates) if d >= start)
        return dates[idx + n]
    except Exception:
        return None


# ── TPEX API ──────────────────────────────────────────────────────────────────
def fetch_tpex(action: str, start_date: str, end_date: str) -> list[dict]:
    url    = f"{API_BASE}/{action}"
    params = urllib.parse.urlencode({
        "startDate": start_date, "endDate": end_date,
        "type": "all", "reason": "-1", "measure": "-1",
        "order": "date", "response": "json",
    }).encode("utf-8")

    req = urllib.request.Request(url, data=params, headers=HEADERS, method="POST")
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read().decode("utf-8"))

    if data.get("stat") != "ok":
        raise RuntimeError(f"API stat={data.get('stat')}")

    tables = data.get("tables", [])
    if not tables or "data" not in tables[0] or tables[0]["data"] is None:
        return []

    fields = tables[0]["fields"]
    rows   = tables[0]["data"]

    records = []
    for row in rows:
        r = dict(zip(fields, row))

        code = str(r.get("證券代號", "")).strip()
        name = re.sub(r"\(.*?\)$", "", str(r.get("證券名稱", ""))).strip()
        if not code or not name:
            continue  # skip "本日無處置資料" rows

        announce_date          = tw_date_to_iso(str(r.get("公布日期", "")))
        start_date_d, end_date = parse_period(str(r.get("處置起訖時間", "")))

        records.append({
            "announce_date": announce_date,
            "stock_code":    code,
            "stock_name":    name,
            "punish_count":  int(r["累計"]) if str(r.get("累計","")).isdigit() else None,
            "condition":     str(r.get("處置原因", "")).strip(),
            "start_date":    start_date_d,
            "end_date":      end_date,
            "exit_date":     trading_exit_date(start_date_d, 6),
            "measure":       None,   # TPEX doesn't have this field
            "content":       str(r.get("處置內容", "")).strip(),
            "remark":        None,
        })
    return records


# ── DB ────────────────────────────────────────────────────────────────────────
def upsert_records(records: list[dict], source: str) -> tuple[int, int]:
    conn = psycopg2.connect(**DB_CONFIG, connect_timeout=10)
    inserted = skipped = 0
    try:
        with conn:
            with conn.cursor() as cur:
                for r in records:
                    # Use WHERE NOT EXISTS to handle NULL start_date correctly
                    # (ON CONFLICT can't handle NULLs in unique key columns reliably)
                    cur.execute("""
                        INSERT INTO disposal
                            (announce_date, stock_code, stock_name, punish_count,
                             condition, start_date, end_date, exit_date,
                             measure, content, remark, source)
                        SELECT %(announce_date)s, %(stock_code)s, %(stock_name)s, %(punish_count)s,
                               %(condition)s, %(start_date)s, %(end_date)s, %(exit_date)s,
                               %(measure)s, %(content)s, %(remark)s, %(source)s
                        WHERE NOT EXISTS (
                            SELECT 1 FROM disposal
                            WHERE announce_date = %(announce_date)s
                              AND stock_code    = %(stock_code)s
                              AND source        = %(source)s
                              AND (
                                  (start_date IS NULL     AND %(start_date)s IS NULL)
                                  OR start_date = %(start_date)s
                              )
                        )
                    """, {**r, "source": source})
                    if cur.rowcount:
                        inserted += 1
                    else:
                        skipped += 1
    finally:
        conn.close()
    return inserted, skipped


# ── Chunked date range ────────────────────────────────────────────────────────
def date_chunks(start: str, end: str, chunk_days: int = 365):
    """Split a date range into chunks to avoid huge API responses."""
    s = datetime.strptime(start, "%Y%m%d").date()
    e = datetime.strptime(end,   "%Y%m%d").date()
    while s <= e:
        chunk_end = min(s + timedelta(days=chunk_days - 1), e)
        yield s.strftime("%Y%m%d"), chunk_end.strftime("%Y%m%d")
        s = chunk_end + timedelta(days=1)


# ── Main ──────────────────────────────────────────────────────────────────────
def ingest_source(source_name: str, action: str, start: str, end: str):
    print(f"\n{'='*60}")
    print(f"Source: {source_name}  ({start} → {end})")
    print(f"{'='*60}")

    total_inserted = total_skipped = 0

    for chunk_start, chunk_end in date_chunks(start, end, chunk_days=365):
        print(f"  Fetching {chunk_start}~{chunk_end} ...", end=" ", flush=True)
        try:
            records = fetch_tpex(action, chunk_start, chunk_end)
            if not records:
                print("0 rows")
                continue
            ins, skip = upsert_records(records, source_name)
            total_inserted += ins
            total_skipped  += skip
            print(f"{len(records)} fetched → inserted={ins} skipped={skip}")
            time.sleep(0.5)   # be polite to TPEX
        except Exception as e:
            print(f"ERROR: {e}")

    print(f"\n  ✅ {source_name} done: inserted={total_inserted} skipped={total_skipped}")
    return total_inserted


def main():
    parser = argparse.ArgumentParser(description="Ingest TPEX disposal data into disposal table")
    parser.add_argument("--start",  default=TPEX_START, help="Start date YYYYMMDD")
    parser.add_argument("--end",    default=None,        help="End date YYYYMMDD (default: today TW)")
    parser.add_argument("--source", default="all",       help="all | TPEX-OTC | TPEX-ESB")
    args = parser.parse_args()

    end = args.end or datetime.now(TW_TZ).strftime("%Y%m%d")

    sources = TPEX_SOURCES if args.source == "all" else {args.source: TPEX_SOURCES[args.source]}
    if args.source != "all" and args.source not in TPEX_SOURCES:
        print(f"Unknown source: {args.source}. Choose: all, TPEX-OTC, TPEX-ESB")
        sys.exit(1)

    total = 0
    for name, action in sources.items():
        total += ingest_source(name, action, args.start, end)

    # Summary
    conn = psycopg2.connect(**DB_CONFIG)
    cur  = conn.cursor()
    cur.execute("SELECT source, COUNT(*) FROM disposal GROUP BY source ORDER BY source")
    print("\n📊 disposal table breakdown:")
    for row in cur.fetchall():
        print(f"   {row[0]:12s} {row[1]:6d} rows")
    cur.execute("SELECT COUNT(*) FROM disposal")
    print(f"   {'TOTAL':12s} {cur.fetchone()[0]:6d} rows")
    conn.close()


if __name__ == "__main__":
    main()
