#!/usr/bin/env python3
"""
Ingest TPEX disposal data into the disposal table via CSV download.

TPEX CSV requires dates in YYYY/MM/DD format.
Full history available from 2011/04/01.

Sources:
  TPEX-OTC  (上櫃): bulletin/disposal
  TPEX-ESB  (興櫃): bulletin/disposalEsb

Usage:
    python3 ingest_tpex_disposal.py                          # full backfill
    python3 ingest_tpex_disposal.py --start 2026/01/01       # custom start
    python3 ingest_tpex_disposal.py --source TPEX-OTC        # single source
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
import urllib.request
import urllib.parse
from datetime import datetime, timedelta, timezone, date
from typing import Optional

import psycopg2
import pandas_market_calendars as mcal

TW_TZ   = timezone(timedelta(hours=8))
TW_CAL  = mcal.get_calendar('XTAI')

DB_CONFIG = dict(
    host="quant-db.cluster-c1igmy0yu89z.ap-northeast-1.rds.amazonaws.com",
    port=5432, dbname="quant_data", user="quant_master", password="e74G2UWuxTDYr1j5Mtf7",
)

TPEX_API  = "https://www.tpex.org.tw/www/zh-tw"
HEADERS   = {
    "Content-Type":    "application/x-www-form-urlencoded; charset=UTF-8",
    "User-Agent":      "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Referer":         "https://www.tpex.org.tw/zh-tw/announce/market/disposal.html",
    "X-Requested-With":"XMLHttpRequest",
}
TPEX_SOURCES = {
    "TPEX-OTC": "bulletin/disposal",
    "TPEX-ESB": "bulletin/disposalEsb",
}
TPEX_HISTORY_START = "2011/04/01"


def tw_date_to_iso(s: str) -> Optional[date]:
    try:
        p = s.strip().split("/")
        return date(int(p[0]) + 1911, int(p[1]), int(p[2]))
    except Exception:
        return None


def parse_period(s: str) -> tuple[Optional[date], Optional[date]]:
    try:
        p = re.split(r"[～~]", s.strip())
        return tw_date_to_iso(p[0]), tw_date_to_iso(p[1]) if len(p) > 1 else None
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


def fetch_tpex_csv(action: str, source_name: str,
                   start_date: str, end_date: str) -> list[dict]:
    """
    Download TPEX disposal CSV and parse into records.
    Dates must be YYYY/MM/DD format.
    """
    url    = f"{TPEX_API}/{action}"
    params = urllib.parse.urlencode({
        "startDate": start_date,
        "endDate":   end_date,
        "type": "all", "reason": "-1", "measure": "-1",
        "order": "date", "response": "csv",
    }).encode("utf-8")

    req = urllib.request.Request(url, data=params, headers=HEADERS, method="POST")
    with urllib.request.urlopen(req, timeout=30) as resp:
        raw = resp.read().decode("ms950", errors="replace")

    lines = raw.splitlines()
    # Line 0: title, Line 1: period/range, Line 2: column headers, Line 3+: data
    data_lines = lines[3:]

    records = []
    reader = csv.reader(data_lines)
    for row in reader:
        if len(row) < 5:
            continue
        try:
            code = row[2].strip()
            name = re.sub(r'\s*\(.*?\)\s*$', '', row[3]).strip()
            if not code or not name:
                continue

            announce_d     = tw_date_to_iso(row[1])
            start_d, end_d = parse_period(row[4])

            records.append({
                "source":        source_name,
                "announce_date": announce_d,
                "stock_code":    code,
                "stock_name":    name,
                "punish_count":  None,
                "condition":     row[5].strip() if len(row) > 5 else None,
                "start_date":    start_d,
                "end_date":      end_d,
                "exit_date":     trading_exit_date(start_d, 6),
                "measure":       None,
                "content":       row[6].strip() if len(row) > 6 else None,
                "remark":        None,
            })
        except Exception:
            pass

    return records


def upsert_records(records: list[dict], source: str) -> tuple[int, int]:
    conn = psycopg2.connect(**DB_CONFIG, connect_timeout=10)
    inserted = skipped = 0
    try:
        with conn:
            with conn.cursor() as cur:
                for r in records:
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
                                  (start_date IS NULL AND %(start_date)s IS NULL)
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


def ingest_source(source_name: str, action: str, start: str, end: str):
    print(f"\n{'='*60}")
    print(f"Source: {source_name}  ({start} → {end})")
    print(f"{'='*60}")

    print(f"  Fetching CSV...", end=" ", flush=True)
    try:
        records = fetch_tpex_csv(action, source_name, start, end)
        print(f"{len(records)} records fetched")
    except Exception as e:
        print(f"ERROR: {e}")
        return 0

    ins, skip = upsert_records(records, source_name)
    print(f"  ✅ inserted={ins}  skipped={skip}")
    return ins


def main():
    parser = argparse.ArgumentParser(description="Ingest TPEX disposal CSV data into disposal table")
    parser.add_argument("--start",  default=TPEX_HISTORY_START,
                        help="Start date YYYY/MM/DD (default: 2011/04/01)")
    parser.add_argument("--end",    default=None,
                        help="End date YYYY/MM/DD (default: today TW)")
    parser.add_argument("--source", default="all",
                        help="all | TPEX-OTC | TPEX-ESB")
    args = parser.parse_args()

    if args.end is None:
        today = datetime.now(TW_TZ)
        args.end = today.strftime("%Y/%m/%d")

    if args.source == "all":
        sources = TPEX_SOURCES
    elif args.source in TPEX_SOURCES:
        sources = {args.source: TPEX_SOURCES[args.source]}
    else:
        print(f"Unknown source: {args.source}. Choose: all, TPEX-OTC, TPEX-ESB")
        sys.exit(1)

    for name, action in sources.items():
        ingest_source(name, action, args.start, args.end)

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
