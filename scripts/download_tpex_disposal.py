#!/usr/bin/env python3
"""
Download 興櫃處置股票 data from TPEX (OTC market).

Source: https://www.tpex.org.tw/zh-tw/announce/market/esb-disposal.html

Usage:
    # Today's data
    python3 download_tpex_disposal.py

    # Specific date range
    python3 download_tpex_disposal.py --start 20260101 --end 20260331

    # Save to CSV
    python3 download_tpex_disposal.py --start 20260101 --end 20260331 --output tpex_disposal.csv
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import urllib.request
import urllib.parse
from datetime import datetime, timedelta, timezone
from typing import Optional

TW_TZ = timezone(timedelta(hours=8))

API_URL = "https://www.tpex.org.tw/www/zh-tw/bulletin/disposalEsb"
HEADERS = {
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/143.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.tpex.org.tw/zh-tw/announce/market/esb-disposal.html",
    "X-Requested-With": "XMLHttpRequest",
    "Origin": "https://www.tpex.org.tw",
}


def tw_date_to_iso(tw_date: str) -> Optional[str]:
    """Convert ROC date '115/03/26' → ISO '2026-03-26'."""
    try:
        parts = tw_date.strip().split("/")
        year = int(parts[0]) + 1911
        return f"{year}-{parts[1].zfill(2)}-{parts[2].zfill(2)}"
    except Exception:
        return tw_date.strip() or None


def parse_period(period_str: str) -> tuple[Optional[str], Optional[str]]:
    """Parse '115/03/26~115/04/01' → ('2026-03-26', '2026-04-01')."""
    try:
        parts = re.split(r"[～~]", period_str.strip())
        start = tw_date_to_iso(parts[0].strip()) if len(parts) > 0 else None
        end   = tw_date_to_iso(parts[1].strip()) if len(parts) > 1 else None
        return start, end
    except Exception:
        return None, None


def clean_stock_name(raw: str) -> str:
    """Strip URL suffix from stock name like '研晶(../../mainboard/...)'."""
    return re.sub(r"\(.*?\)$", "", raw).strip()


def fetch_disposal_data(start_date: str, end_date: str) -> list[dict]:
    """
    Fetch 興櫃處置股票 data from TPEX API.

    Args:
        start_date: YYYYMMDD
        end_date:   YYYYMMDD

    Returns:
        List of dicts with cleaned data.
    """
    params = urllib.parse.urlencode({
        "startDate": start_date,
        "endDate":   end_date,
        "type":      "all",
        "reason":    "-1",
        "measure":   "-1",
        "order":     "date",
        "response":  "json",
    }).encode("utf-8")

    req = urllib.request.Request(API_URL, data=params, headers=HEADERS, method="POST")

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode("utf-8")
    except Exception as e:
        raise RuntimeError(f"Request failed: {e}") from e

    data = json.loads(raw)
    if data.get("stat") != "ok":
        raise RuntimeError(f"API returned stat={data.get('stat')}")

    tables = data.get("tables", [])
    if not tables:
        return []

    fields = tables[0].get("fields", [])
    rows   = tables[0].get("data", [])

    records = []
    for row in rows:
        r = dict(zip(fields, row))

        announce_date = tw_date_to_iso(str(r.get("公布日期", "")))
        stock_code    = str(r.get("證券代號", "")).strip()
        stock_name    = clean_stock_name(str(r.get("證券名稱", "")))
        period_str    = str(r.get("處置起訖時間", "")).strip()
        start_d, end_d = parse_period(period_str)

        # Skip "本日無處置資料" rows
        if not stock_code and not stock_name:
            continue

        # Strip HTML links from remark-style fields
        remark_raw = str(r.get(" ", "") or r.get("備註", ""))
        remark = re.sub(r"[^\n].*?\(.*?\)", lambda m: m.group().split("(")[0], remark_raw).strip()

        records.append({
            "announce_date": announce_date,
            "stock_code":    stock_code,
            "stock_name":    stock_name,
            "punish_count":  r.get("累計", ""),
            "period":        period_str,
            "start_date":    start_d,
            "end_date":      end_d,
            "condition":     str(r.get("處置原因", "")).strip(),
            "content":       str(r.get("處置內容", "")).strip(),
            "close_price":   str(r.get("收盤價", "")).strip(),
        })

    return records


def print_table(records: list[dict]) -> None:
    """Print records as a formatted table."""
    if not records:
        print("No data found.")
        return

    print(f"\n{'Announce':12} {'Code':8} {'Name':12} {'Count':6} {'Start':12} {'End':12} {'Condition'}")
    print("-" * 90)
    for r in records:
        print(
            f"{str(r['announce_date']):12} "
            f"{r['stock_code']:8} "
            f"{r['stock_name']:12} "
            f"{str(r['punish_count']):6} "
            f"{str(r['start_date']):12} "
            f"{str(r['end_date']):12} "
            f"{r['condition']}"
        )
    print(f"\nTotal: {len(records)} records")


def save_csv(records: list[dict], output_path: str) -> None:
    """Save records to CSV file."""
    if not records:
        print("No data to save.")
        return

    fieldnames = ["announce_date", "stock_code", "stock_name", "punish_count",
                  "start_date", "end_date", "condition", "content", "close_price"]

    with open(output_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(records)

    print(f"Saved {len(records)} records to: {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Download 興櫃處置股票 data from TPEX"
    )
    parser.add_argument("--start", "-s", type=str, default=None,
                        help="Start date YYYYMMDD (default: today)")
    parser.add_argument("--end",   "-e", type=str, default=None,
                        help="End date YYYYMMDD (default: today)")
    parser.add_argument("--output", "-o", type=str, default=None,
                        help="Output CSV file path (default: print to console)")
    args = parser.parse_args()

    today = datetime.now(TW_TZ).strftime("%Y%m%d")
    start_date = args.start or today
    end_date   = args.end   or today

    print(f"Fetching 興櫃處置股票: {start_date} → {end_date}")

    try:
        records = fetch_disposal_data(start_date, end_date)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    if args.output:
        save_csv(records, args.output)
    else:
        print_table(records)


if __name__ == "__main__":
    main()
