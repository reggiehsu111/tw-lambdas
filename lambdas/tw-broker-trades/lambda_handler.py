"""
tw-broker-trades

Daily Lambda to ingest 分點資料 (broker trade data) for all TW stocks
from TWSE bsContent API into quant_data.broker_trades table.

Source: https://bsr.twse.com.tw/bshtm/bsContent.aspx?StkNo={code}&RecCount=1

Schedule: weekday evenings after market close (Taiwan time)
"""

from __future__ import annotations

import os
import re
import time
import urllib.request
import urllib.error
from datetime import datetime, timedelta, timezone, date

import psycopg2
import psycopg2.extras
import pandas_market_calendars as mcal

# ── Config ────────────────────────────────────────────────────────────────────
DB_HOST     = os.environ.get("DB_HOST", "quant-db.cluster-c1igmy0yu89z.ap-northeast-1.rds.amazonaws.com")
DB_PORT     = int(os.environ.get("DB_PORT", "5432"))
DB_NAME     = os.environ.get("DB_NAME", "quant_data")
DB_USER     = os.environ.get("DB_USER", "quant_master")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "e74G2UWuxTDYr1j5Mtf7")

TW_TZ    = timezone(timedelta(hours=8))
TW_CAL   = mcal.get_calendar('XTAI')

BASE_URL   = "https://bsr.twse.com.tw/bshtm/bsContent.aspx"
DELAY_SEC  = 0.4   # polite delay between requests
TIMEOUT    = 15

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Referer":    "https://bsr.twse.com.tw/bshtm/bsMenu.aspx",
}


# ── Helpers ───────────────────────────────────────────────────────────────────
def get_today_tw() -> date:
    return datetime.now(TW_TZ).date()


def is_trading_day(d: date) -> bool:
    """Check if date is a XTAI trading day."""
    try:
        sessions = TW_CAL.valid_days(start_date=d, end_date=d)
        return len(sessions) > 0
    except Exception:
        return False


def already_ingested(trade_date: date, stock_code: str) -> bool:
    """Check if we already have data for this date/stock."""
    conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
                            user=DB_USER, password=DB_PASSWORD, connect_timeout=10, sslmode='require')
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM broker_trades WHERE trade_date=%s AND stock_code=%s LIMIT 1",
                (trade_date, stock_code)
            )
            return cur.fetchone() is not None
    finally:
        conn.close()


def get_universe() -> list[str]:
    """Get all unique 4-digit stock codes from disposal table as universe."""
    conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
                            user=DB_USER, password=DB_PASSWORD, connect_timeout=10, sslmode='require')
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT stock_code FROM disposal
                WHERE stock_code ~ '^[0-9]{4}$'
                ORDER BY stock_code
            """)
            return [r[0] for r in cur.fetchall()]
    finally:
        conn.close()


def parse_broker(raw: str) -> tuple[str, str]:
    """
    Parse '1020合　　庫' → (broker_id='1020', broker_name='合庫')
    Splits on first non-digit character, strips whitespace from name.
    """
    m = re.match(r'^(\d+)(.+)$', raw.strip())
    if m:
        broker_id   = m.group(1)
        broker_name = re.sub(r'\s+', '', m.group(2)).strip()
        return broker_id, broker_name
    return raw.strip(), raw.strip()


def parse_response(text: str, stock_code: str, trade_date: date) -> list[dict]:
    """Parse bsContent CSV response into list of records."""
    lines = text.strip().splitlines()
    if len(lines) < 4:
        return []

    records = []
    # Each line has two records separated by ',,'
    for line in lines[3:]:
        line = line.strip()
        if not line or line == ',':
            continue

        # Split into left and right halves
        parts = re.split(r',,', line)
        for part in parts:
            cols = [c.strip() for c in part.split(',')]
            if len(cols) < 5:
                continue
            try:
                # cols: [seq, broker, price, buy_qty, sell_qty]
                broker_raw = cols[1].strip()
                if not broker_raw:
                    continue
                broker_id, broker_name = parse_broker(broker_raw)
                price    = float(cols[2])
                buy_qty  = int(cols[3]) if cols[3].strip() else 0
                sell_qty_raw = cols[4].strip().rstrip()
                sell_qty = int(sell_qty_raw) if sell_qty_raw else 0

                if price <= 0:
                    continue

                records.append({
                    "trade_date":  trade_date,
                    "stock_code":  stock_code,
                    "broker_id":   broker_id,
                    "broker_name": broker_name,
                    "price":       price,
                    "buy_qty":     buy_qty,
                    "sell_qty":    sell_qty,
                })
            except (ValueError, IndexError):
                continue

    return records


def fetch_stock(stock_code: str) -> str | None:
    """Fetch bsContent for one stock. Returns raw text or None on failure."""
    url = f"{BASE_URL}?StkNo={stock_code}&RecCount=1"
    req = urllib.request.Request(url, headers=HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
            return resp.read().decode('ms950', errors='replace')
    except urllib.error.HTTPError as e:
        if e.code == 429:
            print(f"  [{stock_code}] Rate limited (429), sleeping 5s...")
            time.sleep(5)
        return None
    except Exception as e:
        print(f"  [{stock_code}] Fetch error: {e}")
        return None


def upsert_records(records: list[dict]) -> int:
    if not records:
        return 0
    conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
                            user=DB_USER, password=DB_PASSWORD, connect_timeout=10, sslmode='require')
    inserted = 0
    try:
        with conn:
            with conn.cursor() as cur:
                psycopg2.extras.execute_batch(cur, """
                    INSERT INTO broker_trades
                        (trade_date, stock_code, broker_id, broker_name, price, buy_qty, sell_qty)
                    VALUES
                        (%(trade_date)s, %(stock_code)s, %(broker_id)s, %(broker_name)s,
                         %(price)s, %(buy_qty)s, %(sell_qty)s)
                    ON CONFLICT (trade_date, stock_code, broker_id, price)
                    DO NOTHING
                """, records, page_size=500)
                inserted = sum(1 for _ in records)  # approximate
    finally:
        conn.close()
    return inserted


# ── Main handler ──────────────────────────────────────────────────────────────
def lambda_handler(event, context):
    """
    event can contain:
      - 'date': 'YYYYMMDD'   override trade date
      - 'stocks': ['2330']   override universe (for testing)
    """
    # Determine trade date
    date_str = event.get("date") if isinstance(event, dict) else None
    if date_str:
        trade_date = date(int(date_str[:4]), int(date_str[4:6]), int(date_str[6:]))
    else:
        trade_date = get_today_tw()

    print(f"Trade date: {trade_date}")

    # ── Trading day check ─────────────────────────────────────────────────────
    if not is_trading_day(trade_date):
        print(f"  {trade_date} is not a trading day — skipping")
        return {"statusCode": 200, "skipped": True, "reason": "non-trading-day", "date": str(trade_date)}

    # ── Universe ──────────────────────────────────────────────────────────────
    override_stocks = event.get("stocks") if isinstance(event, dict) else None
    if override_stocks:
        universe = override_stocks
    else:
        universe = get_universe()
    print(f"Universe: {len(universe)} stocks")

    # ── Already fully ingested? ───────────────────────────────────────────────
    # Quick check: if 2330 (most liquid, always has data) is already in, skip
    if not override_stocks and already_ingested(trade_date, "2330"):
        print(f"  Data for {trade_date}/2330 already exists — skipping full run")
        return {"statusCode": 200, "skipped": True, "reason": "already-ingested", "date": str(trade_date)}

    # ── Fetch & ingest ────────────────────────────────────────────────────────
    total_inserted  = 0
    total_skipped   = 0
    failed_stocks   = []
    no_data_stocks  = []

    for i, stock_code in enumerate(universe):
        # Skip if this specific stock already ingested today
        if already_ingested(trade_date, stock_code):
            total_skipped += 1
            continue

        raw = fetch_stock(stock_code)
        if raw is None:
            failed_stocks.append(stock_code)
            time.sleep(DELAY_SEC)
            continue

        records = parse_response(raw, stock_code, trade_date)
        if not records:
            no_data_stocks.append(stock_code)
            time.sleep(DELAY_SEC * 0.5)
            continue

        n = upsert_records(records)
        total_inserted += n

        if i % 50 == 0:
            print(f"  Progress: {i}/{len(universe)} | inserted={total_inserted} failed={len(failed_stocks)}")

        time.sleep(DELAY_SEC)

    print(f"\nDone: inserted={total_inserted} skipped={total_skipped} "
          f"no_data={len(no_data_stocks)} failed={len(failed_stocks)}")
    if failed_stocks:
        print(f"Failed stocks: {failed_stocks[:20]}")

    return {
        "statusCode":    200,
        "date":          str(trade_date),
        "total_inserted": total_inserted,
        "skipped":        total_skipped,
        "no_data":        len(no_data_stocks),
        "failed":         len(failed_stocks),
        "failed_stocks":  failed_stocks[:50],
    }
