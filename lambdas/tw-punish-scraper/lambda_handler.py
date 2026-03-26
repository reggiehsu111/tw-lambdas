"""
tw-punish-scraper

每日從 TWSE 抓取處置有價證券清單，寫入 PostgreSQL (quant_data.tw_punish_stocks)，
存到 S3，並發 Discord 通知。

TWSE API:
  GET https://www.twse.com.tw/rwd/zh/announcement/punish
      ?startDate=YYYYMMDD&endDate=YYYYMMDD&querytype=3&response=json
"""

from __future__ import annotations

import json
import os
import re
import urllib.request
import urllib.error
from datetime import datetime, timedelta, timezone, date

import boto3
import psycopg2
import psycopg2.extras

# ── Config from environment ───────────────────────────────────────────────────
S3_BUCKET           = os.environ.get("S3_BUCKET", "tw-lambdas-data")
S3_PREFIX           = os.environ.get("S3_PREFIX", "punish")
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "")

DB_HOST     = os.environ.get("DB_HOST", "quant-db.cluster-c1igmy0yu89z.ap-northeast-1.rds.amazonaws.com")
DB_PORT     = int(os.environ.get("DB_PORT", "5432"))
DB_NAME     = os.environ.get("DB_NAME", "quant_data")
DB_USER     = os.environ.get("DB_USER", "quant_master")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "e74G2UWuxTDYr1j5Mtf7")

TWSE_API = "https://www.twse.com.tw/rwd/zh/announcement/punish"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/143.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "en-US,en;q=0.9,zh-TW;q=0.8,zh;q=0.7",
    "Referer": "https://www.twse.com.tw/zh/announcement/punish.html",
    "X-Requested-With": "XMLHttpRequest",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
}

TW_TZ = timezone(timedelta(hours=8))


def get_today_tw() -> str:
    return datetime.now(TW_TZ).strftime("%Y%m%d")


def tw_date_to_iso(tw_date: str) -> date | None:
    """Convert ROC date string (115/03/26) → Python date (2026-03-26)."""
    try:
        parts = tw_date.strip().split("/")
        year = int(parts[0]) + 1911
        return date(year, int(parts[1]), int(parts[2]))
    except Exception:
        return None


def parse_period(period_str: str) -> tuple[date | None, date | None]:
    """
    Parse period string like '115/03/26～115/04/10' into (start_date, end_date).
    """
    try:
        parts = re.split(r"[～~]", period_str.strip())
        start = tw_date_to_iso(parts[0].strip()) if len(parts) > 0 else None
        end   = tw_date_to_iso(parts[1].strip()) if len(parts) > 1 else None
        return start, end
    except Exception:
        return None, None


def fetch_punish_data(date_str: str) -> dict:
    import time as _time
    cache_bust = int(_time.time() * 1000)
    url = (
        f"{TWSE_API}"
        f"?startDate={date_str}&endDate={date_str}"
        f"&querytype=3"
        f"&stockNo=&selectType=&proceType=&remarkType="
        f"&sortKind=DATE"
        f"&response=json"
        f"&_={cache_bust}"
    )
    print(f"Fetching: {url}")
    req = urllib.request.Request(url, headers=HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"HTTP {e.code} fetching TWSE API") from e
    data = json.loads(raw)
    if data.get("stat") != "OK":
        raise RuntimeError(f"TWSE API returned stat={data.get('stat')}")
    return data


def parse_records(data: dict) -> list[dict]:
    fields = data.get("fields", [])
    rows   = data.get("data", [])
    records = []
    seen = set()

    for row in rows:
        r = dict(zip(fields, row))
        stock_code   = str(r.get("證券代號", "")).strip()
        announce_date = tw_date_to_iso(str(r.get("公布日期", "")))
        period_str   = str(r.get("處置起迄時間", "")).strip()
        start_date, end_date = parse_period(period_str)

        key = (stock_code, str(announce_date), str(start_date))
        if key in seen:
            continue
        seen.add(key)

        # Strip HTML from remark field
        remark_raw = str(r.get("備註", ""))
        remark_clean = re.sub(r"<[^>]+>", "", remark_raw).strip()

        records.append({
            "announce_date": announce_date,
            "stock_code":    stock_code,
            "stock_name":    str(r.get("證券名稱", "")).strip(),
            "punish_count":  r.get("累計"),
            "condition":     str(r.get("處置條件", "")).strip(),
            "start_date":    start_date,
            "end_date":      end_date,
            "measure":       str(r.get("處置措施", "")).strip(),
            "content":       str(r.get("處置內容", "")).strip(),
            "remark":        remark_clean or None,
        })

    records.sort(key=lambda x: (str(x["announce_date"]), x["stock_code"]), reverse=True)
    return records


def write_to_db(records: list[dict]) -> int:
    """Upsert records into tw_punish_stocks. Returns number of rows inserted."""
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD,
        connect_timeout=10,
    )
    try:
        with conn:
            with conn.cursor() as cur:
                inserted = 0
                for r in records:
                    cur.execute("""
                        INSERT INTO tw_punish_stocks
                            (announce_date, stock_code, stock_name, punish_count,
                             condition, start_date, end_date, measure, content, remark)
                        VALUES
                            (%(announce_date)s, %(stock_code)s, %(stock_name)s, %(punish_count)s,
                             %(condition)s, %(start_date)s, %(end_date)s, %(measure)s, %(content)s, %(remark)s)
                        ON CONFLICT (announce_date, stock_code, start_date) DO NOTHING
                    """, r)
                    inserted += cur.rowcount
        print(f"DB: inserted {inserted} new rows (skipped {len(records) - inserted} duplicates)")
        return inserted
    finally:
        conn.close()


def save_to_s3(records: list[dict], date_str: str) -> str:
    s3 = boto3.client("s3")
    s3_key = f"{S3_PREFIX}/{date_str[:4]}/{date_str[4:6]}/{date_str}.json"

    # Convert date objects to strings for JSON serialisation
    serialisable = [
        {k: (v.isoformat() if isinstance(v, date) else v) for k, v in r.items()}
        for r in records
    ]
    payload = {
        "scrape_date":     date_str,
        "scrape_time_utc": datetime.utcnow().isoformat() + "Z",
        "total":           len(records),
        "records":         serialisable,
    }
    s3.put_object(
        Bucket=S3_BUCKET, Key=s3_key,
        Body=json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json; charset=utf-8",
    )
    print(f"S3: saved {len(records)} records to s3://{S3_BUCKET}/{s3_key}")
    return s3_key


def get_active_positions(target_date: date) -> list[dict]:
    """Query DB for currently active 處置股 positions on target_date."""
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD,
        connect_timeout=10,
    )
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    announce_date,
                    stock_code,
                    stock_name,
                    start_date,
                    (start_date + 6) AS exit_date,
                    measure
                FROM tw_punish_stocks
                WHERE
                    start_date IS NOT NULL
                    AND announce_date  <= %(d)s
                    AND (start_date + 6) >= %(d)s
                ORDER BY announce_date DESC, stock_code
            """, {"d": target_date})
            return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def send_discord(date_str: str, inserted: int) -> None:
    if not DISCORD_WEBHOOK_URL:
        print("DISCORD_WEBHOOK_URL not set, skipping")
        return

    target_date = date(int(date_str[:4]), int(date_str[4:6]), int(date_str[6:]))
    date_label  = target_date.isoformat()

    positions = get_active_positions(target_date)
    n         = len(positions)
    weight    = round(1.0 / n, 4) if n > 0 else 0

    if not positions:
        message = f"📋 **{date_label} 處置股策略** — 今日無持倉"
    else:
        lines = [
            f"📋 **{date_label} 處置股策略**",
            f"持倉 {n} 檔　各佔 {weight*100:.2f}%　新寫入 {inserted} 筆",
            "",
        ]
        for p in positions:
            lines.append(
                f"• **{p['stock_code']} {p['stock_name']}**"
                f"　公布 {p['announce_date']}"
                f"　{p['start_date']} ～ {p['exit_date']}"
            )
        message = "\n".join(lines)

    # Discord 2000 char limit — split into chunks if needed
    chunks = []
    while len(message) > 1900:
        split = message[:1900].rfind("\n")
        chunks.append(message[:split])
        message = message[split:]
    chunks.append(message)

    for chunk in chunks:
        payload = json.dumps({"content": chunk}).encode("utf-8")
        req = urllib.request.Request(
            DISCORD_WEBHOOK_URL, data=payload,
            headers={"Content-Type": "application/json", "User-Agent": "tw-punish-scraper/1.0"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                print(f"Discord notified: HTTP {resp.status}")
        except Exception as e:
            print(f"Discord notification failed (non-fatal): {e}")


def lambda_handler(event, context):
    date_str = event.get("date") if isinstance(event, dict) else None
    if not date_str:
        date_str = get_today_tw()

    print(f"Scraping 處置股 for date: {date_str}")

    raw_data = fetch_punish_data(date_str)
    records  = parse_records(raw_data)
    print(f"Found {len(records)} unique records")

    inserted = write_to_db(records)
    s3_key   = save_to_s3(records, date_str)
    send_discord(date_str, inserted)

    return {
        "statusCode": 200,
        "date":       date_str,
        "total":      len(records),
        "inserted":   inserted,
        "s3_key":     s3_key,
    }
