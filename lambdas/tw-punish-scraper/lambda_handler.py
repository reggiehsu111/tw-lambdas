"""
tw-punish-scraper

每日從 TWSE 抓取處置有價證券清單，存到 S3，並發 Discord 通知。

TWSE API:
  GET https://www.twse.com.tw/rwd/zh/announcement/punish
      ?startDate=YYYYMMDD&endDate=YYYYMMDD&response=json
"""

from __future__ import annotations

import json
import os
import urllib.request
import urllib.error
from datetime import datetime, timedelta, timezone

import boto3

# ── Config from environment ───────────────────────────────────────────────────
S3_BUCKET = os.environ.get("S3_BUCKET", "tw-lambdas-data")
S3_PREFIX = os.environ.get("S3_PREFIX", "punish")
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "")  # optional

TWSE_API = "https://www.twse.com.tw/rwd/zh/announcement/punish"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.twse.com.tw/zh/announcement/punish.html",
}

# Taiwan time = UTC+8
TW_TZ = timezone(timedelta(hours=8))


def get_today_tw() -> str:
    """Return today's date in Taiwan time as YYYYMMDD."""
    return datetime.now(TW_TZ).strftime("%Y%m%d")


def tw_date_to_iso(tw_date: str) -> str:
    """Convert ROC date (115/03/26) → ISO (2026-03-26)."""
    try:
        parts = tw_date.strip().split("/")
        year = int(parts[0]) + 1911
        return f"{year}-{parts[1]}-{parts[2]}"
    except Exception:
        return tw_date


def fetch_punish_data(date_str: str) -> dict:
    """Fetch 處置股 data from TWSE for a given date (YYYYMMDD)."""
    url = f"{TWSE_API}?startDate={date_str}&endDate={date_str}&response=json"
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
    """Parse raw API response into clean records."""
    fields = data.get("fields", [])
    rows = data.get("data", [])

    records = []
    seen = set()  # deduplicate by (stock_code, announce_date)

    for row in rows:
        record = dict(zip(fields, row))
        stock_code = str(record.get("證券代號", "")).strip()
        announce_date = record.get("公布日期", "")

        key = (stock_code, announce_date)
        if key in seen:
            continue
        seen.add(key)

        records.append({
            "announce_date": tw_date_to_iso(announce_date),
            "stock_code": stock_code,
            "stock_name": str(record.get("證券名稱", "")).strip(),
            "punish_count": record.get("累計", ""),
            "condition": str(record.get("處置條件", "")).strip(),
            "period": str(record.get("處置起迄時間", "")).strip(),
            "measure": str(record.get("處置措施", "")).strip(),
        })

    # Sort by announce_date desc, then stock_code
    records.sort(key=lambda r: (r["announce_date"], r["stock_code"]), reverse=True)
    return records


def save_to_s3(records: list[dict], date_str: str) -> str:
    """Save records as JSON to S3. Returns S3 key."""
    s3 = boto3.client("s3")
    s3_key = f"{S3_PREFIX}/{date_str[:4]}/{date_str[4:6]}/{date_str}.json"

    payload = {
        "scrape_date": date_str,
        "scrape_time_utc": datetime.utcnow().isoformat() + "Z",
        "total": len(records),
        "records": records,
    }

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json; charset=utf-8",
    )
    print(f"Saved {len(records)} records to s3://{S3_BUCKET}/{s3_key}")
    return s3_key


def send_discord(records: list[dict], date_str: str) -> None:
    """Send a summary to Discord webhook (if configured)."""
    if not DISCORD_WEBHOOK_URL:
        print("DISCORD_WEBHOOK_URL not set, skipping notification")
        return

    # Format date nicely
    date_label = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"

    if not records:
        message = f"📋 **{date_label} 處置股** — 今日無新增處置股"
    else:
        lines = [f"📋 **{date_label} 處置股** — 共 {len(records)} 檔\n"]
        for r in records:
            count_label = f"（第{r['punish_count']}次）" if r["punish_count"] else ""
            lines.append(
                f"• **{r['stock_code']} {r['stock_name']}** {count_label}"
                f"\n  處置期間：{r['period']}　措施：{r['measure']}"
            )
        message = "\n".join(lines)

    # Discord has 2000 char limit per message
    if len(message) > 1900:
        message = message[:1900] + "\n…（更多請查 S3）"

    payload = json.dumps({"content": message}).encode("utf-8")
    req = urllib.request.Request(
        DISCORD_WEBHOOK_URL,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            print(f"Discord notified: HTTP {resp.status}")
    except Exception as e:
        print(f"Discord notification failed (non-fatal): {e}")


def lambda_handler(event, context):
    """
    Entry point.

    event can optionally contain:
      - "date": "YYYYMMDD"  (override scrape date, default = today TW time)
    """
    date_str = event.get("date") if isinstance(event, dict) else None
    if not date_str:
        date_str = get_today_tw()

    print(f"Scraping 處置股 for date: {date_str}")

    try:
        raw_data = fetch_punish_data(date_str)
        records = parse_records(raw_data)
        print(f"Found {len(records)} unique 處置股 records")

        s3_key = save_to_s3(records, date_str)
        send_discord(records, date_str)

        return {
            "statusCode": 200,
            "date": date_str,
            "total": len(records),
            "s3_key": s3_key,
            "records": records,
        }

    except Exception as e:
        print(f"ERROR: {e}")
        raise
