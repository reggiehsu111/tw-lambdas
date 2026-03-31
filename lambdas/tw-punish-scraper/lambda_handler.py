"""
tw-punish-scraper

每日從三個來源抓取處置有價證券清單，寫入 PostgreSQL (quant_data.disposal)，
存到 S3，並發 Discord 通知。

Sources:
  TWSE     (上市): https://www.twse.com.tw/rwd/zh/announcement/punish
  TPEX-OTC (上櫃): https://www.tpex.org.tw/www/zh-tw/bulletin/disposal
  TPEX-ESB (興櫃): https://www.tpex.org.tw/www/zh-tw/bulletin/disposalEsb
"""

from __future__ import annotations

import json
import os
import re
import urllib.request
import urllib.error
import urllib.parse
from datetime import datetime, timedelta, timezone, date

import boto3
import psycopg2
import psycopg2.extras
import pandas_market_calendars as mcal

_TW_CAL = mcal.get_calendar('XTAI')

# ── Config ────────────────────────────────────────────────────────────────────
S3_BUCKET           = os.environ.get("S3_BUCKET", "tw-lambdas-data")
S3_PREFIX           = os.environ.get("S3_PREFIX", "punish")
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "")
DB_HOST             = os.environ.get("DB_HOST", "quant-db.cluster-c1igmy0yu89z.ap-northeast-1.rds.amazonaws.com")
DB_PORT             = int(os.environ.get("DB_PORT", "5432"))
DB_NAME             = os.environ.get("DB_NAME", "quant_data")
DB_USER             = os.environ.get("DB_USER", "quant_master")
DB_PASSWORD         = os.environ.get("DB_PASSWORD", "e74G2UWuxTDYr1j5Mtf7")

TW_TZ = timezone(timedelta(hours=8))

# ── Source definitions ────────────────────────────────────────────────────────
TWSE_API    = "https://www.twse.com.tw/rwd/zh/announcement/punish"
TPEX_API    = "https://www.tpex.org.tw/www/zh-tw"

TWSE_HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
    "Accept":          "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "en-US,en;q=0.9,zh-TW;q=0.8,zh;q=0.7",
    "Referer":         "https://www.twse.com.tw/zh/announcement/punish.html",
    "X-Requested-With":"XMLHttpRequest",
    "Sec-Fetch-Dest":  "empty",
    "Sec-Fetch-Mode":  "cors",
    "Sec-Fetch-Site":  "same-origin",
}
TPEX_HEADERS = {
    "Content-Type":    "application/x-www-form-urlencoded; charset=UTF-8",
    "Accept":          "application/json, text/javascript, */*; q=0.01",
    "User-Agent":      "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
    "Referer":         "https://www.tpex.org.tw/zh-tw/announce/market/disposal.html",
    "X-Requested-With":"XMLHttpRequest",
    "Origin":          "https://www.tpex.org.tw",
}


# ── Date helpers ──────────────────────────────────────────────────────────────
def get_today_tw() -> str:
    return datetime.now(TW_TZ).strftime("%Y%m%d")


def tw_date_to_iso(s: str) -> date | None:
    try:
        parts = s.strip().split("/")
        return date(int(parts[0]) + 1911, int(parts[1]), int(parts[2]))
    except Exception:
        return None


def parse_period(s: str) -> tuple[date | None, date | None]:
    try:
        parts = re.split(r"[～~]", s.strip())
        start = tw_date_to_iso(parts[0].strip()) if len(parts) > 0 else None
        end   = tw_date_to_iso(parts[1].strip()) if len(parts) > 1 else None
        return start, end
    except Exception:
        return None, None


def trading_exit_date(start: date, n: int = 6) -> date | None:
    if not start:
        return None
    try:
        sessions = _TW_CAL.valid_days(start_date=start, end_date=start + timedelta(days=60))
        dates = [s.date() for s in sessions]
        idx = dates.index(start) if start in dates else next(i for i, d in enumerate(dates) if d >= start)
        return dates[idx + n]
    except Exception:
        return None


def nth_trading_day_after(start: date, n: int) -> date:
    sessions = _TW_CAL.valid_days(start_date=start, end_date=start + timedelta(days=60))
    dates = [s.date() for s in sessions]
    idx = next((i for i, d in enumerate(dates) if d >= start), 0)
    return dates[idx + n]


def nth_trading_day_before(end: date, n: int) -> date:
    sessions = _TW_CAL.valid_days(start_date=end - timedelta(days=60), end_date=end)
    dates = [s.date() for s in sessions]
    idx = next((i for i in range(len(dates)-1, -1, -1) if dates[i] <= end), -1)
    return dates[idx - n]


# ── Fetchers ──────────────────────────────────────────────────────────────────
def fetch_twse(date_str: str) -> list[dict]:
    """Fetch TWSE (上市) disposal data."""
    import time as _time
    cache_bust = int(_time.time() * 1000)
    url = (
        f"{TWSE_API}?startDate={date_str}&endDate={date_str}"
        f"&querytype=3&stockNo=&selectType=&proceType=&remarkType="
        f"&sortKind=DATE&response=json&_={cache_bust}"
    )
    print(f"[TWSE] Fetching: {url}")
    req = urllib.request.Request(url, headers=TWSE_HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"TWSE HTTP {e.code}") from e

    if data.get("stat") != "OK":
        raise RuntimeError(f"TWSE stat={data.get('stat')}")

    fields = data.get("fields", [])
    rows   = data.get("data", [])
    records = []
    seen = set()

    for row in rows:
        r = dict(zip(fields, row))
        code         = str(r.get("證券代號", "")).strip()
        announce_d   = tw_date_to_iso(str(r.get("公布日期", "")))
        period_str   = str(r.get("處置起迄時間", "")).strip()
        start_d, end_d = parse_period(period_str)
        key = (code, str(announce_d), str(start_d))
        if key in seen or not code:
            continue
        seen.add(key)
        remark_raw = str(r.get("備註", ""))
        records.append({
            "source":        "TWSE",
            "announce_date": announce_d,
            "stock_code":    code,
            "stock_name":    str(r.get("證券名稱", "")).strip(),
            "punish_count":  r.get("累計"),
            "condition":     str(r.get("處置條件", "")).strip(),
            "start_date":    start_d,
            "end_date":      end_d,
            "exit_date":     trading_exit_date(start_d, 6),
            "measure":       str(r.get("處置措施", "")).strip(),
            "content":       str(r.get("處置內容", "")).strip(),
            "remark":        re.sub(r"<[^>]+>", "", remark_raw).strip() or None,
        })
    print(f"[TWSE] {len(records)} records")
    return records


def fetch_tpex(action: str, source_name: str) -> list[dict]:
    """
    Fetch TPEX (上櫃/興櫃) disposal data via CSV download.
    TPEX requires YYYY/MM/DD date format. Without a historical start date
    it returns only currently-active disposals, which is what we want for daily updates.
    """
    import csv as _csv
    url        = f"{TPEX_API}/{action}"
    today_str  = datetime.now(TW_TZ).strftime("%Y/%m/%d")
    # Use a 30-day lookback to catch newly announced stocks
    start_str  = (datetime.now(TW_TZ) - timedelta(days=30)).strftime("%Y/%m/%d")

    params = urllib.parse.urlencode({
        "startDate": start_str,
        "endDate":   today_str,
        "type": "all", "reason": "-1", "measure": "-1",
        "order": "date", "response": "csv",
    }).encode("utf-8")

    print(f"[{source_name}] Fetching CSV: {start_str} → {today_str}")
    req = urllib.request.Request(url, data=params, headers=TPEX_HEADERS, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode("ms950", errors="replace")
    except Exception as e:
        raise RuntimeError(f"{source_name} fetch failed: {e}") from e

    lines = raw.splitlines()
    # Lines: [0] title, [1] period, [2] headers, [3+] data
    records = []
    reader = _csv.reader(lines[3:])
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
    print(f"[{source_name}] {len(records)} records")
    return records


# ── DB ────────────────────────────────────────────────────────────────────────
def write_to_db(records: list[dict]) -> int:
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD, connect_timeout=10,
    )
    inserted = 0
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
                    """, r)
                    if cur.rowcount:
                        inserted += 1
    finally:
        conn.close()
    print(f"DB: inserted {inserted} / {len(records)} records")
    return inserted


# ── S3 ────────────────────────────────────────────────────────────────────────
def save_to_s3(all_records: dict[str, list], date_str: str) -> str:
    s3     = boto3.client("s3")
    s3_key = f"{S3_PREFIX}/{date_str[:4]}/{date_str[4:6]}/{date_str}.json"

    def serialise(r):
        return {k: (v.isoformat() if isinstance(v, date) else v) for k, v in r.items()}

    payload = {
        "scrape_date":     date_str,
        "scrape_time_utc": datetime.utcnow().isoformat() + "Z",
        "sources":         {src: [serialise(r) for r in recs] for src, recs in all_records.items()},
        "total":           sum(len(v) for v in all_records.values()),
    }
    s3.put_object(
        Bucket=S3_BUCKET, Key=s3_key,
        Body=json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json; charset=utf-8",
    )
    print(f"S3: saved to s3://{S3_BUCKET}/{s3_key}")
    return s3_key


# ── Discord ───────────────────────────────────────────────────────────────────
def get_active_positions(target_date: date) -> list[dict]:
    """
    Strategy window:
        entry = start_date + 2 trading days
        exit  = end_date  (actual 處置 end date)
        Active when: entry <= target_date <= end_date
    """
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD, connect_timeout=10,
    )
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT announce_date, stock_code, stock_name,
                       start_date, end_date, exit_date, measure, source
                FROM disposal
                WHERE start_date IS NOT NULL AND end_date IS NOT NULL
                  AND announce_date <= %(d)s AND end_date >= %(d)s
                ORDER BY source, announce_date DESC, stock_code
            """, {"d": target_date})
            all_pos = [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()

    filtered = []
    for p in all_pos:
        try:
            entry = nth_trading_day_after(p["start_date"], 2)
            # Hold all the way to end_date (actual 處置 end)
            if entry <= target_date <= p["end_date"]:
                p["strategy_entry"] = entry
                p["strategy_exit"]  = p["end_date"]
                filtered.append(p)
        except Exception:
            pass
    return filtered


def get_all_punished_today(target_date: date) -> list[dict]:
    """All stocks currently in 處置 window (start_date <= today <= end_date), all sources."""
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD, connect_timeout=10,
    )
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT announce_date, stock_code, stock_name,
                       start_date, end_date, measure, source
                FROM disposal
                WHERE start_date IS NOT NULL AND end_date IS NOT NULL
                  AND start_date <= %(d)s AND end_date >= %(d)s
                ORDER BY source, start_date, stock_code
            """, {"d": target_date})
            return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def send_discord(date_str: str, inserted_by_source: dict[str, int]) -> None:
    if not DISCORD_WEBHOOK_URL:
        print("DISCORD_WEBHOOK_URL not set, skipping")
        return

    target_date = date(int(date_str[:4]), int(date_str[4:6]), int(date_str[6:]))
    date_label  = target_date.isoformat()

    # Previous trading day
    try:
        prev_sessions = _TW_CAL.valid_days(
            start_date=target_date - timedelta(days=10), end_date=target_date)
        prev_date = [s.date() for s in prev_sessions][-2]
    except Exception:
        prev_date = target_date - timedelta(days=1)

    today_positions = get_active_positions(target_date)
    prev_positions  = get_active_positions(prev_date)
    today_codes = {p["stock_code"] for p in today_positions}
    prev_codes  = {p["stock_code"] for p in prev_positions}
    added       = today_codes - prev_codes
    removed     = prev_codes  - today_codes

    n_today = len(today_positions)
    n_prev  = len(prev_positions)
    w_today = round(1.0 / n_today, 4) if n_today > 0 else 0
    w_prev  = round(1.0 / n_prev,  4) if n_prev  > 0 else 0
    w_delta = w_today - w_prev

    # Source labels
    source_tag = {"TWSE": "上市", "TPEX-OTC": "上櫃", "TPEX-ESB": "興櫃"}
    new_counts = " | ".join(f"{source_tag.get(s,s)} +{c}" for s, c in inserted_by_source.items() if c > 0)

    lines = [f"📋 **{date_label} 處置股策略** （處置第2日起）"]

    if n_today == 0:
        lines.append("今日無持倉")
    else:
        w_change_str = ""
        if w_delta != 0 and n_prev > 0:
            sign = "+" if w_delta > 0 else ""
            w_change_str = f"　({sign}{w_delta*100:.2f}%)"
        lines.append(f"持倉 **{n_today}** 檔　各佔 **{w_today*100:.2f}%**{w_change_str}")
        if new_counts:
            lines.append(f"今日新收錄：{new_counts}")

    if added:
        lines.append("")
        lines.append("🟢 **新增持倉**")
        for p in today_positions:
            if p["stock_code"] in added:
                tag = source_tag.get(p["source"], p["source"])
                lines.append(
                    f"  ＋ **{p['stock_code']} {p['stock_name']}** `{tag}`"
                    f"　{p['strategy_entry']} ～ {p['strategy_exit']}"
                )

    if removed:
        lines.append("")
        lines.append("🔴 **移除持倉**")
        for p in prev_positions:
            if p["stock_code"] in removed:
                tag = source_tag.get(p["source"], p["source"])
                lines.append(
                    f"  － **{p['stock_code']} {p['stock_name']}** `{tag}`"
                    f"　{p['strategy_entry']} ～ {p['strategy_exit']}"
                )

    if today_positions:
        lines.append("")
        lines.append("📌 **當前持倉**")
        for p in today_positions:
            tag   = source_tag.get(p["source"], p["source"])
            new_m = " 🆕" if p["stock_code"] in added else ""
            lines.append(
                f"  • **{p['stock_code']} {p['stock_name']}**{new_m} `{tag}`"
                f"　{p['strategy_entry']} ～ {p['strategy_exit']}"
            )

    all_punished = get_all_punished_today(target_date)
    if all_punished:
        lines.append("")
        lines.append(f"⚠️ **目前所有處置股** （共 {len(all_punished)} 檔）")
        for p in all_punished:
            in_strategy = p["stock_code"] in today_codes
            marker = "✅" if in_strategy else "  "
            tag    = source_tag.get(p["source"], p["source"])
            lines.append(
                f"  {marker} {p['stock_code']} {p['stock_name']} `{tag}`"
                f"　{p['start_date']} ～ {p['end_date']}"
            )

    message = "\n".join(lines)
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


# ── Entry point ───────────────────────────────────────────────────────────────
def lambda_handler(event, context):
    date_str = event.get("date") if isinstance(event, dict) else None
    if not date_str:
        date_str = get_today_tw()

    print(f"Scraping disposal data for date: {date_str}")

    inserted_by_source = {}
    all_records        = {}

    # 1. TWSE (上市)
    try:
        twse_records = fetch_twse(date_str)
        ins = write_to_db(twse_records)
        inserted_by_source["TWSE"] = ins
        all_records["TWSE"] = twse_records
    except Exception as e:
        print(f"[TWSE] ERROR: {e}")
        inserted_by_source["TWSE"] = 0
        all_records["TWSE"] = []

    # 2. TPEX-OTC (上櫃)
    try:
        otc_records = fetch_tpex("bulletin/disposal", "TPEX-OTC")
        ins = write_to_db(otc_records)
        inserted_by_source["TPEX-OTC"] = ins
        all_records["TPEX-OTC"] = otc_records
    except Exception as e:
        print(f"[TPEX-OTC] ERROR: {e}")
        inserted_by_source["TPEX-OTC"] = 0
        all_records["TPEX-OTC"] = []

    # 3. TPEX-ESB (興櫃)
    try:
        esb_records = fetch_tpex("bulletin/disposalEsb", "TPEX-ESB")
        ins = write_to_db(esb_records)
        inserted_by_source["TPEX-ESB"] = ins
        all_records["TPEX-ESB"] = esb_records
    except Exception as e:
        print(f"[TPEX-ESB] ERROR: {e}")
        inserted_by_source["TPEX-ESB"] = 0
        all_records["TPEX-ESB"] = []

    total_inserted = sum(inserted_by_source.values())
    total_records  = sum(len(v) for v in all_records.values())
    print(f"Total: {total_records} fetched, {total_inserted} new rows inserted")

    s3_key = save_to_s3(all_records, date_str)
    send_discord(date_str, inserted_by_source)

    return {
        "statusCode":        200,
        "date":              date_str,
        "inserted":          inserted_by_source,
        "total_fetched":     total_records,
        "s3_key":            s3_key,
    }
