# -*- coding: utf-8 -*-
"""
XQ 下引線選股｜TWSE / TPEx 全市場日行情 GitHub Actions 部署版
=====================================================

原始 XQ 選股腳本（@guid:0b7b6893ea2a4f6ebef945d654dfe780）條件，逐條對照：
1. Condition1：Close - Low > Close × 2%          （XQ 原稿稱「下引線」，這裡照原公式，不改成 min(O,C)-L）
2. Condition2：High - Low > Close × 3%           （振幅）
3. Condition3：Low > Lowest(Low, 20)[1]          （前 20 日最低價，不含今日）
4. Condition4：Volume > Average(Volume, 20) × 0.8（20 日均量含今日）
5. Condition5：Volume > 500                      （XQ 日線 Volume 單位是「張」）
原稿的 Value1 = Average(Close, 5) 沒有被任何條件用到，這裡不算。

部署重點：
1. 不用 yfinance 逐檔抓 1,800 檔，改抓 TWSE MI_INDEX / TPEx dailyQuotes「單日全市場」行情，
   22 個交易日約 44 次請求就有上市櫃全部個股的 OHLCV，2～3 分鐘跑完。
2. TPEx 在 GitHub Actions（海外）會被導到 Cloudflare、缺 TWCA 中繼憑證，
   沿用 Stock_V116 main.py 的 CA bundle 修正（驗證照常開啟，不是 verify=False）。
3. 今天不是交易日、或交易所還沒出今日行情時直接結束、不推播，避免把昨天的結果當今天推。
4. 任一交易日任一市場抓不到就中止（門檻可調），不推播不完整的選股結果。
5. 結果推 Discord（純文字、自動切段）並輸出 CSV 給 Actions artifact。

GitHub Actions 建議 secrets：
- DISCORD_WEBHOOK_URL（或 DISCORD_WEBHOOK_URL_TEST）

手動補跑某一天：workflow_dispatch 輸入 TARGET_DATE=YYYYMMDD。

必要套件：
pip install requests pandas
"""

import os
import re
import time
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests


# ================= 設定區 =================
# --- XQ 選股參數（預設值 = 原 XQ 腳本）---
SHADOW_PCT = float(os.getenv("SHADOW_PCT", "0.02"))          # Condition1 下引線 / 收盤
RANGE_PCT = float(os.getenv("RANGE_PCT", "0.03"))            # Condition2 振幅 / 收盤
LOW_LOOKBACK = max(1, int(os.getenv("LOW_LOOKBACK", "20")))  # Condition3 Lowest(Low, N)[1]
VOL_MA_DAYS = max(1, int(os.getenv("VOL_MA_DAYS", "20")))    # Condition4 Average(Volume, N)
VOL_RATIO = float(os.getenv("VOL_RATIO", "0.8"))             # Condition4 倍數
MIN_VOLUME_LOTS = float(os.getenv("MIN_VOLUME_LOTS", "500")) # Condition5 張數門檻

# Condition3 要「今日 + 前 LOW_LOOKBACK 日」，Condition4 要 VOL_MA_DAYS 日，取大者再多抓幾天緩衝。
EXTRA_TRADING_DAYS = max(0, int(os.getenv("EXTRA_TRADING_DAYS", "1")))
NEED_TRADING_DAYS = max(LOW_LOOKBACK + 1, VOL_MA_DAYS) + EXTRA_TRADING_DAYS

# 往回掃日曆日的上限。農曆年連假最長約 9 天，60 天綽綽有餘；超過代表資料源出問題。
MAX_CALENDAR_SCAN_DAYS = max(30, int(os.getenv("MAX_CALENDAR_SCAN_DAYS", "60")))

# TWSE 對同一 IP 短時間大量請求會暫時封鎖（約 5 秒 3 次以上就有風險），每個交易日之間固定休息。
REQUEST_SLEEP = max(0.5, float(os.getenv("REQUEST_SLEEP", "2.5")))
REQUEST_RETRIES = max(1, int(os.getenv("REQUEST_RETRIES", "4")))
REQUEST_RETRY_SLEEP = max(1.0, float(os.getenv("REQUEST_RETRY_SLEEP", "6")))

# 指定結算日（YYYYMMDD）。留空 = 今天（台北時間）。補跑歷史日期時用。
TARGET_DATE = os.getenv("TARGET_DATE", "").strip()

# 從「最新完整交易日」再往前 N 個交易日結算。給 GitHub 手動執行的下拉選單用，
# 不用自己算日期、也不用管中間有沒有假日。0 = 最新交易日。
# 選單值長得像「1｜前 1 個交易日」，只取開頭數字。
_days_back_raw = re.match(r"\s*(\d+)", os.getenv("DAYS_BACK", "0") or "0")
DAYS_BACK = int(_days_back_raw.group(1)) if _days_back_raw else 0

# 預設：今天（或指定日）沒有完整行情時，自動改用最近一個上市櫃都有資料的交易日，
# Discord 標題會寫明實際資料日期。設 SKIP_IF_NOT_TODAY=1 則改成「不是今天就不推播」，
# 避免週末 / 假日重複推上一個交易日的結果。
SKIP_IF_NOT_TODAY = os.getenv("SKIP_IF_NOT_TODAY", "0").strip().lower() in ("1", "true", "yes", "on")

# 預設只篩一般股票（4 碼、非 0 開頭）。ETF（0050、00878…）的下引線意義不同，預設排除。
INCLUDE_ETF = os.getenv("INCLUDE_ETF", "0").strip().lower() in ("1", "true", "yes", "on")

# 沒有任何股票符合時是否仍推一則「今日無符合」訊息，方便確認排程有在跑。
PUSH_WHEN_EMPTY = os.getenv("PUSH_WHEN_EMPTY", "1").strip().lower() not in ("0", "false", "no", "off")

# 允許抓取失敗的「交易日×市場」次數。缺任一天，該市場全部股票的 20 日最低 / 均量都會算不出來，
# 等於整個市場被默默排除，所以預設 0：缺就中止、不推播。
MAX_ALLOWED_FAILED_DATES = max(0, int(os.getenv("MAX_ALLOWED_FAILED_DATES", "0")))

OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "output"))

DISCORD_WEBHOOK_URL = (
    os.getenv("DISCORD_WEBHOOK_URL_TEST")
    or os.getenv("DISCORD_WEBHOOK_URL")
    or ""
)
# Discord 單則上限 2000 字，留緩衝。
DISCORD_MAX_CHARS = 1900

TWSE_DAILY_URL = "https://www.twse.com.tw/exchangeReport/MI_INDEX"
TPEX_DAILY_URL = "https://www.tpex.org.tw/www/zh-tw/afterTrading/dailyQuotes"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/128.0 Safari/537.36"
)
HDR_TWSE = {
    "User-Agent": USER_AGENT,
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.twse.com.tw/zh/trading/historical/mi-index.html",
}
HDR_TPEX = {
    "User-Agent": USER_AGENT,
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.tpex.org.tw/www/zh-tw/afterTrading/dailyQuotes",
}

TW_TZ = timezone(timedelta(hours=8))

# TPEx 自 2026-09 起用地理 DNS：台灣來源連到自家主機，憑證鏈完整；
# 海外來源 (GitHub Actions runner) 被導到 Cloudflare (172.65.90.66 / .67)，
# 那層只送葉憑證、少了中繼憑證「TWCA SSL Certification Authority」。
# 瀏覽器會用 AIA 自動補抓，所以網頁看起來正常；requests 不會，會噴 unable to get local issuer certificate。
# 台灣本機測一定成功，不代表 Actions 上會過——要重現得指定 IP：
#   openssl s_client -connect 172.65.90.66:443 -servername www.tpex.org.tw
# 解法：把缺的中繼憑證併進 certifi 做成 TPEx 專用 CA 檔。內容與 Stock_V116 main.py 相同。
# 此中繼憑證效期到 2033-02-23 (SHA256 01:AF:23:24:...:E5:31)；TPEx 換中繼 CA 時要更新這段。
# 設 TPEX_CA_FIX_ENABLE=0 可退回 requests 預設憑證驗證。
TPEX_CA_FIX_ENABLE = os.getenv("TPEX_CA_FIX_ENABLE", "1").strip().lower() not in ("0", "false", "no", "off")
TPEX_TWCA_INTERMEDIATE_PEM = """-----BEGIN CERTIFICATE-----
MIIG1DCCBLygAwIBAgIQQAE0sE8AAAAAAAAAA+MkrDANBgkqhkiG9w0BAQwFADBQ
MQswCQYDVQQGEwJUVzESMBAGA1UEChMJVEFJV0FOLUNBMRAwDgYDVQQLEwdSb290
IENBMRswGQYDVQQDExJUV0NBIENZQkVSIFJvb3QgQ0EwHhcNMjMwMjIzMDcyMjI0
WhcNMzMwMjIzMTU1OTU5WjBhMQswCQYDVQQGEwJUVzESMBAGA1UEChMJVEFJV0FO
LUNBMRMwEQYDVQQLEwpTU0wgU3ViLUNBMSkwJwYDVQQDEyBUV0NBIFNTTCBDZXJ0
aWZpY2F0aW9uIEF1dGhvcml0eTCCAiIwDQYJKoZIhvcNAQEBBQADggIPADCCAgoC
ggIBAMquxiSlMrxfOO29yqxCo/BIYBswnE7snZnuZDPcx8N9WhOdNGDsF024VjXK
nXoVaZBcv56eFsU+w9Mcq+uIVYzjVrBoe5u8ZLE0hPSkluH8URhcxtSQJ+gXcB0L
JHsseAeXVcgqoxTSJ6/n0xTCeXEnGwSRAzrqTvjS2gbd3TILxsfIHwRgwwPjBDgm
tjzbHHOFTJB3GCtH65T9A0viM2B/IW9Wz73jkz02AVMrZBHQ67IJ2W9CoIjd5mdG
eIV36U9NXl+wZa/D90pLRsFVbItKgLXgF71CQ92vS/biTx8fA6UUCU2ToNczP5Ur
A/mDXCBCLakwa1I3ylRkgFwluJw9DqiYh56MRgsEABa+ZPrm1Qb9njQZK4Y4V+ML
IvGM3xVoHIlvaSN29ubTueLpTeuAwN2VTiRzfOyCRKTcMBCtlMw1WCJNAMiNWDWS
BMnY9SlKv1oujmjS/ti0ptcipMymIoeWpVuQt3Mj8lYlKRpd6Zg8MbljMwQRSClK
6O6MSwpM3Xy5uJGh2cY5oYmKtxfyHSuKtKsk+daAPV1lpWYp9bNrbsLUPwmSY+zk
VgkZdiWBF//RP/72/esANONINy5hkWjkVd0NLjA5TAgk+DVVmnPtQIj1vBgtk8ak
Y9CczIbEgKBonkHWn+GX2ycR6jadg2P+xrBFg4MGjomkb2gtAgMBAAGjggGXMIIB
kzAfBgNVHSMEGDAWgBSdhWEUfMFib5do5E83QOGt4A1WNzAdBgNVHQ4EFgQU8ijU
+dQcfhprFoLl75Mpae3KFSAwDgYDVR0PAQH/BAQDAgEGMBMGA1UdJQQMMAoGCCsG
AQUFBwMBMEoGA1UdIARDMEEwNQYLKwYBBAGCvyUBARUwJjAkBggrBgEFBQcCARYY
aHR0cHM6Ly93d3cudHdjYS5jb20udHcvMAgGBmeBDAECAjBNBgNVHR8ERjBEMEKg
QKA+hjxodHRwOi8vUm9vdENBLnR3Y2EuY29tLnR3L1RXQ0FSQ0EvY3liZXJfcm9v
dF9yZXZva2VfMjAyMi5jcmwwEgYDVR0TAQH/BAgwBgEB/wIBADB9BggrBgEFBQcB
AQRxMG8wQwYIKwYBBQUHMAKGN2h0dHA6Ly9zc2xzZXJ2ZXIudHdjYS5jb20udHcv
Y2FjZXJ0L2N5YmVyX3Jvb3RfMjAyMi5jcnQwKAYIKwYBBQUHMAGGHGh0dHA6Ly9y
b290b2NzcC50d2NhLmNvbS50dy8wDQYJKoZIhvcNAQEMBQADggIBAIFF/6Gnvu8L
3xQDIampB8QVgoKS2bcjte0uJBbCrQHpzcGTuVTkZaiA86LwVz6SAU7TVgVYRXmt
x8l29WzfKI6wOAzmvlGZxSYAdN0I6YBkJK1nmDs0+TSw5lCzb+UOpajNOaMdJ5SN
YTN87yRwl82AFrwUmSLaMV4tN7W49N0SsELWs/d4uNHSMM0mBjd0hLDIWJFwOkuD
yOWahnCVfPlCwSVWpUntOGgOHOA02IUE+JNX+spIV1SwAMYaEVyHe316YUgiGA5y
k3liTa3vuv06eE1J2yiWrs9booW2VTHD+amzucFFNN1KvSLjSbYxG1t/FclHEN/y
6hGM3bkjRC31A0jzpv93D3MUQTdJascicPa0H4i8hviRriyetaC6HC4q8FQUTo2A
cEpxicNGgyHhDV+YdbnS6GZL+f3bsmMM8ZFYZ77mDTS9mRO1VnIwkjiN4vpzh67a
KTpoD9TQzZcGQiJy6Pi+PCSFiqjK7UD/63L/Pt0hpoNKvZLrz4ngrlpyzpx8KjeS
A5cjKcc6vlHm0Kk07k5djhJsaqQELso5r+UXi9qC+nwqPuR/w5kJZv4fz0ND4UhY
5y3qd+iCikkF3WzOzey7jUH9URKb3iZnRAHZvmyLK57UI0FwP+5xZEByvwXDtxbe
914Hj3cSUrmKT3g/ZlOQQ1THeu48MA79
-----END CERTIFICATE-----
"""
_TPEX_CA_BUNDLE = None
_TPEX_CA_LOCK = threading.Lock()


# ================= 基本工具 =================
_ZERO_WIDTH_RE = re.compile(r"[​-‏‪-‮﻿]")
_TAG_RE = re.compile(r"<[^>]+>")
_STOCK_CODE_RE = re.compile(r"^[1-9][0-9]{3}$")
_ETF_CODE_RE = re.compile(r"^0[0-9]{3,4}[A-Z]?$")


def log(msg):
    print(f"[{datetime.now(TW_TZ).strftime('%H:%M:%S')}] {msg}", flush=True)


def clean_cell(s) -> str:
    s = "" if s is None else str(s)
    s = _TAG_RE.sub("", s)
    s = s.replace("\xa0", " ")
    s = _ZERO_WIDTH_RE.sub("", s)
    return re.sub(r"\s+", " ", s).strip()


def to_float(x, default=None):
    """交易所沒成交會給 '--' / '---' / 空字串，一律回 default，不炸。"""
    try:
        s = clean_cell(x).replace(",", "").replace("%", "")
        if not s or set(s) <= {"-"}:
            return default
        return float(s)
    except Exception:
        return default


def _normalize_header(h) -> str:
    # TPEx 欄名常帶尾端空白（例如「收盤 」「成交股數  」），全部去掉再比對。
    return re.sub(r"\s+", "", clean_cell(h))


def _is_target_code(code: str) -> bool:
    if _STOCK_CODE_RE.match(code):
        return True
    return INCLUDE_ETF and bool(_ETF_CODE_RE.match(code))


def _tpex_get_ca_bundle():
    """回傳 TPEx 請求用的 verify 參數：certifi + TWCA 中繼憑證合併檔路徑（整輪只建一次）。

    關閉修正或建立失敗時回傳 True，退回 requests 預設憑證驗證。
    """
    global _TPEX_CA_BUNDLE
    with _TPEX_CA_LOCK:
        if _TPEX_CA_BUNDLE is not None:
            return _TPEX_CA_BUNDLE

        if not TPEX_CA_FIX_ENABLE:
            _TPEX_CA_BUNDLE = True
            return _TPEX_CA_BUNDLE

        try:
            import tempfile
            from requests.certs import where as requests_ca_where

            with open(requests_ca_where(), "r", encoding="utf-8") as f:
                base_pem = f.read()

            bundle_path = os.path.join(tempfile.gettempdir(), "tpex_ca_bundle.pem")
            with open(bundle_path, "w", encoding="utf-8") as f:
                f.write(base_pem.rstrip() + "\n" + TPEX_TWCA_INTERMEDIATE_PEM)

            _TPEX_CA_BUNDLE = bundle_path
            log(f"✅ TPEx 使用補上 TWCA 中繼憑證的 CA 檔：{bundle_path}")
        except Exception as e:
            log(f"⚠️ TPEx CA 檔建立失敗，改用 requests 預設憑證：{type(e).__name__}: {e}")
            _TPEX_CA_BUNDLE = True

        return _TPEX_CA_BUNDLE


# ================= 資料層 =================
_SESSION = requests.Session()

# 欄名對照：先比完全相等，再比包含。TWSE 用「收盤價」，TPEx 用「收盤」。
_FIELD_ALIASES = {
    "code": (("證券代號", "代號", "股票代號"), ("證券代號", "股票代號")),
    "name": (("證券名稱", "名稱", "股票名稱"), ("證券名稱", "股票名稱")),
    "open": (("開盤價", "開盤"), ("開盤",)),
    "high": (("最高價", "最高"), ("最高",)),
    "low": (("最低價", "最低"), ("最低",)),
    "close": (("收盤價", "收盤"), ("收盤",)),
    "volume": (("成交股數",), ("成交股數",)),
}
_REQUIRED_FIELDS = ("code", "high", "low", "close", "volume")


def _field_index_map(fields) -> dict:
    normalized = [_normalize_header(f) for f in fields]
    result = {}
    for key, (exact, contains) in _FIELD_ALIASES.items():
        idx = next((i for i, h in enumerate(normalized) if h in exact), None)
        if idx is None:
            idx = next((i for i, h in enumerate(normalized) if any(t in h for t in contains)), None)
        if idx is not None:
            result[key] = idx
    return result


def _iter_tables(payload):
    """走訪 TWSE / TPEx 各種 JSON 格式，吐出 (fields, rows)。

    - 新版 TWSE / TPEx：{"tables": [{"fields": [...], "data": [...]}, ...]}
    - 舊版 TWSE：fields1/data1、fields9/data9 ...
    - 舊版 TPEx：fields/aaData
    """
    stack = [payload]
    while stack:
        obj = stack.pop()
        if isinstance(obj, list):
            stack.extend(x for x in obj if isinstance(x, (dict, list)))
            continue
        if not isinstance(obj, dict):
            continue
        for key, val in obj.items():
            if not (isinstance(key, str) and key.startswith("fields") and isinstance(val, list)):
                continue
            suffix = key[len("fields"):]
            for data_key in (f"data{suffix}", "aaData"):
                rows = obj.get(data_key)
                if isinstance(rows, list):
                    yield val, rows
                    break
        for val in obj.values():
            if isinstance(val, (dict, list)):
                stack.append(val)


def _payload_date_matches(payload, date_str: str) -> bool:
    """回應若帶日期必須等於請求日。

    TPEx 查詢非交易日時有機會回「最近一個交易日」的資料，不檢查會把同一天的行情算兩次，
    20 日最低 / 均量全部算錯。所以日期對不上就整批丟棄。
    """
    if not isinstance(payload, dict):
        return True
    for key in ("date", "reportDate"):
        raw = payload.get(key)
        if not raw:
            continue
        digits = re.sub(r"[^0-9]", "", str(raw))
        if len(digits) == 7:  # 民國年 1151013
            digits = f"{int(digits[:3]) + 1911}{digits[3:]}"
        if len(digits) == 8 and digits != date_str:
            log(f"⚠️ 回應日期 {digits} ≠ 請求日 {date_str}，整批丟棄")
            return False
    return True


def _parse_daily_payload(payload, date_str: str, market: str) -> pd.DataFrame:
    if not _payload_date_matches(payload, date_str):
        return pd.DataFrame()

    records = {}
    for fields, rows in _iter_tables(payload):
        idx = _field_index_map(fields)
        if any(k not in idx for k in _REQUIRED_FIELDS):
            continue
        max_idx = max(idx.values())
        for row in rows:
            if not isinstance(row, (list, tuple)) or len(row) <= max_idx:
                continue
            code = clean_cell(row[idx["code"]]).upper()
            if not _is_target_code(code):
                continue
            records[code] = {
                "date": date_str,
                "code": code,
                "name": clean_cell(row[idx["name"]]) if "name" in idx else "",
                "market": market,
                "open": to_float(row[idx["open"]]) if "open" in idx else None,
                "high": to_float(row[idx["high"]]),
                "low": to_float(row[idx["low"]]),
                "close": to_float(row[idx["close"]]),
                # 交易所給股數，XQ 日線 Volume 是張，這裡統一換成張。
                "volume": (to_float(row[idx["volume"]], 0.0) or 0.0) / 1000.0,
            }
    return pd.DataFrame(list(records.values()))


def _get_json_with_retry(url, params, headers, verify=True, label="request"):
    last_err = None
    for i in range(1, REQUEST_RETRIES + 1):
        try:
            r = _SESSION.get(url, params=params, headers=headers, timeout=(8, 30), verify=verify)
            r.raise_for_status()
            try:
                return r.json()
            except ValueError:
                # TWSE 被限流時會回 HTML 而不是 JSON，當成可重試錯誤。
                raise RuntimeError(f"非 JSON 回應（前 80 字）：{r.text[:80]!r}")
        except requests.exceptions.SSLError as e:
            # 憑證錯誤每次結果都一樣，重試只是白等。
            log(f"❌ {label} SSL 憑證驗證失敗，不重試：{e}")
            raise
        except Exception as e:
            last_err = e
            log(f"⚠️ {label} 第 {i}/{REQUEST_RETRIES} 次失敗：{e}")
            if i < REQUEST_RETRIES:
                time.sleep(REQUEST_RETRY_SLEEP * i)
    raise last_err


def fetch_twse_day(d: datetime) -> pd.DataFrame:
    """上市單日全市場。假日回空 DataFrame（stat 不是 OK、沒有表格）。"""
    date_str = d.strftime("%Y%m%d")
    # ALLBUT0999 = 全部（不含權證、牛熊證），type=ALL 會多出上萬筆權證白白拖慢解析。
    params = {"response": "json", "date": date_str, "type": "ALLBUT0999"}
    payload = _get_json_with_retry(TWSE_DAILY_URL, params, HDR_TWSE, label=f"TWSE {date_str}")
    return _parse_daily_payload(payload, date_str, "上市")


def fetch_tpex_day(d: datetime) -> pd.DataFrame:
    """上櫃單日全市場。"""
    date_str = d.strftime("%Y%m%d")
    params = {"date": d.strftime("%Y/%m/%d"), "id": "", "response": "json"}
    payload = _get_json_with_retry(
        TPEX_DAILY_URL, params, HDR_TPEX, verify=_tpex_get_ca_bundle(), label=f"TPEx {date_str}"
    )
    return _parse_daily_payload(payload, date_str, "上櫃")


def fetch_recent_trading_days(end_date: datetime):
    """從 end_date 往回抓，直到湊滿 NEED_TRADING_DAYS 個交易日。

    以 TWSE 判斷是否為交易日（TWSE 有資料 = 交易日），TPEx 跟著抓同一天。
    最新一天若上市或上櫃還沒公布，就自動往前找「上市櫃都有資料」的最近交易日當結算日——
    2026-10-13 14:35 實測：TWSE 當日 1,087 檔已出，TPEx 同一天還是 0 筆，
    兩個交易所公布時間不同步，不能只看 TWSE。
    結算日確定之後，更早的日子缺資料就是真的抓取失敗，照 MAX_ALLOWED_FAILED_DATES 中止。
    回傳 (DataFrame, 交易日清單由舊到新)；SKIP_IF_NOT_TODAY 且最新資料不是 end_date 時回 (None, [])。
    """
    frames = []
    trading_days = []
    failed = []
    d = end_date
    days_back_left = DAYS_BACK

    for _ in range(MAX_CALENDAR_SCAN_DAYS + DAYS_BACK * 2):
        if len(trading_days) >= NEED_TRADING_DAYS:
            break
        # 還沒找到結算日之前，缺資料一律當「尚未公布 / 休市」往前找，不算失敗。
        finding_latest = not trading_days

        if d.weekday() >= 5:
            d -= timedelta(days=1)
            continue

        date_str = d.strftime("%Y%m%d")
        try:
            twse = fetch_twse_day(d)
        except Exception as e:
            if finding_latest:
                log(f"⚠️ TWSE {d:%Y/%m/%d} 抓取失敗，改找前一個交易日：{e}")
                time.sleep(REQUEST_SLEEP)
                d -= timedelta(days=1)
                continue
            failed.append(f"TWSE {date_str}：{e}")
            twse = None

        if twse is not None and twse.empty:
            log(f"⏭️ {d:%Y/%m/%d} 上市無行情（休市或尚未公布）")
            time.sleep(REQUEST_SLEEP)
            d -= timedelta(days=1)
            continue

        try:
            tpex = fetch_tpex_day(d)
            tpex_err = "交易日卻沒有資料" if tpex.empty else ""
        except Exception as e:
            tpex = None
            tpex_err = str(e)

        if tpex_err and finding_latest:
            log(f"⚠️ TPEx {d:%Y/%m/%d} 上櫃行情尚未公布，改用前一個交易日當結算日")
            time.sleep(REQUEST_SLEEP)
            d -= timedelta(days=1)
            continue
        if tpex_err:
            failed.append(f"TPEx {date_str}：{tpex_err}")

        # 手動選「往前 N 個交易日」：完整交易日先跳過 N 個，再開始當結算日累積。
        if finding_latest and days_back_left > 0:
            log(f"⏭️ {d:%Y/%m/%d} 有完整行情，依 DAYS_BACK 再往前（剩 {days_back_left} 天）")
            days_back_left -= 1
            time.sleep(REQUEST_SLEEP)
            d -= timedelta(days=1)
            continue

        if SKIP_IF_NOT_TODAY and finding_latest and date_str != end_date.strftime("%Y%m%d"):
            log(f"⚠️ 最新完整行情是 {d:%Y/%m/%d}，不是 {end_date:%Y/%m/%d}，依設定不推播。")
            return None, []

        if twse is not None:
            frames.append(twse)
        if tpex is not None and not tpex.empty:
            frames.append(tpex)
        trading_days.append(date_str)
        log(
            f"✅ {d:%Y/%m/%d}｜上市 {0 if twse is None else len(twse):,} 檔"
            f"｜上櫃 {0 if tpex is None else len(tpex):,} 檔"
            f"｜進度 {len(trading_days)}/{NEED_TRADING_DAYS}"
        )

        if len(failed) > MAX_ALLOWED_FAILED_DATES:
            raise RuntimeError(f"抓取失敗 {len(failed)} 次，超過上限 {MAX_ALLOWED_FAILED_DATES}：{failed}")

        time.sleep(REQUEST_SLEEP)
        d -= timedelta(days=1)

    if len(trading_days) < NEED_TRADING_DAYS:
        raise RuntimeError(
            f"往回掃 {MAX_CALENDAR_SCAN_DAYS} 天只湊到 {len(trading_days)} 個交易日"
            f"（需要 {NEED_TRADING_DAYS}），中止。"
        )
    if failed:
        log(f"⚠️ 在容許範圍內的抓取失敗：{failed}")

    return pd.concat(frames, ignore_index=True), sorted(trading_days)


# ================= 運算層 =================
def build_lower_shadow_hits(df: pd.DataFrame, trading_days) -> pd.DataFrame:
    """把 XQ 五個條件套在最後一個交易日上，回傳符合的股票。"""
    last_day = trading_days[-1]

    def panel(col):
        return df.pivot(index="date", columns="code", values=col).reindex(trading_days).sort_index()

    high = panel("high")
    low = panel("low")
    close = panel("close")
    # 沒成交那天交易所給 '--'，量視為 0（XQ 也是 0 量 K 棒）。
    volume = panel("volume").fillna(0.0)

    # Value30[1] = 前 LOW_LOOKBACK 日最低價（不含今日）。
    # 窗口內有停牌（價格 NaN）時 rolling 湊不滿 min_periods 會得到 NaN，該股直接不入選——
    # 停牌股前後低點不連續，這樣比較保守。
    lowest_prev = low.shift(1).rolling(LOW_LOOKBACK, min_periods=LOW_LOOKBACK).min()
    vol_ma = volume.rolling(VOL_MA_DAYS, min_periods=VOL_MA_DAYS).mean()

    c = close.loc[last_day]
    h = high.loc[last_day]
    lo = low.loc[last_day]
    v = volume.loc[last_day]
    prev_close = close.shift(1).loc[last_day]
    low_ref = lowest_prev.loc[last_day]
    ma = vol_ma.loc[last_day]

    cond1 = (c - lo) > c * SHADOW_PCT
    cond2 = (h - lo) > c * RANGE_PCT
    cond3 = lo > low_ref
    cond4 = v > ma * VOL_RATIO
    cond5 = v > MIN_VOLUME_LOTS
    # NaN 比較一律 False，資料不全的股票自然被排除。
    mask = cond1 & cond2 & cond3 & cond4 & cond5

    codes = mask[mask].index.tolist()
    if not codes:
        return pd.DataFrame()

    latest = df[df["date"] == last_day].set_index("code")
    out = pd.DataFrame({
        "代號": codes,
        "名稱": [latest.at[k, "name"] for k in codes],
        "市場": [latest.at[k, "market"] for k in codes],
        "收盤": c[codes].values,
        "漲跌幅%": ((c[codes] / prev_close[codes] - 1) * 100).round(2).values,
        "下引線%": ((c[codes] - lo[codes]) / c[codes] * 100).round(2).values,
        "振幅%": ((h[codes] - lo[codes]) / c[codes] * 100).round(2).values,
        "最低": lo[codes].values,
        f"前{LOW_LOOKBACK}日低": low_ref[codes].values,
        "成交量(張)": v[codes].round(0).astype(int).values,
        "量/均量": (v[codes] / ma[codes]).round(2).values,
    })
    return out.sort_values("下引線%", ascending=False).reset_index(drop=True)


# ================= 推播層 =================
def _post_discord(content: str) -> bool:
    try:
        r = requests.post(DISCORD_WEBHOOK_URL, json={"content": content}, timeout=30)
        if r.status_code in (200, 204):
            return True
        log(f"❌ Discord 推播失敗：{r.status_code} {r.text[:200]}")
    except Exception as e:
        log(f"❌ Discord 推播例外：{e}")
    return False


def _fmt_day(yyyymmdd: str) -> str:
    return f"{yyyymmdd[:4]}/{yyyymmdd[4:6]}/{yyyymmdd[6:]}"


def send_hits_to_discord(hits: pd.DataFrame, last_day: str, requested_day: str):
    if not DISCORD_WEBHOOK_URL:
        log("⚠️ 找不到 DISCORD_WEBHOOK_URL_TEST / DISCORD_WEBHOOK_URL，略過 Discord 推播。")
        return

    header = f"📊 **XQ 下引線選股｜資料日期：{_fmt_day(last_day)}｜共 {len(hits)} 檔**"
    # 改用前一個交易日時一定要講清楚，不然看的人會以為是今天的盤。
    if DAYS_BACK > 0:
        header += f"\n🔁 手動回補：最新交易日往前 {DAYS_BACK} 個交易日的選股結果。"
    elif last_day != requested_day:
        header += (
            f"\n⚠️ {_fmt_day(requested_day)} 沒有完整行情（休市或交易所尚未公布），"
            f"以下為最近交易日 **{_fmt_day(last_day)}** 收盤的選股結果。"
        )

    if hits.empty:
        if PUSH_WHEN_EMPTY:
            _post_discord(f"{header}\n本日無符合條件個股。")
        return

    # Discord 只列代號、名稱、當日漲跌幅；其餘欄位（下引、振幅、量比）留在 CSV。
    lines = []
    for _, r in hits.iterrows():
        chg = r["漲跌幅%"]
        chg_text = "—" if pd.isna(chg) else f"{chg:+.2f}%"
        lines.append(f"`{r['代號']}` {r['名稱']}｜{chg_text}")

    # 依 Discord 2000 字上限切段，第一段帶標題。
    chunks = []
    buf = header
    for line in lines:
        if len(buf) + 1 + len(line) > DISCORD_MAX_CHARS:
            chunks.append(buf)
            buf = line
        else:
            buf = f"{buf}\n{line}"
    chunks.append(buf)

    for i, chunk in enumerate(chunks, 1):
        if _post_discord(chunk):
            log(f"✅ Discord 推播完成 {i}/{len(chunks)}")
        time.sleep(1)  # Discord webhook 限流約每秒 5 則，保守一點


def save_hits_csv(hits: pd.DataFrame, last_day: str) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    path = OUTPUT_DIR / f"lower_shadow_{last_day}.csv"
    hits.to_csv(path, index=False, encoding="utf-8-sig")
    return path


# ================= 主流程 =================
def push_lower_shadow_to_dc():
    start = time.time()
    log("=" * 80)
    log("🚀 啟動：XQ 下引線選股｜TWSE / TPEx 全市場日行情")
    log(
        f"條件：下引>{SHADOW_PCT:.0%}｜振幅>{RANGE_PCT:.0%}｜低點>前{LOW_LOOKBACK}日低"
        f"｜量>{VOL_MA_DAYS}日均量×{VOL_RATIO}｜量>{MIN_VOLUME_LOTS:g}張"
    )
    log("=" * 80)

    if TARGET_DATE:
        end_date = datetime.strptime(TARGET_DATE, "%Y%m%d")
        log(f"指定結算日：{TARGET_DATE}")
    else:
        end_date = datetime.now(TW_TZ).replace(tzinfo=None)

    requested_day = end_date.strftime("%Y%m%d")
    df, trading_days = fetch_recent_trading_days(end_date)
    if df is None:
        log("結束：今日無行情，不選股。")
        return

    last_day = trading_days[-1]
    if last_day != requested_day:
        log(f"⚠️ {requested_day} 無完整行情，改用最近交易日 {last_day} 結算")
    hits = build_lower_shadow_hits(df, trading_days)
    log(f"📊 {last_day} 符合 {len(hits)} 檔")

    csv_path = save_hits_csv(hits, last_day)
    log(f"✅ CSV 已輸出：{csv_path}")

    send_hits_to_discord(hits, last_day, requested_day)

    log("=" * 80)
    log("完成：XQ 下引線選股")
    log(f"資料區間：{trading_days[0]} ～ {last_day}（{len(trading_days)} 個交易日）")
    log(f"⏱️ 耗時：{time.time() - start:.2f} 秒")
    log("=" * 80)


if __name__ == "__main__":
    push_lower_shadow_to_dc()
