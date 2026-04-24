from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional
from datetime import date, timedelta, datetime
import yfinance as yf
import pandas as pd
import pymysql
import os
import requests as _requests
import re as _re
from collections import defaultdict
from dotenv import load_dotenv
import boto3
import json

load_dotenv()

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# yfinance fetch period per interval (limited by API)
INTERVALS = {
    "1h":  {"period": "730d", "is_date": False},
    "1d":  {"period": "2y",   "is_date": True},
    "1wk": {"period": "5y",   "is_date": True},
}


def get_conn():
    return pymysql.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME", "stockdb"),
        cursorclass=pymysql.cursors.DictCursor,
    )


def init_db():
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                trade_date VARCHAR(10) NOT NULL,
                code VARCHAR(20) NOT NULL,
                name VARCHAR(200),
                side VARCHAR(4) NOT NULL,
                qty FLOAT NOT NULL,
                price FLOAT NOT NULL,
                commission FLOAT DEFAULT 0,
                tax FLOAT DEFAULT 0,
                settlement FLOAT DEFAULT 0,
                trade_key VARCHAR(300) NOT NULL,
                UNIQUE KEY uq_trade_key (trade_key),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS candles (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                symbol VARCHAR(20) NOT NULL,
                interval_type VARCHAR(10) NOT NULL,
                candle_time VARCHAR(30) NOT NULL,
                open FLOAT,
                high FLOAT,
                low FLOAT,
                close FLOAT,
                volume BIGINT DEFAULT 0,
                UNIQUE KEY uq_candle (symbol, interval_type, candle_time)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS analysis_history (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                analysis_text MEDIUMTEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        try:
            cur.execute("ALTER TABLE analysis_history ADD COLUMN label VARCHAR(300)")
        except Exception:
            pass
        cur.execute("""
            CREATE TABLE IF NOT EXISTS signal_history (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                analysis_id BIGINT NOT NULL,
                symbol VARCHAR(20) NOT NULL,
                interval_type VARCHAR(10) NOT NULL,
                candle_time VARCHAR(30) NOT NULL,
                side VARCHAR(4) NOT NULL,
                price FLOAT NOT NULL,
                reason VARCHAR(500),
                stop_loss FLOAT DEFAULT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS news_cache (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                symbol VARCHAR(20) NOT NULL,
                url VARCHAR(1000) NOT NULL,
                title VARCHAR(500),
                title_ja VARCHAR(500),
                published BIGINT,
                source VARCHAR(200),
                summary_ja TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY uq_news (symbol, url(500))
            )
        """)
        try:
            cur.execute("ALTER TABLE signal_history ADD COLUMN stop_loss FLOAT DEFAULT NULL")
        except Exception:
            pass
        cur.execute("""
            CREATE TABLE IF NOT EXISTS financials_cache (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                symbol VARCHAR(20) NOT NULL,
                data MEDIUMTEXT NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY uq_fin (symbol)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS holders_cache (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                symbol VARCHAR(20) NOT NULL,
                data MEDIUMTEXT NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY uq_hld (symbol)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS favorites (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                symbol VARCHAR(20) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY uq_fav (symbol)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS analyst_cache (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                symbol VARCHAR(20) NOT NULL,
                data MEDIUMTEXT NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY uq_analyst (symbol)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS chat_history (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                symbol VARCHAR(20) NOT NULL,
                role VARCHAR(20) NOT NULL,
                content MEDIUMTEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_chat_symbol (symbol, created_at)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS stock_memos (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                symbol VARCHAR(20) NOT NULL,
                content MEDIUMTEXT NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY uq_memo (symbol)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS trend_lines (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                symbol VARCHAR(20) NOT NULL,
                interval_type VARCHAR(10) NOT NULL,
                p1_time VARCHAR(30) NOT NULL,
                p1_value FLOAT NOT NULL,
                p2_time VARCHAR(30) NOT NULL,
                p2_value FLOAT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_tl (symbol, interval_type)
            )
        """)
    conn.commit()
    conn.close()


@app.on_event("startup")
def startup():
    init_db()


# ===== FAVORITES =====

@app.get("/favorites")
def get_favorites():
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT f.symbol, COALESCE(p.name, '') AS name
                FROM favorites f
                LEFT JOIN prime_stocks p ON p.code = f.symbol
                ORDER BY f.created_at DESC
            """)
            rows = cur.fetchall()
            return [{"code": r["symbol"], "name": r["name"]} for r in rows]
    finally:
        conn.close()

@app.post("/favorites/{symbol}")
def add_favorite(symbol: str):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("INSERT IGNORE INTO favorites (symbol) VALUES (%s)", (symbol,))
        conn.commit()
        return {"ok": True}
    finally:
        conn.close()

@app.delete("/favorites/{symbol}")
def remove_favorite(symbol: str):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM favorites WHERE symbol=%s", (symbol,))
        conn.commit()
        return {"ok": True}
    finally:
        conn.close()


# ===== TRADES =====

class Trade(BaseModel):
    date: str
    code: str
    name: str
    side: str
    qty: float
    price: float
    commission: float = 0
    tax: float = 0
    settlement: float = 0
    trade_key: str


@app.get("/trades")
def get_trades():
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT trade_date as `date`, code, name, side, qty, price, "
                "commission, tax, settlement FROM trades ORDER BY trade_date, code"
            )
            return cur.fetchall()
    finally:
        conn.close()


@app.post("/trades")
def save_trades(trades: List[Trade]):
    if not trades:
        return {"added": 0}
    conn = get_conn()
    added = 0
    try:
        with conn.cursor() as cur:
            for t in trades:
                try:
                    cur.execute(
                        """INSERT IGNORE INTO trades
                           (trade_date, code, name, side, qty, price, commission, tax, settlement, trade_key)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                        (t.date, t.code, t.name, t.side, t.qty, t.price,
                         t.commission, t.tax, t.settlement, t.trade_key)
                    )
                    if cur.rowcount > 0:
                        added += 1
                except Exception:
                    pass
        conn.commit()
        return {"added": added}
    finally:
        conn.close()


@app.delete("/trades")
def delete_trades():
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM trades")
        conn.commit()
        return {"deleted": True}
    finally:
        conn.close()


# ===== CANDLES =====

def _df_to_candles(df, is_date: bool) -> list:
    result = []
    for ts, row in df.iterrows():
        try:
            if is_date:
                t = str(ts.date())
            else:
                pt = pd.Timestamp(ts)
                utc = int(pt.tz_convert("UTC").timestamp()) if pt.tzinfo else int(pt.timestamp())
                t = str(utc + 9 * 3600)
            result.append({
                "time":   t,
                "open":   round(float(row["Open"]), 1),
                "high":   round(float(row["High"]), 1),
                "low":    round(float(row["Low"]),  1),
                "close":  round(float(row["Close"]), 1),
                "volume": int(row["Volume"]) if not pd.isna(row["Volume"]) else 0,
            })
        except Exception:
            pass
    return result


TWELVE_DATA_API_KEY = "1810b430cc7e4b92a7d83e845d2573fc"

def _fetch_twelve_data(sym: str, interval: str) -> list:
    """Twelve Data APIで日本株OHLCVを取得（JPX上場銘柄向け）"""
    import datetime as _dt
    # sym例: "7203.T" → "7203", "6758.T" → "6758"
    td_sym = sym.replace(".T", "").replace(".OS", "")
    interv_map = {"1d": "1day", "1wk": "1week", "1h": "1h"}
    outputsize_map = {"1d": 500, "1wk": 260, "1h": 500}
    td_interval = interv_map.get(interval)
    if not td_interval:
        return []
    url = "https://api.twelvedata.com/time_series"
    params = {
        "symbol": td_sym,
        "exchange": "JPX",
        "interval": td_interval,
        "outputsize": outputsize_map.get(interval, 500),
        "apikey": TWELVE_DATA_API_KEY,
        "timezone": "Asia/Tokyo",
        "order": "ASC",
    }
    try:
        resp = _requests.get(url, params=params, timeout=15)
        data = resp.json()
        if data.get("status") != "ok" or "values" not in data:
            return []
        bars = []
        for v in data["values"]:
            try:
                close = float(v["close"])
                open_ = float(v["open"])
                high  = float(v["high"])
                low   = float(v["low"])
                vol   = int(v.get("volume", 0) or 0)
                dt_str = v["datetime"]  # "2026-04-17" or "2026-04-17 09:00:00"
                if interval in ("1d", "1wk"):
                    t = dt_str[:10]  # date only
                else:
                    # 1h: "2026-04-17 09:00:00" JST → unix timestamp
                    # strptime without tz = naive, dt.timestamp() uses local tz on server (UTC)
                    # so subtract 9h offset manually to convert JST→UTC unix
                    dt = _dt.datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
                    # サーバーはUTC: naive JST文字列をそのままtimestamp()するとJST-offset unix
                    # = Yahoo rawの ts+9*3600 と同じ形式（LightweightCharts用）
                    t = str(int(dt.timestamp()))
                bars.append({
                    "time":   t,
                    "open":   round(open_, 1),
                    "high":   round(high,  1),
                    "low":    round(low,   1),
                    "close":  round(close, 1),
                    "volume": vol,
                })
            except Exception:
                pass
        return bars
    except Exception:
        return []


def _fetch_yahoo_raw(sym: str, interval: str) -> list:
    """Yahoo Finance生APIを直接叩いて取得（yfinanceより最新データが得られる）"""
    import datetime as _dt
    range_map  = {"1d": "2y",  "1wk": "5y", "1h": "60d"}
    interv_map = {"1d": "1d",  "1wk": "1wk","1h": "1h"}
    is_date    = interval in ("1d", "1wk")

    url = (f"https://query2.finance.yahoo.com/v8/finance/chart/{sym}"
           f"?interval={interv_map[interval]}&range={range_map[interval]}")
    headers = {"User-Agent": "Mozilla/5.0 (compatible)"}
    try:
        resp = _requests.get(url, headers=headers, timeout=15)
        data = resp.json()
        res0 = data.get("chart", {}).get("result", [None])[0]
        if not res0:
            return []
        timestamps = res0.get("timestamp", [])
        q = res0["indicators"]["quote"][0]
        bars = []
        for i, ts in enumerate(timestamps):
            try:
                close = q["close"][i]
                if close is None:          # 未確定バー（取引中・休場）はスキップ
                    continue
                open_  = q["open"][i]  or close
                high   = q["high"][i]  or close
                low    = q["low"][i]   or close
                vol    = q["volume"][i] or 0
                if is_date:
                    t = str(_dt.datetime.utcfromtimestamp(ts).date())
                else:
                    t = str(int(ts) + 9 * 3600)   # JST unix → LightweightCharts用
                bars.append({
                    "time":   t,
                    "open":   round(float(open_), 1),
                    "high":   round(float(high),  1),
                    "low":    round(float(low),   1),
                    "close":  round(float(close), 1),
                    "volume": int(vol),
                })
            except Exception:
                pass
        return bars
    except Exception:
        return []


def fetch_from_yfinance(sym: str, interval: str) -> list:
    cfg = INTERVALS[interval]
    from datetime import date as _date
    today_str = str(_date.today())
    is_japan = sym.endswith(".T") or sym.endswith(".OS")

    # 1. 日本株はTwelve Dataを優先
    result = []
    if is_japan:
        result = _fetch_twelve_data(sym, interval)

    # 2. Twelve Dataが空 or 非日本株 → Yahoo Finance 生API
    if not result:
        result = _fetch_yahoo_raw(sym, interval)

    # 3. それも空ならyfinanceにフォールバック
    if not result:
        ticker = yf.Ticker(sym)
        df = ticker.history(period=cfg["period"], interval=interval, auto_adjust=True)
        result = _df_to_candles(df, cfg["is_date"]) if not df.empty else []

    # 4. 日本株の1時間足はTwelve Dataが提供するので補完不要
    #    非日本株の日足のみ1h補完を試みる
    if interval == "1d" and not is_japan:
        try:
            ticker = yf.Ticker(sym)
            df_h = ticker.history(period="1d", interval="1h", auto_adjust=True)
            if not df_h.empty:
                daily = {}
                for ts, row in df_h.iterrows():
                    d = str(ts.date())
                    if d not in daily:
                        daily[d] = {"open": float(row["Open"]), "high": float(row["High"]),
                                    "low": float(row["Low"]), "close": float(row["Close"]),
                                    "volume": int(row["Volume"]) if not pd.isna(row["Volume"]) else 0}
                    else:
                        daily[d]["high"]   = max(daily[d]["high"],   float(row["High"]))
                        daily[d]["low"]    = min(daily[d]["low"],    float(row["Low"]))
                        daily[d]["close"]  = float(row["Close"])
                        daily[d]["volume"] += int(row["Volume"]) if not pd.isna(row["Volume"]) else 0
                existing = {c["time"] for c in result}
                added = False
                for d, v in daily.items():
                    bar = {"time": d, "open": round(v["open"],1), "high": round(v["high"],1),
                           "low": round(v["low"],1), "close": round(v["close"],1), "volume": v["volume"]}
                    if d not in existing:
                        result.append(bar); existing.add(d); added = True
                    elif d == today_str:
                        result = [bar if c["time"]==d else c for c in result]
                if added:
                    result.sort(key=lambda x: x["time"])
        except Exception:
            pass

    return result


def save_candles_to_db(conn, symbol: str, interval: str, candles: list):
    with conn.cursor() as cur:
        for c in candles:
            try:
                cur.execute(
                    """INSERT INTO candles
                       (symbol, interval_type, candle_time, open, high, low, close, volume)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                       ON DUPLICATE KEY UPDATE
                         open=%s, high=%s, low=%s, close=%s, volume=%s""",
                    (symbol, interval, str(c["time"]),
                     c["open"], c["high"], c["low"], c["close"], c["volume"],
                     c["open"], c["high"], c["low"], c["close"], c["volume"])
                )
            except Exception:
                pass
    conn.commit()


def load_candles_from_db(conn, symbol: str, interval: str) -> list:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT candle_time, open, high, low, close, volume FROM candles "
            "WHERE symbol=%s AND interval_type=%s ORDER BY candle_time",
            (symbol, interval)
        )
        rows = cur.fetchall()
    cfg = INTERVALS[interval]
    result = []
    for r in rows:
        t = r["candle_time"]
        result.append({
            "time":   t if cfg["is_date"] else int(t),
            "open":   r["open"],
            "high":   r["high"],
            "low":    r["low"],
            "close":  r["close"],
            "volume": r["volume"],
        })
    return result


def get_candle_context(conn, code: str, date_str: str, n: int = 25) -> dict:
    """指定日前後のローソク足からMA/BB/出来高などの指標を計算して返す"""
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT candle_time, open, high, low, close, volume FROM candles "
                "WHERE symbol=%s AND interval_type='1d' AND candle_time <= %s "
                "ORDER BY candle_time DESC LIMIT %s",
                (code, date_str, n)
            )
            rows = cur.fetchall()
        if len(rows) < 5:
            return {}
        rows = list(reversed(rows))
        closes = [r['close'] for r in rows]
        volumes = [r['volume'] for r in rows]
        price = closes[-1]

        # MA
        ma5  = sum(closes[-5:])  / 5  if len(closes) >= 5  else None
        ma25 = sum(closes[-25:]) / 25 if len(closes) >= 25 else None

        # Bollinger (20期間, 2σ)
        if len(closes) >= 20:
            c20 = closes[-20:]
            mean = sum(c20) / 20
            std  = (sum((x - mean)**2 for x in c20) / 20) ** 0.5
            bb_upper = mean + 2 * std
            bb_lower = mean - 2 * std
            bb_pct = (price - bb_lower) / (bb_upper - bb_lower) * 100 if bb_upper != bb_lower else 50
        else:
            bb_pct = None

        # 出来高トレンド（直近5日平均 vs 直近25日平均）
        vol_avg5  = sum(volumes[-5:])  / 5  if len(volumes) >= 5  else None
        vol_avg25 = sum(volumes[-25:]) / 25 if len(volumes) >= 25 else None
        vol_ratio = vol_avg5 / vol_avg25 if vol_avg5 and vol_avg25 and vol_avg25 > 0 else None

        # 一目: 転換線(9)・基準線(26)
        def midprice(cs):
            return (max(cs) + min(cs)) / 2
        tenkan = midprice(closes[-9:])  if len(closes) >= 9  else None
        kijun  = midprice(closes[-26:]) if len(closes) >= 26 else None

        ctx = {"price": price}
        if ma5:  ctx["ma5"]  = round(ma5, 1)
        if ma25: ctx["ma25"] = round(ma25, 1)
        if bb_pct is not None: ctx["bb_pct"] = round(bb_pct, 1)
        if tenkan: ctx["tenkan"] = round(tenkan, 1)
        if kijun:  ctx["kijun"]  = round(kijun, 1)
        if vol_ratio: ctx["vol_ratio"] = round(vol_ratio, 2)
        return ctx
    except Exception:
        return {}


def describe_context(ctx: dict, side: str) -> str:
    if not ctx:
        return ""
    parts = []
    p = ctx.get("price", 0)
    ma5 = ctx.get("ma5")
    ma25 = ctx.get("ma25")
    if ma5:
        rel = "上" if p > ma5 else "下"
        parts.append(f"MA5{rel}({abs(p-ma5)/p*100:.1f}%乖離)")
    if ma25:
        rel = "上" if p > ma25 else "下"
        parts.append(f"MA25{rel}({abs(p-ma25)/p*100:.1f}%乖離)")
    if ctx.get("bb_pct") is not None:
        bp = ctx["bb_pct"]
        if bp > 80:   parts.append(f"BB上限付近({bp:.0f}%)")
        elif bp < 20: parts.append(f"BB下限付近({bp:.0f}%)")
        else:         parts.append(f"BB中間({bp:.0f}%)")
    if ctx.get("tenkan") and ctx.get("kijun"):
        t, k = ctx["tenkan"], ctx["kijun"]
        rel = "転換線>基準線(上昇シグナル)" if t > k else "転換線<基準線(下降シグナル)"
        parts.append(f"一目{rel}")
    if ctx.get("vol_ratio"):
        vr = ctx["vol_ratio"]
        if vr > 1.5:   parts.append(f"出来高急増({vr:.1f}倍)")
        elif vr > 1.1: parts.append(f"出来高やや増({vr:.1f}倍)")
        elif vr < 0.7: parts.append(f"出来高低調({vr:.1f}倍)")
    return "、".join(parts) if parts else ""


def build_candle_summary(symbol: str, interval: str) -> str:
    """現在表示中の銘柄のローソク足サマリーを文字列で返す（シグナル生成用）"""
    try:
        conn = get_conn()
        candles = load_candles_from_db(conn, symbol, interval)
        conn.close()
        if not candles:
            return ""
        # 時間足ごとに送る本数を調整（トークン節約のため列は date,close,MA25,BB%,IKクロス,VIX に絞る）
        limits = {"1wk": len(candles), "1d": 400, "1h": 120}
        target = candles[-limits.get(interval, 400):]
        closes = [c['close'] for c in target]
        highs  = [c.get('high',  c['close']) for c in target]
        lows   = [c.get('low',   c['close']) for c in target]

        # VIX過去データを取得（1d足のみ。他の足は現在値のみ使用）
        vix_by_date = {}
        try:
            import yfinance as yf
            from datetime import datetime, timezone, timedelta
            # データ範囲を計算
            def to_date_str(c):
                t = c['time']
                if isinstance(t, str): return t[:10]
                return datetime.fromtimestamp(int(t), tz=timezone.utc).strftime('%Y-%m-%d')
            first_date = to_date_str(target[0])
            last_date  = to_date_str(target[-1])
            # 終了日の翌日まで取得
            end_dt = (datetime.strptime(last_date, '%Y-%m-%d') + timedelta(days=2)).strftime('%Y-%m-%d')
            vix_df = yf.Ticker("^VIX").history(start=first_date, end=end_dt)
            if not vix_df.empty:
                for idx, row in vix_df.iterrows():
                    d = str(idx)[:10]
                    vix_by_date[d] = round(float(row['Close']), 1)
        except Exception:
            pass

        def _tk(i):
            if i < 8: return None
            return (max(highs[i-8:i+1]) + min(lows[i-8:i+1])) / 2
        def _kj(i):
            if i < 25: return None
            return (max(highs[i-25:i+1]) + min(lows[i-25:i+1])) / 2

        lines = ["日時,終値,MA25,BB%,IKクロス,VIX"]
        for i, c in enumerate(target):
            t = c['time'] if isinstance(c['time'], str) else str(c['time'])
            if not isinstance(c['time'], str):
                from datetime import datetime, timezone
                ts = int(c['time'])
                t = datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%Y-%m-%d')
            date_key = t[:10]
            ma25 = round(sum(closes[max(0,i-24):i+1]) / min(i+1, 25), 1)
            bb_pct = ""
            if i >= 19:
                c20   = closes[i-19:i+1]
                mean  = sum(c20) / 20
                std   = (sum((x - mean)**2 for x in c20) / 20) ** 0.5
                upper = mean + 2 * std
                lower = mean - 2 * std
                bb_pct = round((c['close'] - lower) / (upper - lower) * 100, 1) if upper != lower else 50
            # 一目均衡表クロス（その日に転換線と基準線が交差したかどうか）
            ik = ""
            if i > 0:
                tk_c, kj_c = _tk(i), _kj(i)
                tk_p, kj_p = _tk(i-1), _kj(i-1)
                if tk_c and kj_c and tk_p and kj_p:
                    if tk_p <= kj_p and tk_c > kj_c: ik = "上抜け"
                    elif tk_p >= kj_p and tk_c < kj_c: ik = "下抜け"
            vix_val = vix_by_date.get(date_key, "")
            lines.append(f"{t},{c['close']},{ma25},{bb_pct},{ik},{vix_val}")
        return "\n".join(lines)
    except Exception:
        return ""


def generate_rule_signals(symbol: str, interval: str, _candles=None, _vix_by_date=None) -> list:
    """ポイントスコアリング式ルールベースシグナル生成
    買い観点（各1pt）:
      TL=トレンドライン近傍, GC=ゴールデンクロス, IK3=一目三役好転,
      BB反転=BB下限から反転上昇, BBウォーク=BBバンドウォーク,
      抵抗ブレイク=直近高値ブレイク, 支持反転=直近安値で反転
    売り観点: DC=デッドクロス, BB↑=BB上限到達, IK↓=一目下抜け
    _candles: 事前取得済みのローソク足データ（省略時はDBから取得）
    _vix_by_date: 事前取得済みのVIX辞書 date->value（省略時はyfinanceから取得）
    """
    try:
        from datetime import datetime as _dt, timezone, timedelta
        if _candles is not None:
            candles = _candles
        else:
            conn = get_conn()
            candles = load_candles_from_db(conn, symbol, interval)
            conn.close()
        if len(candles) < 30:
            return {"signals": [], "scores": {}}

        closes  = [c['close'] for c in candles]
        highs   = [c.get('high', c['close']) for c in candles]
        lows    = [c.get('low',  c['close']) for c in candles]
        volumes = [c.get('volume', 0) or 0 for c in candles]

        def get_date(c):
            t = c['time']
            if isinstance(t, str): return t[:10]
            return _dt.fromtimestamp(int(t), tz=timezone.utc).strftime('%Y-%m-%d')

        # VIXデータ取得（事前取得済みがあればそれを使用）
        if _vix_by_date is not None:
            vix_by_date = _vix_by_date
        else:
            vix_by_date = {}
            try:
                first_date = get_date(candles[0])
                last_date  = get_date(candles[-1])
                end_dt = (_dt.strptime(last_date, '%Y-%m-%d') + timedelta(days=2)).strftime('%Y-%m-%d')
                vix_df = yf.Ticker("^VIX").history(start=first_date, end=end_dt)
                if not vix_df.empty:
                    for idx, row in vix_df.iterrows():
                        vix_by_date[str(idx)[:10]] = round(float(row['Close']), 1)
            except Exception:
                pass

        def get_vix(date_key):
            v = vix_by_date.get(date_key)
            if v is None:
                for d, val in sorted(vix_by_date.items(), reverse=True):
                    if d <= date_key:
                        return val
            return v


        def _ma(i, n):
            if i < n - 1: return None
            return sum(closes[i-n+1:i+1]) / n

        def _bb(i):
            if i < 19: return None
            c20 = closes[i-19:i+1]
            mean = sum(c20) / 20
            std  = (sum((x - mean)**2 for x in c20) / 20) ** 0.5
            upper = mean + 2 * std
            lower = mean - 2 * std
            if upper == lower: return 50.0
            return (closes[i] - lower) / (upper - lower) * 100

        def _ik_cross(i):
            """転換線・基準線クロス判定"""
            if i < 1: return ""
            def tk(j):
                if j < 8: return None
                return (max(highs[j-8:j+1]) + min(lows[j-8:j+1])) / 2
            def kj(j):
                if j < 25: return None
                return (max(highs[j-25:j+1]) + min(lows[j-25:j+1])) / 2
            tc, kc = tk(i), kj(i)
            tp, kp = tk(i-1), kj(i-1)
            if tc and kc and tp and kp:
                if tp <= kp and tc > kc: return "上抜け"
                if tp >= kp and tc < kc: return "下抜け"
            return ""

        def _ik3_buy(i):
            """一目三役好転: 転換>基準 かつ 終値>雲上限 かつ 遅行>26本前終値"""
            def tk(j):
                if j < 8: return None
                return (max(highs[j-8:j+1]) + min(lows[j-8:j+1])) / 2
            def kj(j):
                if j < 25: return None
                return (max(highs[j-25:j+1]) + min(lows[j-25:j+1])) / 2
            def sa(j):
                t, k = tk(j), kj(j)
                if t and k: return max(t, k)
                return None
            def sb(j):
                if j < 51: return None
                return (max(highs[j-51:j+1]) + min(lows[j-51:j+1])) / 2

            if i < 52: return False
            tc, kc = tk(i), kj(i)
            # 雲上限 = max(先行スパン1[26本前], 先行スパン2[26本前])
            sa26 = sa(i - 26)
            sb26 = sb(i - 26)
            if not (tc and kc and sa26 and sb26): return False
            kumo_top = max(sa26, sb26)
            lagging_price = closes[i - 26] if i >= 26 else None
            if not lagging_price: return False
            return tc > kc and closes[i] > kumo_top and closes[i] > lagging_price

        def _get_pivot_lows(i, lookback=5):
            """ピボットロー一覧（前後lookback本で最安値の点）"""
            result = []
            for j in range(lookback, i - lookback + 1):
                l = lows[j]
                is_low = all(lows[k] > l for k in range(j - lookback, j + lookback + 1) if k != j and 0 <= k < len(lows))
                if is_low:
                    result.append({'idx': j, 'price': l})
            return result

        def _get_pivot_highs(i, lookback=5):
            """ピボットハイ一覧（前後lookback本で最高値の点）"""
            result = []
            for j in range(lookback, i - lookback + 1):
                h = highs[j]
                is_high = all(highs[k] < h for k in range(j - lookback, j + lookback + 1) if k != j and 0 <= k < len(highs))
                if is_high:
                    result.append({'idx': j, 'price': h})
            return result

        def _resistance_val(i, lookback=5):
            """ピボットハイをクラスタリングした抵抗線の最近傍値（価格より上）"""
            if i < lookback * 2 + 1: return None
            price = closes[i]
            range_limit = price * 0.20
            raw = [p['price'] for p in _get_pivot_highs(i, lookback)
                   if price < p['price'] <= price + range_limit]
            if not raw: return None
            sorted_highs = sorted(raw)
            groups, idx = [], 0
            while idx < len(sorted_highs):
                group = [sorted_highs[idx]]
                while idx + 1 < len(sorted_highs) and abs(sorted_highs[idx+1] - sorted_highs[idx]) / sorted_highs[idx] < 0.012:
                    idx += 1
                    group.append(sorted_highs[idx])
                groups.append({'level': sum(group) / len(group), 'strength': len(group)})
                idx += 1
            groups.sort(key=lambda x: -x['strength'])
            above = [g['level'] for g in groups[:3] if g['level'] > price]
            return min(above) if above else None

        def _trendline_val(i, lookback=5):
            """グラフと同一ロジック: 直近の上昇ピボットロー2点を結ぶ線の現在値"""
            if i < lookback * 2 + 1: return None
            price = closes[i]
            pivot_lows = _get_pivot_lows(i, lookback)
            for ii in range(len(pivot_lows) - 1, 0, -1):
                p2 = pivot_lows[ii]
                for jj in range(ii - 1, max(-1, ii - 7), -1):
                    p1 = pivot_lows[jj]
                    if p2['price'] <= p1['price']: continue
                    slope = (p2['price'] - p1['price']) / (p2['idx'] - p1['idx'])
                    # 中間ピボットが線を下回っていないか検証
                    valid = all(
                        pivot_lows[kk]['price'] >= (p1['price'] + slope * (pivot_lows[kk]['idx'] - p1['idx'])) * 0.997
                        for kk in range(jj + 1, ii)
                    )
                    if not valid: continue
                    end_price = p2['price'] + slope * (i - p2['idx'])
                    if price >= end_price * 0.96:
                        return end_price
            return None

        def _trendline_near(i, thresh=0.02):
            """直近安値のトレンドライン近傍（価格がトレンドライン±thresh%以内）"""
            tl_val = _trendline_val(i)
            if tl_val is None: return False
            return abs(closes[i] - tl_val) / tl_val <= thresh

        def _trendline_below(i, thresh=0.02):
            """TLが現在価格のthresh%以内の下にある（支持として機能）"""
            tl = _trendline_val(i)
            if tl is None: return False
            price = closes[i]
            return tl <= price and (price - tl) / tl <= thresh

        def _trendline_above(i, thresh=0.02):
            """TLが現在価格のthresh%以内の上にある（抵抗として機能）"""
            tl = _trendline_val(i)
            if tl is None: return False
            price = closes[i]
            return tl > price and (tl - price) / tl <= thresh

        def _support_nearby(i, thresh=0.02):
            """支持線が現在価格のthresh%以内の下にある"""
            sv = _support_val(i)
            if sv is None: return False
            price = closes[i]
            return price * (1 - thresh) <= sv < price

        def _resistance_nearby(i, thresh=0.02):
            """抵抗線が現在価格のthresh%以内の上にある"""
            rv = _resistance_val(i)
            if rv is None: return False
            price = closes[i]
            return price < rv <= price * (1 + thresh)

        def _support_val(i, lookback=5):
            """グラフと同一ロジック: ピボットローをクラスタリングした支持線の最近傍値"""
            if i < lookback * 2 + 1: return None
            price = closes[i]
            range_limit = price * 0.20
            raw = [p['price'] for p in _get_pivot_lows(i, lookback)
                   if p['price'] <= price and p['price'] >= price - range_limit]
            if not raw: return None
            sorted_lows = sorted(raw, reverse=True)
            groups = []
            idx = 0
            while idx < len(sorted_lows):
                group = [sorted_lows[idx]]
                while idx + 1 < len(sorted_lows) and abs(sorted_lows[idx+1] - sorted_lows[idx]) / sorted_lows[idx] < 0.012:
                    idx += 1
                    group.append(sorted_lows[idx])
                groups.append({'level': sum(group) / len(group), 'strength': len(group)})
                idx += 1
            groups.sort(key=lambda x: -x['strength'])
            levels = [g['level'] for g in groups[:3]]
            below = [l for l in levels if l < price]
            return max(below) if below else None

        def _resistance_break(i, window=20):
            """直近window本の高値を上抜け"""
            if i < window: return False
            prev_high = max(highs[i-window:i])
            return closes[i] > prev_high

        def _support_bounce(i, window=20, thresh=0.015):
            """直近window本の安値±thresh%で反転上昇"""
            if i < window + 1: return False
            prev_low = min(lows[i-window:i])
            near = abs(lows[i] - prev_low) / prev_low <= thresh
            bounce = closes[i] > closes[i-1]
            return near and bounce

        def _bb_walk(i, n=3):
            """直近n本連続BB%≥80"""
            if i < n - 1 + 19: return False
            return all((_bb(j) or 0) >= 80 for j in range(i-n+1, i+1))

        def _bb_walk_down(i, n=3):
            """直近n本連続BB%≤20（バンドウォーク下落）"""
            if i < n - 1 + 19: return False
            return all((_bb(j) or 100) <= 20 for j in range(i-n+1, i+1))

        def _macd(i):
            """MACD(12,26,9) と シグナル線を返す (macd, signal) or (None, None)"""
            if i < 33: return None, None  # 26+9-1
            def ema(vals, period):
                k = 2 / (period + 1)
                e = vals[0]
                for v in vals[1:]:
                    e = v * k + e * (1 - k)
                return e
            # MACD線 = EMA12 - EMA26
            macd_vals = []
            for j in range(i - 8, i + 1):  # 9本分のMACD値を計算
                if j < 25: return None, None
                e12 = ema(closes[j-11:j+1], 12)
                e26 = ema(closes[j-25:j+1], 26)
                macd_vals.append(e12 - e26)
            sig = ema(macd_vals, 9)
            return macd_vals[-1], sig

        def _rsi(i, period=14):
            """RSI(period)を返す。データ不足時はNone"""
            if i < period: return None
            gains, losses = 0.0, 0.0
            for j in range(i - period + 1, i + 1):
                diff = closes[j] - closes[j - 1]
                if diff >= 0: gains += diff
                else: losses -= diff
            avg_gain = gains / period
            avg_loss = losses / period
            if avg_loss == 0: return 100.0
            return 100 - 100 / (1 + avg_gain / avg_loss)

        def _kumo(i):
            """現在足iの雲上限・下限を返す (None, None) if insufficient data"""
            if i < 52: return None, None
            def tk(j):
                if j < 8: return None
                return (max(highs[j-8:j+1]) + min(lows[j-8:j+1])) / 2
            def kj(j):
                if j < 25: return None
                return (max(highs[j-25:j+1]) + min(lows[j-25:j+1])) / 2
            def sb(j):
                if j < 51: return None
                return (max(highs[j-51:j+1]) + min(lows[j-51:j+1])) / 2
            t26 = tk(i - 26)
            k26 = kj(i - 26)
            s26 = sb(i - 26)
            if t26 is None or k26 is None or s26 is None: return None, None
            sa = (t26 + k26) / 2
            kumo_top = max(sa, s26)
            kumo_bot = min(sa, s26)
            return kumo_top, kumo_bot

        def stop_loss(i, price):
            """支持線・MA・BB下限・一目各線のうち価格直下で最も近い値を損切りラインとする"""
            candidates = []

            # 移動平均線 (5, 25, 75)
            for n in [5, 25, 75]:
                ma = _ma(i, n)
                if ma is not None and ma < price:
                    candidates.append(ma)

            # BBバンド下限（実価格）
            if i >= 19:
                c20 = closes[i-19:i+1]
                mean = sum(c20) / 20
                std = (sum((x - mean)**2 for x in c20) / 20) ** 0.5
                bb_lower = mean - 2 * std
                if bb_lower < price:
                    candidates.append(bb_lower)

            # 一目均衡表: 転換線・基準線
            def _tk(j):
                if j < 8: return None
                return (max(highs[j-8:j+1]) + min(lows[j-8:j+1])) / 2
            def _kj(j):
                if j < 25: return None
                return (max(highs[j-25:j+1]) + min(lows[j-25:j+1])) / 2
            tenkan = _tk(i)
            kijun = _kj(i)
            for v in [tenkan, kijun]:
                if v is not None and v < price:
                    candidates.append(v)

            # 雲（上限・下限）
            kumo_top, kumo_bot = _kumo(i)
            for v in [kumo_top, kumo_bot]:
                if v is not None and v < price:
                    candidates.append(v)

            # トレンドライン
            tl = _trendline_val(i)
            if tl is not None and tl < price:
                candidates.append(tl)

            # 支持線（直近20本安値）
            sup = _support_val(i)
            if sup is not None and sup < price:
                candidates.append(sup)

            if candidates:
                sl = max(candidates)        # 価格直下で最も近い値
                sl = max(sl, price * 0.85)  # 最大15%下まで
                return round(sl, 1)

            # フォールバック: 直近20本の安値-2%
            start = max(0, i - 19)
            recent_low = min(lows[start:i+1])
            return round(max(recent_low * 0.98, price * 0.85), 1)

        def _slope(arr):
            mn = len(arr)
            if mn < 2: return 0.0
            mean_x = (mn - 1) / 2
            mean_y = sum(arr) / mn
            num = sum((j - mean_x) * (arr[j] - mean_y) for j in range(mn))
            den = sum((j - mean_x) ** 2 for j in range(mn))
            return (num / den) / (mean_y or 1) if den else 0.0

        # パターンシグナル集合（2pt）
        _pattern_buy  = {"急騰+4%", "ハンマー", "逆ハンマー", "陽の包み足", "三白兵",
                         "ダブルボトム", "逆H&S", "上昇フラッグ", "レクタングル上抜け",
                         "上昇三角形", "対称三角形↑", "下降ウェッジ", "C&H"}
        _pattern_sell = {"急落-4%", "BBウォーク↓", "雲下抜け",
                         "トンカチ", "陰の包み足", "三羽烏",
                         "ダブルトップ", "H&S", "下降フラッグ", "レクタングル下抜け",
                         "下降三角形", "対称三角形↓", "上昇ウェッジ"}

        # 最低スコア閾値: 3pt以上で買いシグナル表示
        BUY_THRESHOLD = 2
        signals = []
        scores = {}       # date -> {buy: pt, sell: pt}
        buy_reset  = True  # 0ptを経由したらTrue（初回は許可）
        sell_reset = True

        for i in range(1, len(candles)):
            date_key = get_date(candles[i])
            price    = closes[i]
            bb_c     = _bb(i)
            bb_p     = _bb(i - 1)
            ik       = _ik_cross(i)
            ma5_c    = _ma(i, 5);   ma5_p  = _ma(i-1, 5)
            ma25_c   = _ma(i, 25);  ma25_p = _ma(i-1, 25)
            vix      = get_vix(date_key)

            # ---- 買いポイント集計（各+1pt）----
            buy_tags = []

            # 雲の下かつ下降トレンド中の判定（MA5 < MA25）
            kumo_top_i, kumo_bot_i = _kumo(i)
            below_kumo = kumo_bot_i is not None and closes[i] < kumo_bot_i
            downtrend = ma5_c is not None and ma25_c is not None and ma5_c < ma25_c

            if _trendline_near(i):
                buy_tags.append("TL")
            if _support_nearby(i):
                buy_tags.append("支持線近傍")
            if _trendline_below(i):
                buy_tags.append("TL下支持")
            if ma5_c and ma25_c and ma5_p and ma25_p and ma5_p <= ma25_p and ma5_c > ma25_c:
                buy_tags.append("GC")
            if _ik3_buy(i):
                buy_tags.append("IK3")
            if bb_c is not None and bb_c <= 30 and closes[i] > closes[i-1]:
                if not (below_kumo and downtrend):
                    buy_tags.append("BB反転")
            if _bb_walk(i):
                buy_tags.append("BBウォーク")
            if _resistance_break(i):
                buy_tags.append("抵抗ブレイク")
            if _support_bounce(i):
                if not (below_kumo and downtrend):
                    buy_tags.append("支持反転")
            if ik == "上抜け":
                buy_tags.append("IK↑")
            # 急騰: 前日比+2%以上で1pt、+4%以上で2pt
            if i >= 1 and closes[i-1] > 0:
                chg = (closes[i] - closes[i-1]) / closes[i-1]
                if chg >= 0.04:
                    buy_tags.append("急騰+4%")
                    buy_tags.append("急騰+4%_2pt")  # 2pt分
                elif chg >= 0.02:
                    buy_tags.append("急騰+2%")
            # MACDの上昇交差
            if i >= 1:
                mc, ms = _macd(i)
                mcp, msp = _macd(i - 1)
                if mc and ms and mcp and msp and mcp <= msp and mc > ms:
                    buy_tags.append("MACD↑")
            # VIX≤17: 安定水準
            if vix is not None and vix <= 17:
                buy_tags.append("VIX低")
            # RSI≤40: 売られすぎ水準
            rsi_val = _rsi(i)
            if rsi_val is not None and rsi_val <= 40:
                buy_tags.append("RSI低")

            # ---- 売り条件（sell_tags: 表示用、sell_pts: スコア合計）----
            sell_tags = []
            sell_pts  = 0
            def add_sell(tag, pts=1):
                sell_tags.append(tag)
                nonlocal sell_pts
                sell_pts += pts

            if ma5_c and ma25_c and ma5_p and ma25_p and ma5_p >= ma25_p and ma5_c < ma25_c:
                add_sell("DC")
            if ik == "下抜け":
                add_sell("IK↓")
            if i >= 20:
                resistance = max(highs[i-20:i])
                if closes[i] >= resistance * 0.98 and closes[i] < resistance:
                    add_sell("抵抗手前")
            if i >= 20:
                support = min(lows[i-20:i])
                if closes[i] < support and closes[i-1] >= support:
                    add_sell("支持下抜け")
            if vix is not None and vix >= 20:
                add_sell("VIX高")
            # RSI≥70: 買われすぎ水準
            if rsi_val is not None and rsi_val >= 70:
                add_sell("RSI高")
            # 急落: 前日比-2%以下で1pt、-4%以下で2pt
            if i >= 1 and closes[i-1] > 0:
                chg = (closes[i] - closes[i-1]) / closes[i-1]
                if chg <= -0.04:
                    add_sell("急落-4%")
                    add_sell("急落-4%_2pt")  # 2pt分
                elif chg <= -0.02:
                    add_sell("急落-2%")
            # MACDの下降交差
            if i >= 1:
                mc, ms = _macd(i)
                mcp, msp = _macd(i - 1)
                if mc and ms and mcp and msp and mcp >= msp and mc < ms:
                    add_sell("MACD↓")

            # BBバンドウォーク下落: BB%≤20を3本以上継続
            if _bb_walk_down(i):
                add_sell("BBウォーク↓", 2)

            # 抵抗線・TLが現在価格の2%以内の上にある
            if _resistance_nearby(i):
                add_sell("抵抗線近傍")
            if _trendline_above(i):
                add_sell("TL上抵抗")

            # 一目雲: 雲に下向きに入った(-1pt) / 雲を下抜け(-2pt)
            if i >= 1:
                kt_c, kb_c = _kumo(i)
                kt_p, kb_p = _kumo(i - 1)
                if kt_c and kb_c and kt_p and kb_p:
                    was_above  = closes[i-1] > kt_p            # 前日: 雲の上
                    was_in     = kb_p <= closes[i-1] <= kt_p   # 前日: 雲の中
                    now_in     = kb_c <= closes[i]  <= kt_c    # 今日: 雲の中
                    now_below  = closes[i] < kb_c              # 今日: 雲の下
                    if was_above and now_in:
                        add_sell("雲侵入", 1)
                    elif (was_above or was_in) and now_below:
                        add_sell("雲下抜け", 2)

            # ===== チャートパターン検出 =====
            # ローソク足パターン（直近3本）
            if i >= 2:
                co, ch, cl, cc = float(candles[i]['open']), highs[i], lows[i], closes[i]
                po, ph, pl, pc = float(candles[i-1]['open']), highs[i-1], lows[i-1], closes[i-1]
                p2o, p2c = float(candles[i-2]['open']), closes[i-2]
                body = abs(cc - co); rng = ch - cl
                if rng > 0:
                    lw = min(co, cc) - cl; uw = ch - max(co, cc)
                    ma5_now = _ma(i, 5); ma5_prev = _ma(max(0, i-5), 5)
                    up_trend = ma5_now and ma5_prev and ma5_now > ma5_prev
                    dn_trend = ma5_now and ma5_prev and ma5_now < ma5_prev
                    if dn_trend and lw >= 0.55*rng and uw <= 0.15*rng and body <= 0.3*rng:
                        buy_tags.append("ハンマー")
                    if up_trend and lw >= 0.55*rng and uw <= 0.15*rng and body <= 0.3*rng:
                        add_sell("トンカチ", 2)
                    if dn_trend and uw >= 0.55*rng and lw <= 0.15*rng and body <= 0.3*rng:
                        buy_tags.append("逆ハンマー")
                if pc < po and cc > co and co <= pc and cc >= po:
                    buy_tags.append("陽の包み足")
                if pc > po and cc < co and co >= pc and cc <= po:
                    add_sell("陰の包み足", 2)
                if (p2c > p2o and pc > po and cc > co and pc > p2c and cc > pc and po >= p2o and co >= po):
                    buy_tags.append("三白兵")
                if (p2c < p2o and pc < po and cc < co and pc < p2c and cc < pc and po <= p2o and co <= po):
                    add_sell("三羽烏", 2)

            # ダブルトップ / ダブルボトム
            if i >= 15:
                lb = min(40, i); seg_h = highs[i-lb:i+1]; seg_l = lows[i-lb:i+1]
                sn = len(seg_h); lhighs = []; llows = []
                for j in range(2, sn-2):
                    if seg_h[j] > seg_h[j-1] and seg_h[j] > seg_h[j-2] and seg_h[j] > seg_h[j+1] and seg_h[j] > seg_h[j+2]:
                        lhighs.append((j, seg_h[j]))
                    if seg_l[j] < seg_l[j-1] and seg_l[j] < seg_l[j-2] and seg_l[j] < seg_l[j+1] and seg_l[j] < seg_l[j+2]:
                        llows.append((j, seg_l[j]))
                if len(lhighs) >= 2:
                    (j1,h1),(j2,h2) = lhighs[-2], lhighs[-1]
                    if j2-j1 >= 5 and abs(h1-h2)/h1 < 0.03 and sn-1 > j2:
                        neck = min(seg_l[j1:j2+1])
                        if closes[i] < neck:
                            add_sell("ダブルトップ", 2)
                if len(llows) >= 2:
                    (j1,l1),(j2,l2) = llows[-2], llows[-1]
                    if j2-j1 >= 5 and abs(l1-l2)/l1 < 0.03 and sn-1 > j2:
                        neck = max(seg_h[j1:j2+1])
                        if closes[i] > neck:
                            buy_tags.append("ダブルボトム")

            # ヘッド&ショルダー / 逆H&S
            if i >= 20:
                lb = min(60, i); seg_h = highs[i-lb:i+1]; seg_l = lows[i-lb:i+1]; sn = len(seg_h)
                pks = []; trs = []
                for j in range(3, sn-3):
                    if seg_h[j] == max(seg_h[j-3:j+4]): pks.append((j, seg_h[j]))
                    if seg_l[j] == min(seg_l[j-3:j+4]): trs.append((j, seg_l[j]))
                if len(pks) >= 3:
                    (lsi,lsv),(hdi,hdv),(rsi2,rsv) = pks[-3], pks[-2], pks[-1]
                    if hdv > lsv*1.02 and hdv > rsv*1.02 and abs(lsv-rsv)/lsv < 0.05 and rsi2-lsi >= 10:
                        n1 = min(seg_l[lsi:hdi+1]); n2 = min(seg_l[hdi:rsi2+1])
                        if closes[i] < (n1+n2)/2:
                            add_sell("H&S", 2)
                if len(trs) >= 3:
                    (lsi,lsv),(hdi,hdv),(rsi2,rsv) = trs[-3], trs[-2], trs[-1]
                    if hdv < lsv*0.98 and hdv < rsv*0.98 and abs(lsv-rsv)/lsv < 0.05 and rsi2-lsi >= 10:
                        n1 = max(seg_h[lsi:hdi+1]); n2 = max(seg_h[hdi:rsi2+1])
                        if closes[i] > (n1+n2)/2:
                            buy_tags.append("逆H&S")

            # フラッグ
            if i >= 14:
                p_len = 8; f_len = min(6, i - p_len)
                if f_len >= 3:
                    ps2 = i - p_len - f_len; pe = i - f_len
                    if ps2 >= 0:
                        pm = (closes[pe] - closes[ps2]) / closes[ps2] if closes[ps2] else 0
                        fbars_h = highs[pe:i+1]; fbars_l = lows[pe:i+1]; fbars_c = closes[pe:i+1]
                        fh = max(fbars_h); fl = min(fbars_l)
                        fsl = (fbars_c[-1] - fbars_c[0]) / fbars_c[0] if fbars_c[0] else 0
                        fr = (fh - fl) / fl if fl else 0
                        if pm > 0.04 and fr < pm*0.6 and fsl < 0 and fsl > -0.04 and closes[i] > fh:
                            buy_tags.append("上昇フラッグ")
                        if pm < -0.04 and fr < abs(pm)*0.6 and fsl > 0 and fsl < 0.04 and closes[i] < fl:
                            add_sell("下降フラッグ", 2)

            # レクタングル / 三角形 / ウェッジ
            if i >= 20:
                c_len = min(25, i-1); seg_h2 = highs[i-c_len:i]; seg_l2 = lows[i-c_len:i]
                mx_h = max(seg_h2); mn_l = min(seg_l2)
                rng2 = (mx_h - mn_l) / mn_l if mn_l else 0
                if rng2 < 0.07:
                    if closes[i] > mx_h * 1.003: buy_tags.append("レクタングル上抜け")
                    elif closes[i] < mn_l * 0.997: add_sell("レクタングル下抜け", 2)
                rs2 = seg_h2[-12:]; ls2_arr = seg_l2[-12:]
                if len(rs2) >= 8:
                    hs2 = _slope(list(rs2)); ls2_v = _slope(list(ls2_arr))
                    if abs(hs2) < 0.003 and ls2_v > 0.003 and closes[i] > mx_h:
                        buy_tags.append("上昇三角形")
                    if hs2 < -0.003 and abs(ls2_v) < 0.003 and closes[i] < mn_l:
                        add_sell("下降三角形", 2)
                    if hs2 < -0.002 and ls2_v > 0.002:
                        if closes[i] > mx_h: buy_tags.append("対称三角形↑")
                        if closes[i] < mn_l: add_sell("対称三角形↓", 2)
                    if hs2 > 0.001 and ls2_v > hs2*1.3 and closes[i] < mn_l:
                        add_sell("上昇ウェッジ", 2)
                    if hs2 < -0.001 and ls2_v < 0 and hs2 < ls2_v*1.3 and closes[i] > mx_h:
                        buy_tags.append("下降ウェッジ")

            # C&H（カップ&ハンドル）
            if i >= 30:
                c_len2 = min(35, i-5)
                if c_len2 >= 15:
                    cup = candles[i-c_len2:i-3]
                    hdl_l = lows[i-3:i+1]; hdl_c = closes[i-3:i+1]
                    cup_c = [float(r['close']) for r in cup]; cup_l = [float(r['low']) for r in cup]
                    if len(cup_c) >= 10:
                        cleft = sum(cup_c[:5])/5; cright = sum(cup_c[-5:])/5
                        cbotm = min(cup_l); base = min(cleft, cright)
                        depth = (base - cbotm) / base if base else 0
                        if 0.05 < depth < 0.4 and abs(cleft-cright)/cleft < 0.05:
                            res = max(cleft, cright)
                            if min(hdl_l) > cbotm and closes[i] > res:
                                buy_tags.append("C&H")

            # 全日付のポイントを記録（パターンは2pt）
            buy_pt = sum(2 if t in _pattern_buy else 1 for t in buy_tags)
            scores[date_key] = {"buy": buy_pt, "sell": sell_pts}

            # 買いシグナル: 買いpt - 売りpt の合計が BUY_THRESHOLD 以上
            net_buy_pt = buy_pt - sell_pts
            if net_buy_pt <= 0:
                buy_reset = True
            if net_buy_pt >= BUY_THRESHOLD and buy_reset:
                score_str = f"[net+{net_buy_pt}pt]"
                sl = stop_loss(i, price)
                signals.append({
                    "time":         date_key,
                    "side":         "buy",
                    "price":        round(price, 1),
                    "reason":       score_str + "・".join(buy_tags),
                    "stop_loss":    sl,
                    "stop_loss_pct": round((price - sl) / price * 100, 1) if sl and price > 0 else None,
                })
                buy_reset = False

            # 売りシグナル: 売りpt - 買いpt の合計が 2 以上
            net_sell_pt = sell_pts - buy_pt
            if net_sell_pt <= 0:
                sell_reset = True
            if net_sell_pt >= 2 and sell_reset:
                score_str = f"[net-{net_sell_pt}pt]"
                signals.append({
                    "time":      date_key,
                    "side":      "sell",
                    "price":     round(price, 1),
                    "reason":    score_str + "・".join(sell_tags),
                    "stop_loss": None,
                })
                sell_reset = False
                buy_reset = True  # 売りシグナル後は買いシグナルを許可

        return {"signals": signals, "scores": scores}
    except Exception:
        return {"signals": [], "scores": {}}


@app.post("/analyze")
def analyze_trades(body: dict = None):
    from datetime import datetime as dt
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT trade_date, code, name, side, qty, price, settlement FROM trades ORDER BY trade_date")
            rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        return {"analysis": "トレードデータがありません。CSVをインポートしてください。"}

    req = body or {}
    signal_symbol   = req.get("symbol", "")
    signal_interval = req.get("interval", "1d")

    # 分析前に全トレード銘柄の日足ローソク足をyfinanceから取得してDBに保存
    # ただし直近2営業日以内のデータがすでにDBにあればスキップ
    from datetime import datetime as dt2, timedelta
    today = dt2.utcnow().date()
    recent_threshold = (today - timedelta(days=3)).isoformat()  # 土日考慮で3日

    all_codes = list({str(r['code']) for r in rows})
    for code in all_codes:
        try:
            c0 = get_conn()
            try:
                with c0.cursor() as cur:
                    cur.execute(
                        "SELECT MAX(candle_time) as latest FROM candles WHERE symbol=%s AND interval_type='1d'",
                        (code,)
                    )
                    row0 = cur.fetchone()
                    latest = row0['latest'] if row0 else None
            finally:
                c0.close()

            # 最新データが3日以内なら取得不要
            if latest and str(latest) >= recent_threshold:
                continue

            sym = f"{code}.T" if (code.isdigit() or (len(code) == 4 and code.isalnum())) else code
            fresh = fetch_from_yfinance(sym, "1d")
            if fresh:
                c0 = get_conn()
                try:
                    save_candles_to_db(c0, code, "1d", fresh)
                finally:
                    c0.close()
        except Exception:
            pass

    conn = get_conn()
    try:
        by_code = defaultdict(list)
        for r in rows:
            by_code[r['code']].append(r)

        winners = []
        losers  = []
        holding = []

        for code, ts in by_code.items():
            name = ts[0]['name']
            buy_trades  = [t for t in ts if t['side'] == 'buy']
            sell_trades = [t for t in ts if t['side'] == 'sell']
            buy_qty  = sum(t['qty'] for t in buy_trades)
            sell_qty = sum(t['qty'] for t in sell_trades)
            buy_settl  = sum(t['settlement'] for t in buy_trades)
            sell_settl = sum(t['settlement'] for t in sell_trades)

            if sell_qty == 0:
                holding.append(name)
                continue

            cost_basis = buy_settl * (sell_qty / buy_qty) if buy_qty > 0 else 0
            pnl = sell_settl - cost_basis
            avg_buy_price  = sum(t['price']*t['qty'] for t in buy_trades)  / buy_qty  if buy_qty  else 0
            avg_sell_price = sum(t['price']*t['qty'] for t in sell_trades) / sell_qty if sell_qty else 0

            buy_dates  = sorted(str(t['trade_date'])[:10] for t in buy_trades)
            sell_dates = sorted(str(t['trade_date'])[:10] for t in sell_trades)

            try:
                hold_days = (dt.strptime(sell_dates[-1], '%Y-%m-%d') - dt.strptime(buy_dates[0], '%Y-%m-%d')).days
            except Exception:
                hold_days = None

            buy_ctx  = get_candle_context(conn, code, buy_dates[0])
            sell_ctx = get_candle_context(conn, code, sell_dates[-1])
            buy_desc  = describe_context(buy_ctx,  'buy')
            sell_desc = describe_context(sell_ctx, 'sell')

            entry = {
                "name": name, "code": code,
                "pnl": round(pnl), "pnl_pct": round(pnl/cost_basis*100, 1) if cost_basis else 0,
                "avg_buy_price": round(avg_buy_price, 1),
                "avg_sell_price": round(avg_sell_price, 1),
                "buy_count": len(buy_trades), "sell_count": len(sell_trades),
                "hold_days": hold_days,
                "first_buy": buy_dates[0], "last_sell": sell_dates[-1],
                "buy_desc": buy_desc, "sell_desc": sell_desc,
            }
            if pnl >= 0:
                winners.append(entry)
            else:
                losers.append(entry)

        winners.sort(key=lambda x: x['pnl'], reverse=True)
        losers.sort(key=lambda x: x['pnl'])

        # ---- ルールベース分析テキスト生成 ----
        total_pnl   = sum(e['pnl'] for e in winners) + sum(e['pnl'] for e in losers)
        win_rate    = len(winners) / (len(winners) + len(losers)) * 100 if (winners or losers) else 0
        avg_win_pnl = sum(e['pnl'] for e in winners) / len(winners) if winners else 0
        avg_los_pnl = sum(e['pnl'] for e in losers)  / len(losers)  if losers  else 0

        def fmt_entry(e):
            hold = f"保有{e['hold_days']}日" if e['hold_days'] is not None else ""
            buy_info  = f" [買い時: {e['buy_desc']}]"  if e['buy_desc']  else ""
            sell_info = f" [売り時: {e['sell_desc']}]" if e['sell_desc'] else ""
            return (
                f"- {e['name']}({e['code']}): 損益{e['pnl']:+,}円({e['pnl_pct']:+.1f}%) "
                f"買均{e['avg_buy_price']:,.0f}円→売均{e['avg_sell_price']:,.0f}円 {hold}"
                f"{buy_info}{sell_info}"
            )

        win_lines = "\n".join(fmt_entry(e) for e in winners) if winners else "なし"
        los_lines = "\n".join(fmt_entry(e) for e in losers)  if losers  else "なし"
        holding_lines = ("**保有中（未決済）**\n" + "\n".join(f"- {n}" for n in holding) + "\n\n") if holding else ""
        legend = "**シグナル凡例:** GC=ゴールデンクロス(MA5↑MA25) / DC=デッドクロス(MA5↓MA25) / BB↓=BB下限(BB%≤30) / BB↑=BB上限(BB%≥70) / IK↑=一目転換線上抜け / IK↓=一目転換線下抜け"

        analysis_text = (
            f"### 1. サマリー\n"
            f"- 総損益: {total_pnl:+,}円\u3000勝率: {win_rate:.0f}%（{len(winners)}勝{len(losers)}敗）\n"
            f"- 平均利益: {avg_win_pnl:+,.0f}円 / 平均損失: {avg_los_pnl:+,.0f}円\n\n"
            f"### 2. 利益トレード（{len(winners)}件）\n"
            f"{win_lines}\n\n"
            f"### 3. 損失トレード（{len(losers)}件）\n"
            f"{los_lines}\n\n"
            f"{holding_lines}"
            f"{legend}"
        )

        # シグナル用ローソク足データ取得（最新でなければ更新）
        if signal_symbol and signal_interval in INTERVALS:
            try:
                c2 = get_conn()
                try:
                    with c2.cursor() as cur:
                        cur.execute(
                            "SELECT MAX(candle_time) as latest FROM candles WHERE symbol=%s AND interval_type=%s",
                            (signal_symbol, signal_interval)
                        )
                        row2 = cur.fetchone()
                        latest2 = row2['latest'] if row2 else None
                finally:
                    c2.close()
                needs_fetch = not latest2 or str(latest2) < recent_threshold
                if needs_fetch:
                    sym2 = f"{signal_symbol}.T" if (signal_symbol.isdigit() or (len(signal_symbol) == 4 and signal_symbol.isalnum())) else signal_symbol
                    fresh = fetch_from_yfinance(sym2, signal_interval)
                    if fresh:
                        c2 = get_conn()
                        try:
                            save_candles_to_db(c2, signal_symbol, signal_interval, fresh)
                        finally:
                            c2.close()
            except Exception:
                pass

    finally:
        conn.close()

    # ルールベースシグナル生成
    result = generate_rule_signals(signal_symbol, signal_interval) if signal_symbol else {"signals": [], "scores": {}}
    signals = result["signals"]

    return {"analysis": analysis_text, "signals": signals}



@app.post("/history/analysis")
def save_analysis(data: dict):
    symbol  = data.get("symbol", "")
    label   = data.get("label", symbol)
    content = data.get("content", "")
    if not symbol:
        raise HTTPException(400, "No symbol")
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO analysis_history (label, analysis_text) VALUES (%s, %s)",
                (label, content)
            )
        conn.commit()
        with conn.cursor() as cur:
            cur.execute("SELECT LAST_INSERT_ID() as id")
            row = cur.fetchone()
            return {"analysis_id": row['id'] if row else None}
    finally:
        conn.close()


@app.delete("/history/analysis")
def delete_all_analysis():
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM signal_history")
            cur.execute("DELETE FROM analysis_history")
        conn.commit()
        return {"deleted": True}
    finally:
        conn.close()


@app.get("/history/analysis")
def get_analysis_history():
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, COALESCE(label, LEFT(analysis_text, 100)) as label, created_at "
                "FROM analysis_history ORDER BY created_at DESC LIMIT 20"
            )
            rows = cur.fetchall()
        return [{"id": r["id"], "symbol": r["label"], "created_at": str(r["created_at"])} for r in rows]
    finally:
        conn.close()


@app.get("/history/analysis/{analysis_id}")
def get_analysis_detail(analysis_id: int):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id, analysis_text, created_at FROM analysis_history WHERE id=%s", (analysis_id,))
            row = cur.fetchone()
        if not row:
            raise HTTPException(404, "Not found")
        return {"id": row["id"], "analysis": row["analysis_text"], "created_at": str(row["created_at"])}
    finally:
        conn.close()


@app.delete("/history/analysis/{analysis_id}")
def delete_analysis(analysis_id: int):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM signal_history WHERE analysis_id=%s", (analysis_id,))
            cur.execute("DELETE FROM analysis_history WHERE id=%s", (analysis_id,))
        conn.commit()
        return {"deleted": analysis_id}
    finally:
        conn.close()


@app.post("/history/signals")
def save_signals(data: dict):
    analysis_id = data.get("analysis_id")
    symbol      = data.get("symbol")
    interval    = data.get("interval")
    signals     = data.get("signals", [])
    if not signals:
        return {"saved": 0}
    conn = get_conn()
    saved = 0
    try:
        with conn.cursor() as cur:
            for s in signals:
                cur.execute(
                    "INSERT INTO signal_history (analysis_id, symbol, interval_type, candle_time, side, price, reason, stop_loss) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                    (analysis_id, symbol, interval, str(s["time"]), s["side"], s["price"],
                     s.get("reason", ""), s.get("stop_loss"))
                )
                saved += 1
        conn.commit()
        return {"saved": saved}
    finally:
        conn.close()


@app.get("/history/signals/{analysis_id}")
def get_signals(analysis_id: int, symbol: str = Query(...), interval: str = Query("1d")):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT candle_time, side, price, reason, stop_loss FROM signal_history "
                "WHERE analysis_id=%s AND symbol=%s AND interval_type=%s ORDER BY candle_time",
                (analysis_id, symbol, interval)
            )
            rows = cur.fetchall()
        cfg = INTERVALS.get(interval, {"is_date": True})
        return [{
            "time":      r["candle_time"] if cfg["is_date"] else int(r["candle_time"]),
            "side":      r["side"],
            "price":     r["price"],
            "reason":    r["reason"],
            "stop_loss": r["stop_loss"],
        } for r in rows]
    finally:
        conn.close()


@app.get("/signals")
def get_signals(symbol: str = Query(...), interval: str = Query("1d")):
    """ルールベースシグナルを返す（銘柄表示時に自動呼び出し）"""
    # ローソク足データを最新化
    try:
        from datetime import datetime as _dt2, timedelta as _td
        recent_threshold = (_dt2.utcnow().date() - _td(days=3)).isoformat()
        c0 = get_conn()
        try:
            with c0.cursor() as cur:
                cur.execute(
                    "SELECT MAX(candle_time) as latest FROM candles WHERE symbol=%s AND interval_type=%s",
                    (symbol, interval)
                )
                row0 = c0.cursor().fetchone() if False else cur.fetchone()
                latest0 = row0['latest'] if row0 else None
        finally:
            c0.close()
        if not latest0 or str(latest0) < recent_threshold:
            sym = f"{symbol}.T" if (symbol.isdigit() or (len(symbol) == 4 and symbol.isalnum())) else symbol
            fresh = fetch_from_yfinance(sym, interval)
            if fresh:
                c0 = get_conn()
                try:
                    save_candles_to_db(c0, symbol, interval, fresh)
                finally:
                    c0.close()
    except Exception:
        pass
    result = generate_rule_signals(symbol, interval)
    return result


@app.get("/suggest")
def suggest_signals(symbol: str = Query(...), interval: str = Query("1d")):
    """ローソク足データから技術指標でAI推奨買い/売りシグナルを生成"""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT candle_time, open, high, low, close, volume FROM candles "
                "WHERE symbol=%s AND interval_type=%s ORDER BY candle_time",
                (symbol, interval)
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    if len(rows) < 30:
        return []

    cfg = INTERVALS.get(interval, {"is_date": True})
    closes  = [r['close']  for r in rows]
    highs   = [r['high']   for r in rows]
    lows    = [r['low']    for r in rows]
    volumes = [r['volume'] for r in rows]
    times   = [r['candle_time'] for r in rows]

    def ma(arr, n, i):
        if i < n - 1: return None
        return sum(arr[i-n+1:i+1]) / n

    def bb(arr, n, i):
        if i < n - 1: return None, None
        c = arr[i-n+1:i+1]
        mean = sum(c) / n
        std  = (sum((x - mean)**2 for x in c) / n) ** 0.5
        return mean + 2*std, mean - 2*std

    def ichimoku_tenkan(i):
        n = 9
        if i < n - 1: return None
        return (max(highs[i-n+1:i+1]) + min(lows[i-n+1:i+1])) / 2

    def ichimoku_kijun(i):
        n = 26
        if i < n - 1: return None
        return (max(highs[i-n+1:i+1]) + min(lows[i-n+1:i+1])) / 2

    signals = []
    in_position = False

    for i in range(30, len(rows)):
        t = times[i]
        price = closes[i]

        ma5_cur  = ma(closes, 5,  i)
        ma5_prev = ma(closes, 5,  i-1)
        ma25_cur = ma(closes, 25, i)
        ma25_prev= ma(closes, 25, i-1)
        bb_up, bb_lo = bb(closes, 20, i)
        tenkan = ichimoku_tenkan(i)
        kijun  = ichimoku_kijun(i)
        vol_avg5  = sum(volumes[i-4:i+1]) / 5
        vol_avg25 = sum(volumes[i-24:i+1]) / 25 if i >= 24 else None

        reasons_buy  = []
        reasons_sell = []

        # ゴールデンクロス（MA5がMA25を上抜け）
        if ma5_cur and ma25_cur and ma5_prev and ma25_prev:
            if ma5_prev <= ma25_prev and ma5_cur > ma25_cur:
                reasons_buy.append("MA5がMA25をゴールデンクロス")
            if ma5_prev >= ma25_prev and ma5_cur < ma25_cur:
                reasons_sell.append("MA5がMA25をデッドクロス")

        # BBバンド下限タッチ + 出来高増加
        if bb_lo and price <= bb_lo * 1.01:
            vol_ok = vol_avg25 and vol_avg5 > vol_avg25 * 1.2
            reasons_buy.append("BB下限タッチ" + ("・出来高増加" if vol_ok else ""))

        # BBバンド上限タッチ
        if bb_up and price >= bb_up * 0.99:
            reasons_sell.append("BB上限タッチ")

        # 一目: 転換線が基準線を上抜け
        if tenkan and kijun:
            t_prev = ichimoku_tenkan(i-1)
            k_prev = ichimoku_kijun(i-1)
            if t_prev and k_prev:
                if t_prev <= k_prev and tenkan > kijun:
                    reasons_buy.append("一目:転換線が基準線を上抜け")
                if t_prev >= k_prev and tenkan < kijun:
                    reasons_sell.append("一目:転換線が基準線を下抜け")

        # シグナル発行（同じ方向の複数シグナルが重なった時だけ）
        if not in_position and len(reasons_buy) >= 1:
            time_val = t if cfg["is_date"] else int(t)
            signals.append({
                "time":   time_val,
                "side":   "buy",
                "price":  round(price, 1),
                "reason": "・".join(reasons_buy[:2]),
            })
            in_position = True

        elif in_position and len(reasons_sell) >= 1:
            time_val = t if cfg["is_date"] else int(t)
            signals.append({
                "time":   time_val,
                "side":   "sell",
                "price":  round(price, 1),
                "reason": "・".join(reasons_sell[:2]),
            })
            in_position = False

    return signals


# ===== BOJ MEETING DATES =====
BOJ_MEETINGS = [
    # 2025
    {"date": "2025-01-24", "result": "政策金利0.25%→0.5%に引き上げ"},
    {"date": "2025-03-19", "result": "政策金利0.5%据え置き"},
    {"date": "2025-05-01", "result": "政策金利0.5%据え置き"},
    {"date": "2025-06-17", "result": None},
    {"date": "2025-07-31", "result": None},
    {"date": "2025-09-19", "result": None},
    {"date": "2025-10-29", "result": None},
    {"date": "2025-12-19", "result": None},
    # 2026
    {"date": "2026-01-24", "result": None},
    {"date": "2026-03-19", "result": None},
    {"date": "2026-05-01", "result": None},
    {"date": "2026-06-17", "result": None},
    {"date": "2026-07-31", "result": None},
    {"date": "2026-09-18", "result": None},
    {"date": "2026-10-29", "result": None},
    {"date": "2026-12-18", "result": None},
]

# ===== BOJ TANKAN DATES =====
BOJ_TANKAN = [
    # 2025
    {"date": "2025-04-01", "detail": "2025年3月調査", "result": "大企業製造業DI +12"},
    {"date": "2025-07-01", "detail": "2025年6月調査", "result": None},
    {"date": "2025-10-01", "detail": "2025年9月調査", "result": None},
    {"date": "2026-01-14", "detail": "2025年12月調査", "result": None},
    # 2026
    {"date": "2026-04-01", "detail": "2026年3月調査", "result": None},
    {"date": "2026-07-01", "detail": "2026年6月調査", "result": None},
    {"date": "2026-10-01", "detail": "2026年9月調査", "result": None},
]

# ===== US ECONOMIC EVENTS =====
US_ECON_EVENTS = [
    # FOMC 2025
    {"date": "2025-01-29", "title": "FOMC（米連邦公開市場委員会）", "detail": "米国の金融政策決定会合", "result": "政策金利据え置き4.25-4.5%"},
    {"date": "2025-03-19", "title": "FOMC（米連邦公開市場委員会）", "detail": "米国の金融政策決定会合", "result": "政策金利据え置き4.25-4.5%"},
    {"date": "2025-05-07", "title": "FOMC（米連邦公開市場委員会）", "detail": "米国の金融政策決定会合", "result": "政策金利据え置き4.25-4.5%"},
    {"date": "2025-06-18", "title": "FOMC（米連邦公開市場委員会）", "detail": "米国の金融政策決定会合", "result": None},
    {"date": "2025-07-30", "title": "FOMC（米連邦公開市場委員会）", "detail": "米国の金融政策決定会合", "result": None},
    {"date": "2025-09-17", "title": "FOMC（米連邦公開市場委員会）", "detail": "米国の金融政策決定会合", "result": None},
    {"date": "2025-10-29", "title": "FOMC（米連邦公開市場委員会）", "detail": "米国の金融政策決定会合", "result": None},
    {"date": "2025-12-10", "title": "FOMC（米連邦公開市場委員会）", "detail": "米国の金融政策決定会合", "result": None},
    # FOMC 2026
    {"date": "2026-01-28", "title": "FOMC（米連邦公開市場委員会）", "detail": "米国の金融政策決定会合", "result": None},
    {"date": "2026-03-18", "title": "FOMC（米連邦公開市場委員会）", "detail": "米国の金融政策決定会合", "result": None},
    {"date": "2026-04-29", "title": "FOMC（米連邦公開市場委員会）", "detail": "米国の金融政策決定会合", "result": None},
    {"date": "2026-06-10", "title": "FOMC（米連邦公開市場委員会）", "detail": "米国の金融政策決定会合", "result": None},
    {"date": "2026-07-29", "title": "FOMC（米連邦公開市場委員会）", "detail": "米国の金融政策決定会合", "result": None},
    {"date": "2026-09-16", "title": "FOMC（米連邦公開市場委員会）", "detail": "米国の金融政策決定会合", "result": None},
    {"date": "2026-10-28", "title": "FOMC（米連邦公開市場委員会）", "detail": "米国の金融政策決定会合", "result": None},
    {"date": "2026-12-09", "title": "FOMC（米連邦公開市場委員会）", "detail": "米国の金融政策決定会合", "result": None},
    # US CPI 2025
    {"date": "2025-01-15", "title": "米CPI発表", "detail": "2024年12月分の米消費者物価指数", "result": "前年比+2.9%"},
    {"date": "2025-02-12", "title": "米CPI発表", "detail": "2025年1月分の米消費者物価指数", "result": "前年比+3.0%"},
    {"date": "2025-03-12", "title": "米CPI発表", "detail": "2025年2月分の米消費者物価指数", "result": "前年比+2.8%"},
    {"date": "2025-04-10", "title": "米CPI発表", "detail": "2025年3月分の米消費者物価指数", "result": None},
    {"date": "2025-05-13", "title": "米CPI発表", "detail": "2025年4月分の米消費者物価指数", "result": None},
    {"date": "2025-06-11", "title": "米CPI発表", "detail": "2025年5月分の米消費者物価指数", "result": None},
    {"date": "2025-07-11", "title": "米CPI発表", "detail": "2025年6月分の米消費者物価指数", "result": None},
    {"date": "2025-08-12", "title": "米CPI発表", "detail": "2025年7月分の米消費者物価指数", "result": None},
    {"date": "2025-09-10", "title": "米CPI発表", "detail": "2025年8月分の米消費者物価指数", "result": None},
    {"date": "2025-10-15", "title": "米CPI発表", "detail": "2025年9月分の米消費者物価指数", "result": None},
    {"date": "2025-11-13", "title": "米CPI発表", "detail": "2025年10月分の米消費者物価指数", "result": None},
    {"date": "2025-12-10", "title": "米CPI発表", "detail": "2025年11月分の米消費者物価指数", "result": None},
    # US CPI 2026
    {"date": "2026-01-14", "title": "米CPI発表", "detail": "2025年12月分の米消費者物価指数", "result": None},
    {"date": "2026-02-11", "title": "米CPI発表", "detail": "2026年1月分の米消費者物価指数", "result": None},
    {"date": "2026-03-11", "title": "米CPI発表", "detail": "2026年2月分の米消費者物価指数", "result": None},
    {"date": "2026-04-10", "title": "米CPI発表", "detail": "2026年3月分の米消費者物価指数", "result": None},
    {"date": "2026-05-13", "title": "米CPI発表", "detail": "2026年4月分の米消費者物価指数", "result": None},
    # US NFP 2025 (first Friday of each month)
    {"date": "2025-01-10", "title": "米雇用統計（NFP）", "detail": "2024年12月分の非農業部門雇用者数", "result": "25.6万人増"},
    {"date": "2025-02-07", "title": "米雇用統計（NFP）", "detail": "2025年1月分の非農業部門雇用者数", "result": "14.3万人増"},
    {"date": "2025-03-07", "title": "米雇用統計（NFP）", "detail": "2025年2月分の非農業部門雇用者数", "result": "15.1万人増"},
    {"date": "2025-04-04", "title": "米雇用統計（NFP）", "detail": "2025年3月分の非農業部門雇用者数", "result": None},
    {"date": "2025-05-02", "title": "米雇用統計（NFP）", "detail": "2025年4月分の非農業部門雇用者数", "result": None},
    {"date": "2025-06-06", "title": "米雇用統計（NFP）", "detail": "2025年5月分の非農業部門雇用者数", "result": None},
    {"date": "2025-07-03", "title": "米雇用統計（NFP）", "detail": "2025年6月分の非農業部門雇用者数", "result": None},
    {"date": "2025-08-01", "title": "米雇用統計（NFP）", "detail": "2025年7月分の非農業部門雇用者数", "result": None},
    {"date": "2025-09-05", "title": "米雇用統計（NFP）", "detail": "2025年8月分の非農業部門雇用者数", "result": None},
    {"date": "2025-10-03", "title": "米雇用統計（NFP）", "detail": "2025年9月分の非農業部門雇用者数", "result": None},
    {"date": "2025-11-07", "title": "米雇用統計（NFP）", "detail": "2025年10月分の非農業部門雇用者数", "result": None},
    {"date": "2025-12-05", "title": "米雇用統計（NFP）", "detail": "2025年11月分の非農業部門雇用者数", "result": None},
    {"date": "2026-01-09", "title": "米雇用統計（NFP）", "detail": "2025年12月分の非農業部門雇用者数", "result": None},
    {"date": "2026-02-06", "title": "米雇用統計（NFP）", "detail": "2026年1月分の非農業部門雇用者数", "result": None},
    {"date": "2026-03-06", "title": "米雇用統計（NFP）", "detail": "2026年2月分の非農業部門雇用者数", "result": None},
    {"date": "2026-04-03", "title": "米雇用統計（NFP）", "detail": "2026年3月分の非農業部門雇用者数", "result": None},
    {"date": "2026-05-01", "title": "米雇用統計（NFP）", "detail": "2026年4月分の非農業部門雇用者数", "result": None},
    # US GDP 2025 (quarterly advance estimates)
    {"date": "2025-01-30", "title": "米GDP（速報値）", "detail": "2024年Q4 GDP速報値", "result": None},
    {"date": "2025-04-30", "title": "米GDP（速報値）", "detail": "2025年Q1 GDP速報値", "result": None},
    {"date": "2025-07-30", "title": "米GDP（速報値）", "detail": "2025年Q2 GDP速報値", "result": None},
    {"date": "2025-10-29", "title": "米GDP（速報値）", "detail": "2025年Q3 GDP速報値", "result": None},
    {"date": "2026-01-28", "title": "米GDP（速報値）", "detail": "2025年Q4 GDP速報値", "result": None},
    {"date": "2026-04-29", "title": "米GDP（速報値）", "detail": "2026年Q1 GDP速報値", "result": None},
]


@app.get("/prices")
def get_prices(symbols: str = Query(...)):
    """カンマ区切りの銘柄コードの最新終値を一括返却"""
    codes = [s.strip() for s in symbols.split(",") if s.strip()]
    conn = get_conn()
    result = {}
    missing = []
    try:
        with conn.cursor() as cur:
            for code in codes:
                cur.execute(
                    "SELECT close FROM candles WHERE symbol=%s AND interval_type='1d' ORDER BY candle_time DESC LIMIT 1",
                    (code,)
                )
                row = cur.fetchone()
                if row:
                    result[code] = float(row[0])
                else:
                    missing.append(code)
    finally:
        conn.close()

    # DBにない銘柄はyfinanceから直接取得
    for code in missing:
        try:
            suffix = "" if "." in code else ".T"
            ticker = yf.Ticker(code + suffix)
            hist = ticker.history(period="5d", interval="1d")
            if not hist.empty:
                result[code] = float(hist["Close"].iloc[-1])
        except Exception:
            pass

    return result


@app.get("/memo")
def get_memo(symbol: str = Query(...)):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT content FROM stock_memos WHERE symbol=%s", (symbol,))
            row = cur.fetchone()
            return {"content": row[0] if row else ""}
    finally:
        conn.close()


@app.post("/memo")
def save_memo(symbol: str = Query(...), body: dict = None):
    content = (body or {}).get("content", "")
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO stock_memos (symbol, content) VALUES (%s, %s) "
                "ON DUPLICATE KEY UPDATE content=%s, updated_at=CURRENT_TIMESTAMP",
                (symbol, content, content)
            )
        conn.commit()
    finally:
        conn.close()
    return {"ok": True}


@app.get("/events")
def get_events(
    symbol: str = Query(...),
    date_from: str = Query(...),
    date_to: str = Query(...),
):
    try:
        d_from = date.fromisoformat(date_from)
        d_to   = date.fromisoformat(date_to)
    except ValueError:
        raise HTTPException(400, "Invalid date format. Use YYYY-MM-DD")

    today = date.today()
    events = []

    # --- Company events via yfinance ---
    sym = f"{symbol}.T" if (symbol.isdigit() or (len(symbol) == 4 and symbol.isalnum())) else symbol
    try:
        ticker = yf.Ticker(sym)

        # Earnings dates
        try:
            ed = ticker.earnings_dates
            if ed is not None and not ed.empty:
                for idx, row in ed.iterrows():
                    try:
                        d = idx.date() if hasattr(idx, 'date') else date.fromisoformat(str(idx)[:10])
                        if d < d_from or d > d_to:
                            continue
                        eps_est = row.get("EPS Estimate") if hasattr(row, 'get') else None
                        eps_act = row.get("Reported EPS") if hasattr(row, 'get') else None
                        detail = "決算発表"
                        result = None
                        if eps_est is not None and not (isinstance(eps_est, float) and pd.isna(eps_est)):
                            detail = f"EPS予想: {eps_est}"
                        if eps_act is not None and not (isinstance(eps_act, float) and pd.isna(eps_act)):
                            result = f"EPS実績: {eps_act}"
                        events.append({
                            "date":   d.isoformat(),
                            "type":   "company",
                            "title":  "決算発表",
                            "detail": detail,
                            "result": result,
                        })
                    except Exception:
                        pass
        except Exception:
            pass

        # Dividends
        try:
            divs = ticker.dividends
            if divs is not None and not divs.empty:
                for idx, val in divs.items():
                    try:
                        d = idx.date() if hasattr(idx, 'date') else date.fromisoformat(str(idx)[:10])
                        if d < d_from or d > d_to:
                            continue
                        events.append({
                            "date":   d.isoformat(),
                            "type":   "company",
                            "title":  "配当落ち日",
                            "detail": f"配当金: {round(float(val), 4)}",
                            "result": None,
                        })
                    except Exception:
                        pass
        except Exception:
            pass

        # Stock splits
        try:
            splits = ticker.splits
            if splits is not None and not splits.empty:
                for idx, val in splits.items():
                    try:
                        d = idx.date() if hasattr(idx, 'date') else date.fromisoformat(str(idx)[:10])
                        if d < d_from or d > d_to:
                            continue
                        events.append({
                            "date":   d.isoformat(),
                            "type":   "company",
                            "title":  "株式分割",
                            "detail": f"分割比率: {val}",
                            "result": None,
                        })
                    except Exception:
                        pass
        except Exception:
            pass

    except Exception:
        pass

    # --- BOJ events ---
    _monex = "https://mst.monex.co.jp/pc/servlet/ITS/report/EconomyIndexCalendar"
    for m in BOJ_MEETINGS:
        d = date.fromisoformat(m["date"])
        if d_from <= d <= d_to:
            result = m["result"] if d <= today else None
            events.append({
                "date":   m["date"],
                "type":   "boj",
                "title":  "日銀金融政策決定会合",
                "detail": "日本銀行による金融政策の決定会合（最終日）",
                "result": result,
                "url":    _monex,
            })

    # --- BOJ Tankan ---
    for t in BOJ_TANKAN:
        d = date.fromisoformat(t["date"])
        if d_from <= d <= d_to:
            result = t["result"] if d <= today else None
            events.append({
                "date":   t["date"],
                "type":   "boj",
                "title":  "日銀短観（企業短期経済観測調査）",
                "detail": t["detail"],
                "result": result,
                "url":    _monex,
            })

    # --- US economic events ---
    _econ_urls = {
        "FOMC": _monex,
        "CPI":  _monex,
        "NFP":  _monex,
        "GDP":  _monex,
    }
    for e in US_ECON_EVENTS:
        d = date.fromisoformat(e["date"])
        if d_from <= d <= d_to:
            result = e["result"] if d <= today else None
            url = next((v for k, v in _econ_urls.items() if k in e["title"]), None)
            events.append({
                "date":   e["date"],
                "type":   "us_econ",
                "title":  e["title"],
                "detail": e["detail"],
                "result": result,
                "url":    url,
            })

    events.sort(key=lambda x: x["date"])
    return events


@app.get("/margin_balance")
def get_margin_balance(symbol: str = Query(...)):
    """irbank から信用残高データを取得（週次・一般/制度分離）"""
    import re as _re2
    code_only = symbol.replace(".T", "").replace(".OS", "")
    if not code_only.isdigit():
        return {"history": []}
    hdrs = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36", "Accept-Language": "ja"}
    try:
        r = _requests.get(f"https://irbank.net/{code_only}/margin", headers=hdrs, timeout=8)
        text = r.text
        rows_html = _re2.findall(r'<tr class="o(?:cc|bb|dd)"[^>]*>(.*?)</tr>', text, _re2.DOTALL)

        def _num(html):
            s = _re2.sub(r'<[^>]+>', '', html).strip().replace(',', '')
            m = _re2.search(r'[\d]+', s)
            return int(m.group()) if m else None

        def _split(html):
            nums = _re2.findall(r'[\d,]+', html)
            nums = [int(n.replace(',', '')) for n in nums]
            return (nums[0], nums[1]) if len(nums) >= 2 else (None, None)

        current_year = None
        history = []
        for row_html in rows_html:
            year_m = _re2.search(r'<td class="ct">(\d{4})</td>', row_html)
            if year_m:
                current_year = int(year_m.group(1))
                continue
            date_m = _re2.search(r'data-k="\d+"[^>]*>(\d{2}/\d{2})', row_html)
            if not date_m or current_year is None:
                continue
            date_str = f"{current_year}/{date_m.group(1)}"
            tds = _re2.findall(r'<td[^>]*>(.*?)</td>', row_html, _re2.DOTALL)
            if len(tds) < 6:
                continue
            buy_gen, buy_inst = _split(tds[2])
            sell_gen, sell_inst = _split(tds[4])
            buy_total = (buy_gen or 0) + (buy_inst or 0)
            sell_total = (sell_gen or 0) + (sell_inst or 0)
            ratio_m = _re2.search(r'[\d.]+', _re2.sub(r'<[^>]+>', '', tds[5]))
            ratio = float(ratio_m.group()) if ratio_m else None
            # 単位: 株 → 万株
            history.append({
                "date": date_str,
                "buy":             round(buy_total / 10000, 2),
                "buy_general":     round(buy_gen   / 10000, 2) if buy_gen   is not None else None,
                "buy_institutional": round(buy_inst / 10000, 2) if buy_inst is not None else None,
                "sell":            round(sell_total / 10000, 2),
                "sell_general":    round(sell_gen   / 10000, 2) if sell_gen  is not None else None,
                "sell_institutional": round(sell_inst / 10000, 2) if sell_inst is not None else None,
                "ratio": ratio,
            })

        if not history:
            return {"history": []}

        latest = history[0]
        def _wow(key):
            return round(history[0][key] - history[1][key], 2) if len(history) > 1 and history[0][key] is not None and history[1][key] is not None else None

        return {
            "history":  history,
            "sell_wow": _wow("sell"),
            "buy_wow":  _wow("buy"),
            "sell":     latest["sell"],
            "buy":      latest["buy"],
            "ratio":    latest["ratio"],
            "date":     latest["date"],
        }
    except Exception:
        return {"history": []}


@app.get("/news")
def get_news(symbol: str = Query(...)):
    """銘柄関連ニュースをYahoo Finance RSS + yfinance newsから取得しDBに差分保存、直近1週間を返す"""
    import feedparser, re as re2, calendar as cal_mod
    sym = f"{symbol}.T" if (symbol.isdigit() or (len(symbol) == 4 and symbol.isalnum())) else symbol
    one_week_ago_ms = int((datetime.utcnow().timestamp() - 7 * 86400) * 1000)

    conn = get_conn()
    try:
        # 既存URLを取得して差分判定用セット作成
        with conn.cursor() as cur:
            cur.execute("SELECT url FROM news_cache WHERE symbol=%s", (symbol,))
            existing_urls = {row["url"] for row in cur.fetchall()}

        new_items = []

        # yfinance news (新構造: n["content"] にフィールドあり)
        try:
            import yfinance as yf
            t = yf.Ticker(sym)
            news = t.news or []
            for n in news[:20]:
                c = n.get("content") or n  # 新構造はcontentキー、旧構造はフラット
                title = c.get("title", "")
                # URL
                canon = c.get("canonicalUrl") or {}
                url = canon.get("url", "") if isinstance(canon, dict) else ""
                if not url:
                    url = c.get("link") or c.get("url") or n.get("link") or n.get("url") or ""
                if not url:
                    continue
                # 日時
                pub_str = c.get("pubDate") or c.get("displayTime") or ""
                published = None
                if pub_str:
                    try:
                        from datetime import timezone
                        dt = datetime.strptime(pub_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
                        published = int(dt.timestamp() * 1000)
                    except Exception:
                        pass
                if published is None:
                    pt = c.get("providerPublishTime") or n.get("providerPublishTime")
                    if pt:
                        published = int(pt) * 1000
                provider = c.get("provider") or {}
                source = provider.get("displayName", "") if isinstance(provider, dict) else c.get("publisher", "")
                new_items.append({
                    "url": url, "title": title, "published": published,
                    "source": source, "summary_ja": None, "title_ja": None,
                })
        except Exception:
            pass

        # irbank.net（銘柄別IR/決算ニュース）
        try:
            import datetime as _dt2
            code_only = symbol.replace(".T", "").replace(".OS", "")
            _ib_headers = {"User-Agent": "Mozilla/5.0 (compatible)"}
            _ib_resp = _requests.get(f"https://irbank.net/{code_only}/news", headers=_ib_headers, timeout=8)
            if _ib_resp.status_code == 200:
                _ib_text = _ib_resp.text
                # 日付セクションと記事タイトルを対応付け
                _cur_date = None
                _date_re = re2.compile(r'<td class="lf" colspan="\d+"[^>]*>(\d{4})年(\d+)月(\d+)日</td>')
                # title属性がhrefより先に来るパターン: <a class="nxq" title="..." href="/news/...">
                _news_re = re2.compile(r'<a[^>]+title="([^"]+)"[^>]+href="/news/(\d+)"')
                for _line in _ib_text.split('\n'):
                    _dm = _date_re.search(_line)
                    if _dm:
                        try:
                            _cur_date = int(_dt2.datetime(int(_dm.group(1)), int(_dm.group(2)), int(_dm.group(3))).timestamp() * 1000)
                        except Exception:
                            _cur_date = None
                    _nm = _news_re.search(_line)
                    if _nm:
                        _full_title = _nm.group(1)  # "7203 トヨタ自動車、..." 形式
                        _doc_id = _nm.group(2)
                        # 社名プレフィックス "7203 トヨタ自動車、" を除去
                        _title = re2.sub(r'^[\w\s]+[\u3001、]', '', _full_title).strip() or _full_title
                        _url = f"https://irbank.net/news/{_doc_id}"
                        new_items.append({
                            "url": _url,
                            "title": _title,
                            "published": _cur_date,
                            "source": "irbank（企業開示）",
                            "summary_ja": None,
                            "title_ja": _title,
                        })
        except Exception:
            pass

        # Google News RSS: 銘柄名・業種で検索（日本語・英語）
        def fetch_google_news(query: str, lang: str, limit: int = 10):
            import urllib.parse
            hl = "ja" if lang == "ja" else "en"
            gl = "JP" if lang == "ja" else "US"
            ceid = f"{gl}:{hl}"
            q = urllib.parse.quote(query)
            url_rss = f"https://news.google.com/rss/search?q={q}&hl={hl}&gl={gl}&ceid={ceid}"
            results = []
            try:
                feed = feedparser.parse(url_rss)
                for entry in feed.entries[:limit]:
                    u = entry.get("link", "")
                    if not u:
                        continue
                    published = None
                    if hasattr(entry, 'published_parsed') and entry.published_parsed:
                        published = cal_mod.timegm(entry.published_parsed) * 1000
                    results.append({
                        "url": u,
                        "title": entry.get("title", ""),
                        "published": published,
                        "source": entry.get("source", {}).get("title", "Google News") if hasattr(entry.get("source", {}), "get") else "Google News",
                        "summary_ja": None,
                        "title_ja": entry.get("title", "") if lang == "ja" else None,
                    })
            except Exception:
                pass
            return results

        # yfinanceから会社名・業種・セクターを取得
        company_name_ja = ""
        company_name_en = ""
        sector_ja = ""
        industry_ja = ""
        try:
            import yfinance as yf
            info = yf.Ticker(sym).info or {}
            company_name_en = info.get("longName") or info.get("shortName") or ""
            sector_en = info.get("sector", "")
            industry_en = info.get("industry", "")
            # 日本株は longName が日本語の場合あり
            if any(ord(c) > 0x3000 for c in company_name_en):
                company_name_ja = company_name_en
                company_name_en = info.get("shortName", "")
        except Exception:
            pass

        # Google News 検索クエリ構築
        code_only = symbol.replace(".T", "")

        # 日本語: 銘柄コード or 会社名
        ja_query = company_name_ja if company_name_ja else code_only
        new_items += fetch_google_news(ja_query, "ja", 10)

        # 適時開示ニュース（日本語）
        if company_name_ja:
            new_items += fetch_google_news(f"{company_name_ja} 適時開示", "ja", 5)

        # 英語: 会社名
        if company_name_en:
            new_items += fetch_google_news(company_name_en, "en", 8)

        # 業種・セクター（日本語Google News）- Bedrockで翻訳した用語で検索
        if sector_en or industry_en:
            sector_query_en = industry_en or sector_en
            new_items += fetch_google_news(sector_query_en, "en", 5)

        # 差分のみ（既存DBにないURL）
        diff_items = [i for i in new_items if i["url"] not in existing_urls]

        # 英語タイトルをBedrockで翻訳
        to_translate = [i for i in diff_items if not i.get("title_ja") and i.get("title")]
        if to_translate:
            try:
                titles = "\n".join(f"{idx+1}. {i['title']}" for idx, i in enumerate(to_translate))
                body = json.dumps({
                    "anthropic_version": "bedrock-2023-05-31",
                    "max_tokens": 1000,
                    "messages": [{"role": "user", "content": f"以下の英語ニュースタイトルを日本語に翻訳してください。番号付きで返してください。\n{titles}"}]
                })
                resp = boto3.client("bedrock-runtime", region_name="us-east-1").invoke_model(
                    modelId="us.anthropic.claude-haiku-4-5-20251001-v1:0", body=body,
                    contentType="application/json", accept="application/json"
                )
                text = json.loads(resp["body"].read())["content"][0]["text"]
                lines = [l.strip() for l in text.strip().split("\n") if l.strip()]
                for idx, item in enumerate(to_translate):
                    for line in lines:
                        if line.startswith(f"{idx+1}."):
                            item["title_ja"] = line[len(f"{idx+1}."):].strip()
                            break
            except Exception:
                pass

        # 差分をDBに保存
        if diff_items:
            with conn.cursor() as cur:
                for i in diff_items:
                    try:
                        cur.execute(
                            "INSERT IGNORE INTO news_cache (symbol, url, title, title_ja, published, source, summary_ja) VALUES (%s,%s,%s,%s,%s,%s,%s)",
                            (symbol, i["url"], i.get("title"), i.get("title_ja"), i.get("published"), i.get("source"), i.get("summary_ja"))
                        )
                    except Exception:
                        pass
            conn.commit()

        # DBから直近データを返す（一般ニュース：1週間 / 企業開示(irbank)：90日）
        one_week_ago_ms = int((datetime.utcnow().timestamp() - 7 * 86400) * 1000)
        ninety_days_ago_ms = int((datetime.utcnow().timestamp() - 90 * 86400) * 1000)
        with conn.cursor() as cur:
            cur.execute(
                """SELECT url, title, title_ja, published, source, summary_ja
                   FROM news_cache
                   WHERE symbol=%s AND (
                     (source LIKE '%%irbank%%' AND published >= %s) OR
                     (source NOT LIKE '%%irbank%%' AND published >= %s)
                   )
                   ORDER BY published DESC LIMIT 40""",
                (symbol, ninety_days_ago_ms, one_week_ago_ms)
            )
            rows = cur.fetchall()

        return [{"url": r["url"], "title": r["title"], "title_ja": r["title_ja"],
                 "published": r["published"], "source": r["source"], "summary_ja": r["summary_ja"]}
                for r in rows]
    finally:
        conn.close()


def _parse_ih_df(df):
    """institutional_holders / mutualfund_holders をパース（yfinanceの実際のカラム名に対応）"""
    rows = []
    if df is None or df.empty:
        return rows
    cols = {str(c).lower(): c for c in df.columns}

    def find_col(*candidates):
        for cand in candidates:
            for k, v in cols.items():
                if cand.lower() in k:
                    return v
        return None

    c_holder  = find_col("holder")
    c_shares  = find_col("share")
    c_pct     = find_col("pctheld", "% out", "pct_held", "pct held")
    c_value   = find_col("value")
    c_change  = find_col("pctchange", "% change", "pct_change")
    c_date    = find_col("date reported", "date")

    def g(row, col, default=None):
        if col is None: return default
        v = row[col]
        return None if (v is None or (isinstance(v, float) and pd.isna(v))) else v

    for _, row in df.iterrows():
        try:
            rows.append({
                "holder":     str(g(row, c_holder, "")),
                "shares":     int(float(g(row, c_shares, 0) or 0)),
                "pct_held":   float(g(row, c_pct, 0) or 0),
                "value":      int(float(g(row, c_value, 0) or 0)),
                "pct_change": float(g(row, c_change, 0) or 0),
                "date":       str(g(row, c_date, ""))[:10],
            })
        except Exception:
            pass
    return rows


def _scrape_minkabu_picks(code: str) -> list:
    """みんかぶの株価予想（ピック）一覧をスクレイピングして返す"""
    url = f"https://minkabu.jp/stock/{code}/pick"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0"}
    r = _requests.get(url, headers=headers, timeout=12)
    if r.status_code != 200:
        return []
    html = r.text
    posts = []
    # <li class="flex items-center ..."> ブロックを抽出
    li_pat = _re.compile(r'<li class="flex items-center[^"]*">(.*?)</li>', _re.DOTALL)
    for m in li_pat.finditer(html):
        li = m.group(1)
        # 買い/売り判定
        dir_m = _re.search(r'bg-minkabuPicks(Buy|Sell)', li)
        if not dir_m:
            continue
        direction = "買い" if dir_m.group(1) == "Buy" else "売り"
        # リンクとコメントタイトル
        link_m = _re.search(r'href="(/stock/\d+/pick/(\d+))"[^>]*>\s*(.*?)\s*</a>', li, _re.DOTALL)
        if not link_m:
            continue
        pick_id  = link_m.group(2)
        title    = _re.sub(r'\s+', ' ', link_m.group(3)).strip()
        # ユーザー名
        user_m = _re.search(r'<span>([^<]+?)さん</span>', li)
        username = (user_m.group(1) + "さん") if user_m else ""
        # 日付
        date_m = _re.search(r'text-slate-500[^>]*>\s*([^<]+?)\s*</div>', li)
        date_str = date_m.group(1).strip() if date_m else ""
        posts.append({
            "id":              pick_id,
            "text":            title,
            "created_at":      date_str,
            "author_name":     username,
            "author_username": "",
            "author_image":    "",
            "likes":           0,
            "retweets":        0,
            "replies":         0,
            "direction":       direction,
            "url":             f"https://minkabu.jp/stock/{code}/pick/{pick_id}",
            "source":          "minkabu",
        })
    return posts[:50]


@app.get("/x_posts")
def get_x_posts(symbol: str = Query(...), name: str = Query("")):
    """日本株: みんかぶ株価予想 / US株: Twitter API v2"""
    # 日本株（4桁以下の数字コード）はみんかぶをスクレイピング
    is_jp = symbol.isdigit() and len(symbol) <= 4
    if is_jp:
        try:
            return _scrape_minkabu_picks(symbol)
        except Exception as e:
            raise HTTPException(500, str(e))

    # US株: Twitter API v2
    bearer = os.getenv("TWITTER_BEARER_TOKEN", "")
    if not bearer:
        raise HTTPException(503, "Twitter Bearer Token not configured")
    q_name = name.strip() or symbol
    query  = f'({q_name} OR {symbol}) lang:en -is:retweet'
    headers = {"Authorization": f"Bearer {bearer}"}
    params  = {
        "query": query, "max_results": 50,
        "tweet.fields": "created_at,author_id,public_metrics",
        "expansions": "author_id",
        "user.fields": "name,username,profile_image_url",
        "sort_order": "recency",
    }
    try:
        r = _requests.get("https://api.twitter.com/2/tweets/search/recent",
                          headers=headers, params=params, timeout=15)
        if r.status_code == 401: raise HTTPException(401, "Twitter Bearer Token が無効です")
        if r.status_code == 402: raise HTTPException(402, "Twitter API は有料プランが必要です（$100/月〜）")
        if r.status_code == 429: raise HTTPException(429, "Twitter API レート制限に達しました")
        r.raise_for_status()
        data   = r.json()
        tweets = data.get("data") or []
        users  = {u["id"]: u for u in (data.get("includes") or {}).get("users", [])}
        result = []
        for tw in tweets:
            user = users.get(tw.get("author_id", ""), {})
            met  = tw.get("public_metrics") or {}
            result.append({
                "id": tw.get("id", ""), "text": tw.get("text", ""),
                "created_at": tw.get("created_at", ""),
                "author_name": user.get("name", ""), "author_username": user.get("username", ""),
                "author_image": user.get("profile_image_url", ""),
                "likes": met.get("like_count", 0), "retweets": met.get("retweet_count", 0),
                "replies": met.get("reply_count", 0), "source": "twitter",
            })
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/holders")
def get_holders(symbol: str = Query(...)):
    """機関投資家・主要株主データをyfinanceから取得しDBキャッシュ"""
    sym = f"{symbol}.T" if (symbol.isdigit() or (len(symbol) == 4 and symbol.isalnum())) else symbol
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT data, updated_at FROM holders_cache WHERE symbol=%s", (symbol,))
            row = cur.fetchone()
            if row and (datetime.utcnow() - row["updated_at"]).total_seconds() < 86400:
                return json.loads(row["data"])

        t = yf.Ticker(sym)
        result = {
            "institutional": [], "mutualfund": [],
            "aggregate": {"count": 0, "pct_held": 0.0, "float_pct": 0.0, "insider_pct": 0.0},
            "currency": "",
        }
        try:
            result["currency"] = (t.info or {}).get("currency", "")
        except Exception:
            pass

        # major_holders から集計サマリー取得
        try:
            mh = t.major_holders
            if mh is not None and not mh.empty:
                mh_dict = {}
                for _, r in mh.iterrows():
                    if hasattr(r, 'index') and len(r) >= 2:
                        mh_dict[str(r.index[0])] = r.iloc[0]
                    elif len(r.values) >= 1:
                        # Breakdown / Value形式
                        pass
                # インデックスベースで取得
                idx_vals = {}
                for idx_val, data_val in mh.iterrows():
                    idx_vals[str(idx_val)] = data_val.iloc[0]
                agg = result["aggregate"]
                for k, v in idx_vals.items():
                    kl = k.lower()
                    try:
                        fv = float(v)
                        if "institutionscount" in kl or "institutions_count" in kl or kl == "institutionscount":
                            agg["count"] = int(fv)
                        elif "institutionspercentheld" in kl and "float" not in kl:
                            agg["pct_held"] = fv
                        elif "institutionsfloat" in kl:
                            agg["float_pct"] = fv
                        elif "insider" in kl:
                            agg["insider_pct"] = fv
                    except Exception:
                        pass
                # Breakdown列形式の場合
                if "Breakdown" in mh.columns:
                    for _, r in mh.iterrows():
                        k = str(r.get("Breakdown","")).lower()
                        try:
                            fv = float(r.get("Value", 0) or 0)
                            if "institutionscount" in k:
                                agg["count"] = int(fv)
                            elif "institutionspercentheld" in k and "float" not in k:
                                agg["pct_held"] = fv
                            elif "institutionsfloat" in k:
                                agg["float_pct"] = fv
                            elif "insider" in k:
                                agg["insider_pct"] = fv
                        except Exception:
                            pass
        except Exception:
            pass

        try:
            result["institutional"] = _parse_ih_df(t.institutional_holders)
        except Exception:
            pass
        try:
            result["mutualfund"] = _parse_ih_df(t.mutualfund_holders)
        except Exception:
            pass

        # 集計がまだ空なら個別リストから計算
        agg = result["aggregate"]
        all_holders = result["institutional"] + result["mutualfund"]
        if agg["count"] == 0 and all_holders:
            agg["count"] = len(set(h["holder"] for h in all_holders))
        if agg["pct_held"] == 0 and all_holders:
            agg["pct_held"] = sum(h["pct_held"] for h in all_holders)

        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO holders_cache (symbol,data) VALUES (%s,%s) ON DUPLICATE KEY UPDATE data=%s,updated_at=NOW()",
                (symbol, json.dumps(result), json.dumps(result))
            )
        conn.commit()
        return result
    except Exception as e:
        raise HTTPException(500, str(e))
    finally:
        conn.close()


@app.get("/analyst")
def get_analyst(symbol: str = Query(...)):
    """アナリスト予想を複数サイトから取得"""
    symbol = _re.sub(r'\.T$', '', symbol, flags=_re.IGNORECASE)
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT data, updated_at FROM analyst_cache WHERE symbol=%s", (symbol,))
            row = cur.fetchone()
            if row and (datetime.utcnow() - row["updated_at"]).total_seconds() < 10800:
                return json.loads(row["data"])

        result = _scrape_all_analyst(symbol)

        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO analyst_cache (symbol,data) VALUES (%s,%s) ON DUPLICATE KEY UPDATE data=%s,updated_at=NOW()",
                (symbol, json.dumps(result, ensure_ascii=False), json.dumps(result, ensure_ascii=False))
            )
        conn.commit()
        return result
    except Exception as e:
        raise HTTPException(500, str(e))
    finally:
        conn.close()


def _scrape_all_analyst(symbol: str) -> dict:
    sources = []

    # --- みんかぶ アナリストコンセンサス ---
    try:
        mk = _scrape_minkabu_analyst(symbol)
        if mk:
            sources.append(mk)
    except Exception as e:
        sources.append({"source": "みんかぶ", "url": f"https://minkabu.jp/stock/{symbol}/analyst_consensus", "error": str(e), "entries": [], "history": []})

    # --- 株予報Pro ---
    try:
        ky = _scrape_kabuyoho_analyst(symbol)
        if ky:
            sources.append(ky)
    except Exception as e:
        sources.append({"source": "株予報Pro", "url": f"https://kabuyoho.jp/reportTarget?bcode={symbol}", "error": str(e), "entries": []})

    # --- 目標株価まとめ (kabuka.jp.net) ---
    try:
        kb = _scrape_kabuka_analyst(symbol)
        if kb:
            sources.append(kb)
    except Exception as e:
        sources.append({"source": "目標株価まとめ", "url": f"https://www.kabuka.jp.net/rating/{symbol}.html", "error": str(e), "entries": []})

    return {"symbol": symbol, "sources": sources}


def _scrape_kabuyoho_analyst(code: str) -> dict:
    """株予報Pro アナリスト目標株価ページをスクレイピング"""
    url = f"https://kabuyoho.jp/reportTarget?bcode={code}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "ja,en;q=0.9",
    }
    resp = _requests.get(url, headers=headers, timeout=15)
    resp.raise_for_status()
    html = resp.text

    result = {
        "source": "株予報Pro",
        "url": url,
        "date": None,
        "consensus_price": None,
        "price_change_pct": None,
        "divergence_pct": None,
        "consensus_rating": None,
        "analyst_count": None,
        "breakdown": {},
        "history": [],
        "entries": [],
    }

    def clean_cell(c):
        return _re.sub(r'<[^>]+>', '', c).strip().replace('\xa0', '').replace('  ', ' ')

    tbodies = _re.findall(r'<tbody[^>]*>(.*?)</tbody>', html, _re.DOTALL)

    # tbody[0]: 目標株価平均 / 対前週変化率 / 対株価かい離
    if len(tbodies) > 0:
        rows = _re.findall(r'<tr[^>]*>(.*?)</tr>', tbodies[0], _re.DOTALL)
        if rows:
            cells = _re.findall(r'<t[dh][^>]*>(.*?)</t[dh]>', rows[0], _re.DOTALL)
            clean = [clean_cell(c) for c in cells]
            if clean:
                p = _re.search(r'([\d,]+)円', clean[0])
                if p:
                    result["consensus_price"] = int(p.group(1).replace(',', ''))
                if len(clean) > 1:
                    result["price_change_pct"] = clean[1]
                if len(clean) > 2:
                    result["divergence_pct"] = clean[2]

    # tbody[1]: レーティング平均 / アナリスト数
    if len(tbodies) > 1:
        rows = _re.findall(r'<tr[^>]*>(.*?)</tr>', tbodies[1], _re.DOTALL)
        for row in rows:
            cells = _re.findall(r'<t[dh][^>]*>(.*?)</t[dh]>', row, _re.DOTALL)
            c = [clean_cell(x) for x in cells]
            if len(c) >= 2:
                if 'レーティング' in c[0]:
                    result["consensus_rating"] = c[1]
                elif 'アナリスト数' in c[0]:
                    result["analyst_count"] = c[1]

    # tbody[2]: 内訳（強気/やや強気/中立/やや弱気/弱気）
    if len(tbodies) > 2:
        rows = _re.findall(r'<tr[^>]*>(.*?)</tr>', tbodies[2], _re.DOTALL)
        for row in rows:
            cells = _re.findall(r'<t[dh][^>]*>(.*?)</t[dh]>', row, _re.DOTALL)
            c = [clean_cell(x) for x in cells]
            if len(c) >= 2 and c[0] and c[1]:
                result["breakdown"][c[0]] = c[1]

    return result


def _scrape_kabuka_analyst(code: str) -> dict:
    """目標株価まとめ (kabuka.jp.net) をスクレイピング"""
    url = f"https://www.kabuka.jp.net/rating/{code}.html"
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "ja,en;q=0.9",
    }
    resp = _requests.get(url, headers=headers, timeout=15)
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    html = resp.text

    result = {
        "source": "目標株価まとめ",
        "url": url,
        "current_price": None,
        "consensus_price": None,     # 12ヵ月平均
        "divergence": None,          # 乖離率
        "entries": [],               # 個別アナリスト一覧
        "summary": [],               # 平均・中央値サマリー
    }

    def clean(s):
        return _re.sub(r'\s+', ' ', _re.sub(r'<[^>]+>', '', s)).strip()

    trs = _re.findall(r'<tr[^>]*>(.*?)</tr>', html, _re.DOTALL)
    rows = [clean(tr) for tr in trs]

    # tr[2]: 株価
    if len(rows) > 2:
        m = _re.search(r'([\d,]+)\s*\(', rows[2])
        if m:
            result["current_price"] = int(m.group(1).replace(',', ''))

    # tr[4]: 目標株価12ヵ月平均・乖離率
    if len(rows) > 4:
        m = _re.search(r'([\d,]+)([\-+][\d,]+\s*\([^)]+\))', rows[4])
        if m:
            result["consensus_price"] = int(m.group(1).replace(',', ''))
            result["divergence"] = m.group(2).strip()

    # tr[8]: ヘッダー行「発表日 証券会社 レーティング 目標株価 乖離率」
    # tr[9]〜: 個別アナリスト行
    # 集計行（平均値・中央値）で終了
    in_entries = False
    for row in rows:
        if '発表日' in row and '証券会社' in row:
            in_entries = True
            continue
        if not in_entries:
            continue
        # 集計行
        if '平均値' in row or '中央値' in row:
            # サマリー行: "目標株価平均値（6ヵ月） 2,375-8.90%"
            label_m = _re.search(r'(目標株価\S+)\s*([\d,]+)([\-+][\d.]+%)', row)
            if label_m:
                result["summary"].append({
                    "label": label_m.group(1).strip(),
                    "price": int(label_m.group(2).replace(',', '')),
                    "divergence": label_m.group(3),
                })
            continue
        # 個別エントリー行: "2026/03/24ジェフリーズHold → Buy格上げ1,500 → 2,800+7.40%"
        # 日付
        date_m = _re.search(r'^(\d{4}/\d{2}/\d{2})', row)
        if not date_m:
            continue
        date = date_m.group(1)
        rest = row[len(date):].strip()

        # 目標株価: "1,500 → 2,800" or "2,800"
        price_m = _re.search(r'([\d,]+)\s*→\s*([\d,]+)', rest)
        if price_m:
            prev_target = int(price_m.group(1).replace(',', ''))
            new_target = int(price_m.group(2).replace(',', ''))
        else:
            single = _re.search(r'([\d,]{3,})', rest)
            prev_target = None
            new_target = int(single.group(1).replace(',', '')) if single else None

        # 乖離率
        div_m = _re.search(r'([+-][\d.]+%)\s*$', rest)
        divergence = div_m.group(1) if div_m else None

        # 証券会社名とレーティング: 日付・価格・乖離率を除いた部分
        temp = rest
        if price_m:
            temp = temp[:temp.rfind(price_m.group(0))]
        elif single:
            temp = temp[:temp.rfind(single.group(0))]
        if divergence:
            temp = temp.replace(divergence, '')
        temp = temp.strip()

        # レーティング変化パターン: "ジェフリーズHold → Buy格上げ" or "JPMNeutral継続"
        # アクション（格上げ/格下げ/継続/新規）を先に除去
        action = ''
        for act in ['格上げ', '格下げ', '継続', '新規']:
            if act in temp:
                action = act
                temp = temp.replace(act, '').strip()

        # レーティング変化: "Hold → Buy"
        rating_change_m = _re.search(
            r'((?:強気|買い|Hold|Buy|Neutral|中立|売り|Sell|OP|MP|NR|UP|OW|UW|Equal|[1-5])\S*)'
            r'\s*→\s*'
            r'((?:強気|買い|Hold|Buy|Neutral|中立|売り|Sell|OP|MP|NR|UP|OW|UW|Equal|[1-5])\S*)',
            temp
        )
        if rating_change_m:
            prev_rating = rating_change_m.group(1).strip()
            new_rating = rating_change_m.group(2).strip()
            company = temp[:temp.index(rating_change_m.group(0))].strip()
        else:
            # 単一レーティング: "ジェフリーズBuy" → 会社名+レーティング
            single_m = _re.search(
                r'^(.+?)((?:強気|買い|Hold|Buy|Neutral|中立|売り|Sell|OP|MP|NR|UP|OW|UW|Equal|[1-5])\S*)$',
                temp
            )
            if single_m:
                company = single_m.group(1).strip()
                prev_rating = new_rating = single_m.group(2).strip()
            else:
                company = temp
                prev_rating = new_rating = ''

        result["entries"].append({
            "date": date,
            "company": company,
            "prev_rating": prev_rating,
            "rating": new_rating,
            "action": action,
            "prev_target": prev_target,
            "target": new_target,
            "divergence": divergence,
        })

    return result


def _scrape_minkabu_analyst(code: str) -> dict:
    """みんかぶ アナリストコンセンサスページをスクレイピング"""
    url = f"https://minkabu.jp/stock/{code}/analyst_consensus"
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "ja,en;q=0.9",
    }
    resp = _requests.get(url, headers=headers, timeout=15)
    resp.raise_for_status()
    html = resp.text

    result = {
        "source": "みんかぶ",
        "url": url,
        "date": None,
        "consensus_rating": None,
        "consensus_price": None,
        "price_diff": None,
        "upside_pct": None,
        "breakdown": {},   # 強気買い/買い/中立/売り/強気売り の人数
        "history": [],     # 3ヶ月前/1ヶ月前/1週間前/最新 の推移
        "entries": [],
    }

    # 日付 (2026/04/10)
    date_m = _re.search(r'アナリスト予想<br><span[^>]*>\((\d{4}/\d{2}/\d{2})\)</span>', html)
    if date_m:
        result["date"] = date_m.group(1)

    # コンセンサス評価 (買い/中立/売り等)
    rating_m = _re.search(r'<span class="value">([^<]+)</span>', html)
    if not rating_m:
        rating_m = _re.search(r'md_picksPlate[^>]+>.*?<span[^>]*>([^<]+)</span>', html, _re.DOTALL)
    if rating_m:
        result["consensus_rating"] = rating_m.group(1).strip()

    # 目標株価（複数パターン対応）
    price_m = (_re.search(r'<span[^>]*fsxxl[^>]*>([\d,]+)</span>', html)
               or _re.search(r'予想株価\s*([\d,]+)円', html)
               or _re.search(r'平均目標株価は([\d,]+)円', html))
    if price_m:
        result["consensus_price"] = int(price_m.group(1).replace(',', ''))

    # 現在株価との差
    diff_m = _re.search(r'現在株価との差&nbsp;&nbsp;<span>([+-]?[\d.]+)&nbsp;円</span>', html)
    if diff_m:
        result["price_diff"] = float(diff_m.group(1).replace(',', ''))

    # 上昇余地%
    upside_m = _re.search(r'あと([\d.]+)%上昇', html)
    if upside_m:
        result["upside_pct"] = float(upside_m.group(1))

    # 内訳 (強気買いN人、買いN人、中立N人)
    for label in ['強気買い', '買い', '中立', '売り', '強気売り']:
        m = _re.search(label + r'(\d+)人', html)
        if m:
            result["breakdown"][label] = int(m.group(1))

    # 推移テーブル (tbody[0]: 3ヶ月前/1ヶ月前/1週間前/最新)
    tbodies = _re.findall(r'<tbody[^>]*>(.*?)</tbody>', html, _re.DOTALL)
    if tbodies:
        rows = _re.findall(r'<tr[^>]*>(.*?)</tr>', tbodies[0], _re.DOTALL)
        # ヘッダー行から時期ラベルを取得
        header_cells = _re.findall(r'<t[dh][^>]*>(.*?)</t[dh]>', rows[0], _re.DOTALL) if rows else []
        periods = [_re.sub(r'<[^>]+>', '', c).strip() for c in header_cells][1:]  # 先頭空列を除く
        for row in rows[1:]:
            cells = _re.findall(r'<t[dh][^>]*>(.*?)</t[dh]>', row, _re.DOTALL)
            clean = [_re.sub(r'<[^>]+>', '', c).strip() for c in cells]
            if len(clean) >= 2:
                label = clean[0]
                values = clean[1:]
                for i, period in enumerate(periods):
                    if i < len(values):
                        result["history"].append({"period": period, "label": label, "value": values[i]})

    return result


@app.get("/financials")
def get_financials(symbol: str = Query(...)):
    """みんかぶから四半期・年次財務データを取得"""
    symbol = _re.sub(r'\.T$', '', symbol, flags=_re.IGNORECASE)
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT data, updated_at FROM financials_cache WHERE symbol=%s", (symbol,))
            row = cur.fetchone()
            if row and (datetime.utcnow() - row["updated_at"]).total_seconds() < 21600:
                return json.loads(row["data"])

        result = _scrape_minkabu_financials(symbol)

        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO financials_cache (symbol,data) VALUES (%s,%s) ON DUPLICATE KEY UPDATE data=%s,updated_at=NOW()",
                (symbol, json.dumps(result), json.dumps(result))
            )
        conn.commit()
        return result
    except Exception as e:
        raise HTTPException(500, str(e))
    finally:
        conn.close()


def _scrape_minkabu_financials(symbol: str) -> dict:
    """みんかぶ決算ページをスクレイピングして四半期・年次データを返す"""
    url = f"https://minkabu.jp/stock/{symbol}/settlement"
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
    resp = _requests.get(url, headers=headers, timeout=15)
    resp.raise_for_status()
    html = resp.text

    # 銘柄名・通貨
    title_m = _re.search(r'<title>([^(]+)\s*\(', html)
    stock_name = title_m.group(1).strip() if title_m else ""
    currency = "JPY"

    # 「会社予想：XXXX年X月期時点」を抽出（当期=fy_year期）
    fy_m = _re.search(r'会社予想：(\d{4})年(\d{1,2})月期時点', html)
    fy_year   = int(fy_m.group(1)) if fy_m else None
    fy_month  = int(fy_m.group(2)) if fy_m else 3

    # chart-elements の JSON を全て抽出
    ce_pattern = _re.compile(r':chart-elements="\{([^"]+)\}"')
    raw_matches = ce_pattern.findall(html)

    def parse_ce(raw: str) -> dict:
        s = raw.replace("&quot;", '"')
        try:
            return json.loads("{" + s + "}")
        except Exception:
            return {}

    parsed = [parse_ce(m) for m in raw_matches]

    # 出現順: 経常利益(大), 売上高, 営業利益, 純利益
    metric_order = ["ordinary_inc", "revenue", "op_inc", "net_inc"]
    fy_keys = ["two_years", "a_year_ago", "current"]
    key_map = {
        "two_years":  "two_years_ago_results",
        "a_year_ago": "a_year_ago_results",
        "current":    "current_results",
    }

    def to_oku(v):
        """百万円→円"""
        if v is None: return None
        return float(v) * 1_000_000

    # FY年度ラベル: current=fy_year期, a_year_ago=fy_year-1期, two_years=fy_year-2期
    if fy_year:
        fy_label_map = {
            "two_years":  str(fy_year - 2),
            "a_year_ago": str(fy_year - 1),
            "current":    str(fy_year),
        }
        fy_labels = [fy_label_map[k] for k in fy_keys]
    else:
        fy_label_map = {"two_years": "前々期", "a_year_ago": "前期", "current": "当期"}
        fy_labels = ["前々期", "前期", "当期"]

    q_labels = ["1Q", "2Q", "3Q", "通期"]

    # chart-elementsから各指標データを取得
    metric_data = {}  # metric -> {fy_key: [v0,v1,v2,v3]}
    for mi, metric in enumerate(metric_order):
        if mi >= len(parsed): break
        ce = parsed[mi]
        metric_data[metric] = {fk: ce.get(key_map[fk], [None]*4) for fk in fy_keys}
        metric_data[metric]["latest_proj"]  = ce.get("latest_projections",  [None]*4)
        metric_data[metric]["initial_proj"] = ce.get("initial_projections", [None]*4)
        metric_data[metric]["analyst_proj"] = ce.get("analyst_projections", [None]*4)

    # 四半期単期変換（累計→差分、通期はそのまま）
    def cumul_to_single(arr):
        if not arr or len(arr) < 4: return arr
        res = [arr[0]]
        for i in range(1, 3):
            if arr[i] is not None and arr[i-1] is not None:
                res.append(arr[i] - arr[i-1])
            else:
                res.append(arr[i])
        res.append(arr[3])  # 通期はそのまま
        return res

    # quarterly リスト生成
    quarterly = []
    for fk in fy_keys:
        fy_label = fy_label_map[fk]
        for qi, ql in enumerate(q_labels):
            row = {"period": f"{fy_label}年{ql}", "is_estimate": False}
            has_data = False
            for metric in ["revenue", "op_inc", "net_inc", "ordinary_inc"]:
                arr = metric_data.get(metric, {}).get(fk, [None]*4)
                singles = cumul_to_single(arr)
                v = singles[qi] if qi < len(singles) else None
                row[metric] = to_oku(v)
                if row[metric] is not None: has_data = True
            if has_data:
                quarterly.append(row)

    # 予測（通期のみ）
    estimates = []
    for proj_key, label in [("latest_proj","最新会社予想"),("initial_proj","当初会社予想"),("analyst_proj","アナリスト予想")]:
        row = {"period": label, "is_estimate": True}
        has_data = False
        for metric in ["revenue", "op_inc", "net_inc", "ordinary_inc"]:
            arr = metric_data.get(metric, {}).get(proj_key, [None]*4)
            v = arr[3] if len(arr) > 3 else None
            row[metric] = to_oku(v)
            if row[metric] is not None: has_data = True
        if has_data:
            estimates.append(row)

    # 年次テーブルデータをスクレイピング
    annual = _scrape_minkabu_annual(html)

    return {
        "stock_name": stock_name,
        "currency": currency,
        "fy_year": fy_year,
        "fy_month": fy_month,
        "fy_labels": fy_labels,
        "quarterly": quarterly,
        "estimates": estimates,
        "annual": annual,
    }


def _scrape_minkabu_annual(html: str) -> list:
    """みんかぶ決算ページの年次テーブルを抽出"""
    annual = []
    try:
        # 年次テーブルを探す（業績推移テーブル）
        tbl_m = _re.search(r'(決算期.*?</table>)', html, _re.DOTALL)
        if not tbl_m:
            return annual
        tbl = tbl_m.group(1)
        rows = _re.findall(r'<tr[^>]*>(.*?)</tr>', tbl, _re.DOTALL)
        headers = []
        for row in rows:
            cells = _re.findall(r'<t[hd][^>]*>(.*?)</t[hd]>', row, _re.DOTALL)
            cells = [_re.sub(r'<[^>]+>', '', c).strip() for c in cells]
            if not headers:
                headers = cells
                continue
            if len(cells) < 2: continue
            row_dict = {}
            for i, h in enumerate(headers):
                row_dict[h] = cells[i] if i < len(cells) else ""
            annual.append(row_dict)
    except Exception:
        pass
    return annual


class ChatRequest(BaseModel):
    symbol: str
    name: str = ""
    messages: list  # [{"role": "user"|"assistant", "content": "..."}]
    analyst: dict = {}

@app.get("/chat/history")
def get_chat_history(symbol: str = Query(...)):
    try:
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT role, content FROM chat_history WHERE symbol=%s ORDER BY created_at ASC",
                    (symbol,)
                )
                rows = cur.fetchall()
            return [{"role": r["role"], "content": r["content"]} for r in rows]
        finally:
            conn.close()
    except Exception as e:
        raise HTTPException(500, str(e))

@app.delete("/chat/history")
def clear_chat_history(symbol: str = Query(...)):
    try:
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM chat_history WHERE symbol=%s", (symbol,))
            conn.commit()
            return {"ok": True}
        finally:
            conn.close()
    except Exception as e:
        raise HTTPException(500, str(e))

@app.post("/chat")
def chat(req: ChatRequest):
    system = f"""あなたは株式投資アシスタントです。
現在表示中の銘柄: {req.symbol} {req.name}
今日の日付: {datetime.now().strftime('%Y年%m月%d日')}
"""

    # 株価・インジケータをDBから取得（直近60本）
    try:
        import statistics as _stats
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT candle_time, open, high, low, close, volume FROM candles "
                "WHERE symbol=%s AND interval_type='1d' ORDER BY candle_time DESC LIMIT 65",
                (req.symbol,)
            )
            rows = cur.fetchall()
        conn.close()
        if rows:
            rows_asc = list(reversed(rows))  # 古い順
            closes = [float(r['close']) for r in rows_asc]
            highs  = [float(r['high'])  for r in rows_asc]
            lows   = [float(r['low'])   for r in rows_asc]
            vols   = [float(r['volume'] or 0) for r in rows_asc]

            latest = rows[0]
            curr = closes[-1]
            system += f"\n【株価情報】\n"
            system += f"直近終値: {curr:.0f}円 ({latest['candle_time']})\n"
            if len(closes) >= 2:
                prev = closes[-2]
                chg = curr - prev
                system += f"前日比: {chg:+.0f}円 ({chg/prev*100:+.2f}%)\n"

            # 移動平均
            def sma(data, n):
                return sum(data[-n:]) / n if len(data) >= n else None
            ma5  = sma(closes, 5)
            ma25 = sma(closes, 25)
            ma75 = sma(closes, 75) if len(closes) >= 75 else None
            system += f"\n【移動平均】\n"
            if ma5:  system += f"MA5: {ma5:.0f}円\n"
            if ma25: system += f"MA25: {ma25:.0f}円\n"
            if ma75: system += f"MA75: {ma75:.0f}円\n"
            if ma5 and ma25:
                system += f"MA5/MA25: {'ゴールデンクロス（短期>長期、上昇傾向）' if ma5>ma25 else 'デッドクロス（短期<長期、下降傾向）'}\n"

            # ボリンジャーバンド（25日）
            if len(closes) >= 25:
                mean25 = ma25
                std25 = _stats.stdev(closes[-25:])
                bb_upper = mean25 + 2 * std25
                bb_lower = mean25 - 2 * std25
                bb_pos = (curr - bb_lower) / (bb_upper - bb_lower) * 100 if bb_upper != bb_lower else 50
                system += f"\n【ボリンジャーバンド(25)】\n"
                system += f"上限(+2σ): {bb_upper:.0f}円 / 中央(MA25): {mean25:.0f}円 / 下限(-2σ): {bb_lower:.0f}円\n"
                system += f"現在位置: バンド内{bb_pos:.0f}%（0%=下限, 100%=上限）\n"

            # RSI（14日）
            if len(closes) >= 15:
                gains, losses = [], []
                for i in range(-14, 0):
                    d = closes[i] - closes[i-1]
                    (gains if d > 0 else losses).append(abs(d))
                avg_gain = sum(gains) / 14
                avg_loss = sum(losses) / 14
                rsi = 100 - (100 / (1 + avg_gain / avg_loss)) if avg_loss > 0 else 100
                system += f"\n【RSI(14)】\n"
                system += f"RSI: {rsi:.1f}（70以上=買われすぎ、30以下=売られすぎ）\n"

            # 52週高値・安値
            if len(closes) >= 20:
                period_highs = highs[-min(252, len(highs)):]
                period_lows  = lows[-min(252, len(lows)):]
                h52 = max(period_highs)
                l52 = min(period_lows)
                system += f"\n【価格レンジ（直近{len(period_highs)}営業日）】\n"
                system += f"高値: {h52:.0f}円 / 安値: {l52:.0f}円\n"
                system += f"現値は高値から{(h52-curr)/h52*100:.1f}%下、安値から{(curr-l52)/l52*100:.1f}%上\n"

            # 出来高（直近5日平均）
            if len(vols) >= 5:
                avg_vol5 = sum(vols[-5:]) / 5
                system += f"\n【出来高】\n直近5日平均: {avg_vol5/10000:.0f}万株\n"

            # 直近3か月の価格推移（全データ）
            system += f"\n【直近3か月の日次終値推移（{len(rows_asc)}営業日）】\n"
            for r in rows_asc:
                system += f"  {r['candle_time']}: 始{float(r['open']):.0f} 高{float(r['high']):.0f} 安{float(r['low']):.0f} 終{float(r['close']):.0f} 出来高{float(r['volume'] or 0)/10000:.0f}万\n"
    except Exception:
        pass

    # 直近ニュースをDBから取得（最新5件）
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT title_ja, title, published, source FROM news_cache WHERE symbol=%s ORDER BY published DESC LIMIT 5",
                (req.symbol,)
            )
            news_rows = cur.fetchall()
        conn.close()
        if news_rows:
            system += f"\n【直近ニュース】\n"
            for n in news_rows:
                t = n.get('title_ja') or n.get('title') or ''
                pub = n.get('published')
                date_str = ''
                if pub:
                    try:
                        date_str = datetime.fromtimestamp(pub / 1000).strftime('%m/%d') if pub > 1e10 else datetime.fromtimestamp(pub).strftime('%m/%d')
                    except Exception:
                        pass
                system += f"- ({date_str}) {t}\n"
    except Exception:
        pass

    if req.analyst:
        system += "\n【アナリスト目標株価】\n"
        if req.analyst.get("minkabu"):
            m = req.analyst["minkabu"]
            system += f"みんかぶ: {m.get('consensus_price','不明')}円 ({m.get('consensus_label','')})\n"
        if req.analyst.get("kabuyoho") and req.analyst["kabuyoho"].get("target_price"):
            system += f"株予報Pro: {req.analyst['kabuyoho']['target_price']}円\n"
        if req.analyst.get("kabuka") and req.analyst["kabuka"].get("avg"):
            system += f"kabuka.jp.net平均: {req.analyst['kabuka']['avg']}円\n"

    system += "\nユーザーの質問に日本語で答えてください。上記の株価・ニュース・アナリスト情報を積極的に活用して具体的に回答してください。投資判断はユーザー自身が行うものとし、参考情報として回答してください。"

    try:
        body = json.dumps({
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 1024,
            "system": system,
            "messages": req.messages
        })
        resp = boto3.client("bedrock-runtime", region_name="us-east-1").invoke_model(
            modelId="us.anthropic.claude-haiku-4-5-20251001-v1:0",
            body=body, contentType="application/json", accept="application/json"
        )
        text = json.loads(resp["body"].read())["content"][0]["text"]

        # ユーザー発言と返答をDBに保存
        try:
            conn = get_conn()
            user_msg = req.messages[-1] if req.messages else None
            with conn.cursor() as cur:
                if user_msg and user_msg.get("role") == "user":
                    cur.execute(
                        "INSERT INTO chat_history (symbol, role, content) VALUES (%s,%s,%s)",
                        (req.symbol, "user", user_msg["content"])
                    )
                cur.execute(
                    "INSERT INTO chat_history (symbol, role, content) VALUES (%s,%s,%s)",
                    (req.symbol, "assistant", text)
                )
            conn.commit()
            conn.close()
        except Exception:
            pass

        return {"reply": text}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/candles")
def get_candles(symbol: str = Query(...), interval: str = Query("1d")):
    if interval not in INTERVALS:
        raise HTTPException(400, "Invalid interval")
    # 日本株コード判定: 数字のみ or 4桁英数字（例: 314A, 318A）も .T を付与
    sym = f"{symbol}.T" if (symbol.isdigit() or (len(symbol) == 4 and symbol.isalnum())) else symbol
    try:
        conn = get_conn()
        try:
            # 1. yfinanceから最新データ取得
            fresh = fetch_from_yfinance(sym, interval)
            # 2. RDSに追記保存
            if fresh:
                save_candles_to_db(conn, symbol, interval, fresh)
            # 3. RDSから全データ取得して返す
            return load_candles_from_db(conn, symbol, interval)
        finally:
            conn.close()
    except Exception as e:
        raise HTTPException(500, str(e))


# ===== TREND LINES =====

class TLPoint(BaseModel):
    time: str
    value: float

class TrendLineIn(BaseModel):
    symbol: str
    interval: str
    p1: TLPoint
    p2: TLPoint

@app.get("/trendlines")
def get_trendlines(symbol: str = Query(...), interval: str = Query(...)):
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, p1_time, p1_value, p2_time, p2_value FROM trend_lines WHERE symbol=%s AND interval_type=%s ORDER BY id",
                (symbol, interval)
            )
            rows = cur.fetchall()
        conn.close()
        return [
            {"id": r["id"], "p1": {"time": r["p1_time"], "value": r["p1_value"]}, "p2": {"time": r["p2_time"], "value": r["p2_value"]}}
            for r in rows
        ]
    except Exception as e:
        raise HTTPException(500, str(e))

@app.post("/trendlines")
def add_trendline(req: TrendLineIn):
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO trend_lines (symbol, interval_type, p1_time, p1_value, p2_time, p2_value) VALUES (%s,%s,%s,%s,%s,%s)",
                (req.symbol, req.interval, req.p1.time, req.p1.value, req.p2.time, req.p2.value)
            )
            new_id = cur.lastrowid
        conn.commit()
        conn.close()
        return {"id": new_id}
    except Exception as e:
        raise HTTPException(500, str(e))

@app.delete("/trendlines/{tl_id}")
def delete_trendline(tl_id: int):
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("DELETE FROM trend_lines WHERE id=%s", (tl_id,))
        conn.commit()
        conn.close()
        return {"ok": True}
    except Exception as e:
        raise HTTPException(500, str(e))

@app.delete("/trendlines")
def delete_all_trendlines(symbol: str = Query(...), interval: str = Query(...)):
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("DELETE FROM trend_lines WHERE symbol=%s AND interval_type=%s", (symbol, interval))
        conn.commit()
        conn.close()
        return {"ok": True}
    except Exception as e:
        raise HTTPException(500, str(e))


# ===== SCREENING =====

import threading as _threading
import math as _math

_screening_status = {"running": False, "progress": 0, "total": 0, "updated_at": None, "error": None}


def _calc_screening_score(rows: list, vix_latest=None):
    """OHLCVデータからシグナルスコアを計算（チャート表示と同一ルール）"""
    if len(rows) < 35:
        return None
    closes = [float(r['close'])  for r in rows]
    highs  = [float(r['high'])   for r in rows]
    lows   = [float(r['low'])    for r in rows]
    n = len(rows)
    i = n - 1

    # ---- ヘルパー関数 ----
    def sma(arr, p, idx):
        if idx < p - 1: return None
        return sum(arr[idx-p+1:idx+1]) / p

    def tenkan(idx):
        if idx < 8: return None
        return (max(highs[idx-8:idx+1]) + min(lows[idx-8:idx+1])) / 2

    def kijun(idx):
        if idx < 25: return None
        return (max(highs[idx-25:idx+1]) + min(lows[idx-25:idx+1])) / 2

    def cloud(idx):
        ci = idx - 26
        if ci < 51: return None, None
        t = tenkan(ci); k = kijun(ci)
        if t is None or k is None: return None, None
        sa = (t + k) / 2
        sb = (max(highs[ci-51:ci+1]) + min(lows[ci-51:ci+1])) / 2
        return max(sa, sb), min(sa, sb)

    def bb_pct(idx, period=25):
        if idx < period - 1: return None
        w = closes[idx-period+1:idx+1]
        mean = sum(w) / period
        std = (sum((x-mean)**2 for x in w) / period) ** 0.5
        rng = 4 * std
        return (closes[idx] - (mean - 2*std)) / rng if rng > 0 else 0.5

    def macd_and_signal(idx):
        if idx < 33: return None, None
        def ema(vals, p):
            k = 2 / (p + 1); v = vals[0]
            for x in vals[1:]: v = x * k + v * (1 - k)
            return v
        macd_vals = []
        for j in range(idx - 8, idx + 1):
            if j < 25: return None, None
            macd_vals.append(ema(closes[j-11:j+1], 12) - ema(closes[j-25:j+1], 26))
        sig = ema(macd_vals, 9)
        return macd_vals[-1], sig

    def rsi(idx, period=14):
        if idx < period: return None
        g = l = 0.0
        for j in range(idx-period+1, idx+1):
            d = closes[j] - closes[j-1]
            if d > 0: g += d
            else: l -= d
        ag = g / period; al = l / period
        return 100 - 100/(1+ag/al) if al > 0 else 100.0

    def get_pivot_lows(idx, lookback=5):
        result = []
        for j in range(lookback, idx - lookback + 1):
            if all(lows[k] > lows[j] for k in range(j-lookback, j+lookback+1) if k != j and 0 <= k < len(lows)):
                result.append({'idx': j, 'price': lows[j]})
        return result

    def get_pivot_highs(idx, lookback=5):
        result = []
        for j in range(lookback, idx - lookback + 1):
            if all(highs[k] < highs[j] for k in range(j-lookback, j+lookback+1) if k != j and 0 <= k < len(highs)):
                result.append({'idx': j, 'price': highs[j]})
        return result

    def trendline_val(idx, lookback=5):
        """上昇ピボットローを結ぶトレンドラインの現在値"""
        if idx < lookback * 2 + 1: return None
        price = closes[idx]
        pivot_lows = get_pivot_lows(idx, lookback)
        for ii in range(len(pivot_lows) - 1, 0, -1):
            p2 = pivot_lows[ii]
            for jj in range(ii - 1, max(-1, ii - 7), -1):
                p1 = pivot_lows[jj]
                if p2['price'] <= p1['price']: continue
                slope = (p2['price'] - p1['price']) / (p2['idx'] - p1['idx'])
                valid = all(
                    pivot_lows[kk]['price'] >= (p1['price'] + slope * (pivot_lows[kk]['idx'] - p1['idx'])) * 0.997
                    for kk in range(jj + 1, ii)
                )
                if not valid: continue
                end_price = p2['price'] + slope * (idx - p2['idx'])
                if price >= end_price * 0.96:
                    return end_price
        return None

    def trendline_near(idx, thresh=0.02):
        tl = trendline_val(idx)
        if tl is None: return False
        return abs(closes[idx] - tl) / tl <= thresh

    def trendline_below(idx, thresh=0.02):
        """上昇TLが現在価格のthresh%以内の下（支持として機能）"""
        tl = trendline_val(idx)
        if tl is None: return False
        price = closes[idx]
        return tl <= price and (price - tl) / tl <= thresh

    def trendline_above(idx, thresh=0.02):
        """上昇TLが現在価格のthresh%以内の上（価格がTLを下抜け→抵抗）"""
        tl = trendline_val(idx)
        if tl is None: return False
        price = closes[idx]
        return tl > price and (tl - price) / tl <= thresh

    def support_val(idx, lookback=5):
        if idx < lookback * 2 + 1: return None
        price = closes[idx]
        raw = [p['price'] for p in get_pivot_lows(idx, lookback)
               if p['price'] <= price and p['price'] >= price * 0.80]
        if not raw: return None
        sorted_lows = sorted(raw, reverse=True)
        groups, j = [], 0
        while j < len(sorted_lows):
            group = [sorted_lows[j]]
            while j + 1 < len(sorted_lows) and abs(sorted_lows[j+1] - sorted_lows[j]) / sorted_lows[j] < 0.012:
                j += 1
                group.append(sorted_lows[j])
            groups.append({'level': sum(group) / len(group), 'strength': len(group)})
            j += 1
        groups.sort(key=lambda x: -x['strength'])
        below = [g['level'] for g in groups[:3] if g['level'] < price]
        return max(below) if below else None

    def resistance_val(idx, lookback=5):
        if idx < lookback * 2 + 1: return None
        price = closes[idx]
        raw = [p['price'] for p in get_pivot_highs(idx, lookback)
               if price < p['price'] <= price * 1.20]
        if not raw: return None
        sorted_highs = sorted(raw)
        groups, j = [], 0
        while j < len(sorted_highs):
            group = [sorted_highs[j]]
            while j + 1 < len(sorted_highs) and abs(sorted_highs[j+1] - sorted_highs[j]) / sorted_highs[j] < 0.012:
                j += 1
                group.append(sorted_highs[j])
            groups.append({'level': sum(group) / len(group), 'strength': len(group)})
            j += 1
        groups.sort(key=lambda x: -x['strength'])
        above = [g['level'] for g in groups[:3] if g['level'] > price]
        return min(above) if above else None

    def support_nearby(idx, thresh=0.02):
        sv = support_val(idx)
        if sv is None: return False
        price = closes[idx]
        return price * (1 - thresh) <= sv < price

    def resistance_nearby(idx, thresh=0.02):
        rv = resistance_val(idx)
        if rv is None: return False
        price = closes[idx]
        return price < rv <= price * (1 + thresh)

    def bb_walk_down(idx, n=3):
        if idx < n - 1 + 24: return False
        return all((bb_pct(idx - j) or 1) <= 0.2 for j in range(n))

    def _slope(arr):
        mn = len(arr)
        if mn < 2: return 0.0
        mean_x = (mn - 1) / 2
        mean_y = sum(arr) / mn
        num = sum((j - mean_x) * (arr[j] - mean_y) for j in range(mn))
        den = sum((j - mean_x) ** 2 for j in range(mn))
        return (num / den) / (mean_y or 1) if den else 0.0

    # ---- 判定 ----
    buy_sigs  = []
    sell_sigs = []

    ma5  = sma(closes, 5,  i);  ma5p  = sma(closes, 5,  i-1)
    ma25 = sma(closes, 25, i);  ma25p = sma(closes, 25, i-1)
    ct, cb = cloud(i)
    below_cloud = ct and cb and closes[i] < cb and ma5 and ma25 and ma5 < ma25

    # TL: 上昇トレンドライン近傍
    if trendline_near(i):
        buy_sigs.append("TL")
    # 支持線近傍: ピボット安値支持線が2%以内の下
    if support_nearby(i):
        buy_sigs.append("支持線近傍")
    # TL下支持: 上昇TLが2%以内の下で支持
    if trendline_below(i):
        buy_sigs.append("TL下支持")

    # GC / DC
    if None not in (ma5, ma25, ma5p, ma25p):
        if ma5p <= ma25p and ma5 > ma25:   buy_sigs.append("GC")
        elif ma5p >= ma25p and ma5 < ma25: sell_sigs.append("DC")

    # ボリンジャーバンド (25日)
    bbp = bb_pct(i)
    if bbp is not None:
        if bbp <= 0.3 and closes[i] > closes[i-1] and not below_cloud:
            buy_sigs.append("BB反転")
        if i >= 3 and all((bb_pct(i-j) or 0) >= 0.8 for j in range(3)):
            buy_sigs.append("BBウォーク")
        if bb_walk_down(i):
            sell_sigs.append("BBウォーク↓")

    # 抵抗ブレイク / 抵抗手前 / 支持反転 / 支持下抜け (直近20本)
    if i >= 20:
        h20 = max(highs[i-20:i]); l20 = min(lows[i-20:i])
        if closes[i] > h20:
            buy_sigs.append("抵抗ブレイク")
        elif closes[i] >= h20 * 0.98:
            sell_sigs.append("抵抗手前")
        near_support = abs(lows[i] - l20) / l20 <= 0.015
        if near_support and closes[i] > closes[i-1] and not below_cloud:
            buy_sigs.append("支持反転")
        if closes[i] < l20 and closes[i-1] >= l20:
            sell_sigs.append("支持下抜け")

    # 抵抗線近傍 / TL上抵抗
    if resistance_nearby(i):
        sell_sigs.append("抵抗線近傍")
    if trendline_above(i):
        sell_sigs.append("TL上抵抗")

    # RSI (14)
    rv = rsi(i)
    if rv is not None:
        if rv <= 40:  buy_sigs.append("RSI低")
        elif rv >= 70: sell_sigs.append("RSI高")

    # MACD (12,26,9)
    mc, ms = macd_and_signal(i)
    mcp, msp = macd_and_signal(i-1)
    if None not in (mc, ms, mcp, msp):
        if mcp <= msp and mc > ms:  buy_sigs.append("MACD↑")
        elif mcp >= msp and mc < ms: sell_sigs.append("MACD↓")

    # 一目均衡表
    if i >= 52:
        tk = tenkan(i); kj = kijun(i); tkp = tenkan(i-1); kjp = kijun(i-1)
        if None not in (tk, kj, tkp, kjp):
            if tkp <= kjp and tk > kj:  buy_sigs.append("IK↑")
            elif tkp >= kjp and tk < kj: sell_sigs.append("IK↓")
        if None not in (tk, kj, ct, cb) and tk > kj and closes[i] > ct and closes[i] > closes[i-26]:
            buy_sigs.append("IK3")
        if ct and cb:
            pp = closes[i-1]
            if pp >= ct and closes[i] < ct and closes[i] >= cb: sell_sigs.append("雲侵入")
            elif pp >= cb and closes[i] < cb:                    sell_sigs.append("雲下抜け")

    # VIX
    if vix_latest is not None:
        if vix_latest <= 17:  buy_sigs.append("VIX低")
        elif vix_latest >= 20: sell_sigs.append("VIX高")

    # ===== チャートパターン =====
    if i >= 2:
        c = rows[i]; p = rows[i-1]; p2 = rows[i-2]
        co, ch, cl, cc = float(c['open']), float(c['high']), float(c['low']), float(c['close'])
        po, ph, pl, pc = float(p['open']), float(p['high']), float(p['low']), float(p['close'])
        p2o, p2c = float(p2['open']), float(p2['close'])
        body = abs(cc - co); rng = ch - cl
        if rng > 0:
            lw = min(co, cc) - cl; uw = ch - max(co, cc)
            ma5_now = sma(closes, 5, i); ma5_prev = sma(closes, 5, max(0, i-5))
            up_trend = ma5_now and ma5_prev and ma5_now > ma5_prev
            dn_trend = ma5_now and ma5_prev and ma5_now < ma5_prev
            if dn_trend and lw >= 0.55*rng and uw <= 0.15*rng and body <= 0.3*rng:
                buy_sigs.append("ハンマー")
            if up_trend and lw >= 0.55*rng and uw <= 0.15*rng and body <= 0.3*rng:
                sell_sigs.append("トンカチ")
            if dn_trend and uw >= 0.55*rng and lw <= 0.15*rng and body <= 0.3*rng:
                buy_sigs.append("逆ハンマー")
        if pc < po and cc > co and co <= pc and cc >= po:
            buy_sigs.append("陽の包み足")
        if pc > po and cc < co and co >= pc and cc <= po:
            sell_sigs.append("陰の包み足")
        if (p2c > p2o and pc > po and cc > co and pc > p2c and cc > pc and po >= p2o and co >= po):
            buy_sigs.append("三白兵")
        if (p2c < p2o and pc < po and cc < co and pc < p2c and cc < pc and po <= p2o and co <= po):
            sell_sigs.append("三羽烏")

    if i >= 15:
        lb = min(40, i); seg_h = highs[i-lb:i+1]; seg_l = lows[i-lb:i+1]
        sn = len(seg_h)
        lhighs = []; llows = []
        for j in range(2, sn-2):
            if seg_h[j] > seg_h[j-1] and seg_h[j] > seg_h[j-2] and seg_h[j] > seg_h[j+1] and seg_h[j] > seg_h[j+2]:
                lhighs.append((j, seg_h[j]))
            if seg_l[j] < seg_l[j-1] and seg_l[j] < seg_l[j-2] and seg_l[j] < seg_l[j+1] and seg_l[j] < seg_l[j+2]:
                llows.append((j, seg_l[j]))
        if len(lhighs) >= 2:
            (j1,h1),(j2,h2) = lhighs[-2], lhighs[-1]
            if j2-j1 >= 5 and abs(h1-h2)/h1 < 0.03 and sn-1 > j2:
                neck = min(seg_l[j1:j2+1])
                if closes[i] < neck:
                    sell_sigs.append("ダブルトップ")
        if len(llows) >= 2:
            (j1,l1),(j2,l2) = llows[-2], llows[-1]
            if j2-j1 >= 5 and abs(l1-l2)/l1 < 0.03 and sn-1 > j2:
                neck = max(seg_h[j1:j2+1])
                if closes[i] > neck:
                    buy_sigs.append("ダブルボトム")

    if i >= 20:
        lb = min(60, i); seg_h = highs[i-lb:i+1]; seg_l = lows[i-lb:i+1]; sn = len(seg_h)
        pks = []; trs = []
        for j in range(3, sn-3):
            w_h = seg_h[j-3:j+4]; w_l = seg_l[j-3:j+4]
            if seg_h[j] == max(w_h): pks.append((j, seg_h[j]))
            if seg_l[j] == min(w_l): trs.append((j, seg_l[j]))
        if len(pks) >= 3:
            (lsi,lsv),(hdi,hdv),(rsi2,rsv) = pks[-3], pks[-2], pks[-1]
            if hdv > lsv*1.02 and hdv > rsv*1.02 and abs(lsv-rsv)/lsv < 0.05 and rsi2-lsi >= 10:
                n1 = min(seg_l[lsi:hdi+1]); n2 = min(seg_l[hdi:rsi2+1])
                if closes[i] < (n1+n2)/2:
                    sell_sigs.append("H&S")
        if len(trs) >= 3:
            (lsi,lsv),(hdi,hdv),(rsi2,rsv) = trs[-3], trs[-2], trs[-1]
            if hdv < lsv*0.98 and hdv < rsv*0.98 and abs(lsv-rsv)/lsv < 0.05 and rsi2-lsi >= 10:
                n1 = max(seg_h[lsi:hdi+1]); n2 = max(seg_h[hdi:rsi2+1])
                if closes[i] > (n1+n2)/2:
                    buy_sigs.append("逆H&S")

    if i >= 14:
        p_len = 8; f_len = min(6, i - p_len)
        if f_len >= 3:
            ps2 = i - p_len - f_len; pe = i - f_len
            if ps2 >= 0:
                pm = (closes[pe] - closes[ps2]) / closes[ps2] if closes[ps2] else 0
                fbars_h = highs[pe:i+1]; fbars_l = lows[pe:i+1]; fbars_c = closes[pe:i+1]
                fh = max(fbars_h); fl = min(fbars_l)
                fsl = (fbars_c[-1] - fbars_c[0]) / fbars_c[0] if fbars_c[0] else 0
                fr = (fh - fl) / fl if fl else 0
                if pm > 0.04 and fr < pm*0.6 and fsl < 0 and fsl > -0.04 and closes[i] > fh:
                    buy_sigs.append("上昇フラッグ")
                if pm < -0.04 and fr < abs(pm)*0.6 and fsl > 0 and fsl < 0.04 and closes[i] < fl:
                    sell_sigs.append("下降フラッグ")

    if i >= 20:
        c_len = min(25, i-1); seg_h = highs[i-c_len:i]; seg_l = lows[i-c_len:i]
        mx_h = max(seg_h); mn_l = min(seg_l)
        rng2 = (mx_h - mn_l) / mn_l if mn_l else 0
        if rng2 < 0.07:
            if closes[i] > mx_h * 1.003: buy_sigs.append("レクタングル上抜け")
            elif closes[i] < mn_l * 0.997: sell_sigs.append("レクタングル下抜け")
        rs2 = seg_h[-12:]; ls2_arr = seg_l[-12:]
        if len(rs2) >= 8:
            hs2 = _slope(list(rs2)); ls2_v = _slope(list(ls2_arr))
            if abs(hs2) < 0.003 and ls2_v > 0.003 and closes[i] > mx_h:
                buy_sigs.append("上昇三角形")
            if hs2 < -0.003 and abs(ls2_v) < 0.003 and closes[i] < mn_l:
                sell_sigs.append("下降三角形")
            if hs2 < -0.002 and ls2_v > 0.002:
                if closes[i] > mx_h: buy_sigs.append("対称三角形↑")
                if closes[i] < mn_l: sell_sigs.append("対称三角形↓")
            if hs2 > 0.001 and ls2_v > hs2*1.3 and closes[i] < mn_l:
                sell_sigs.append("上昇ウェッジ")
            if hs2 < -0.001 and ls2_v < 0 and hs2 < ls2_v*1.3 and closes[i] > mx_h:
                buy_sigs.append("下降ウェッジ")

    if i >= 30:
        c_len = min(35, i-5)
        if c_len >= 15:
            cup = rows[i-c_len:i-3]; hdl_l = lows[i-3:i+1]; hdl_c = closes[i-3:i+1]
            cup_c = [float(r['close']) for r in cup]; cup_l = [float(r['low']) for r in cup]
            cleft  = sum(cup_c[:5]) / 5; cright = sum(cup_c[-5:]) / 5
            cbotm  = min(cup_l)
            base   = min(cleft, cright)
            depth  = (base - cbotm) / base if base else 0
            if 0.05 < depth < 0.4 and abs(cleft-cright)/cleft < 0.05:
                res = max(cleft, cright)
                if min(hdl_l) > cbotm and closes[i] > res:
                    buy_sigs.append("C&H")

    chg = (closes[i] - closes[i-1]) / closes[i-1] * 100
    if chg >= 4:    buy_sigs.append("急騰+4%")
    elif chg >= 2:  buy_sigs.append("急騰+2%")
    elif chg <= -4: sell_sigs.append("急落-4%")
    elif chg <= -2: sell_sigs.append("急落-2%")

    # パターン系は2pt、その他1pt
    _pattern_buy  = {"急騰+4%","ハンマー","逆ハンマー","陽の包み足","三白兵",
                     "ダブルボトム","逆H&S","上昇フラッグ","レクタングル上抜け",
                     "上昇三角形","対称三角形↑","下降ウェッジ","C&H"}
    _pattern_sell = {"急落-4%","BBウォーク↓","雲下抜け",
                     "トンカチ","陰の包み足","三羽烏",
                     "ダブルトップ","H&S","下降フラッグ","レクタングル下抜け",
                     "下降三角形","対称三角形↓","上昇ウェッジ"}
    buy_score  = sum(2 if s in _pattern_buy  else 1 for s in buy_sigs)
    sell_score = sum(2 if s in _pattern_sell else 1 for s in sell_sigs)

    # 5日平均出来高
    volumes = [float(r.get('volume') or 0) for r in rows]
    vol5 = int(sum(volumes[max(0,i-4):i+1]) / min(5, i+1)) if volumes else 0

    # 20日HV（年率%）
    hv = None
    if n >= 21:
        log_rets = [_math.log(closes[j] / closes[j-1]) for j in range(max(1,i-19), i+1) if closes[j-1] > 0]
        if len(log_rets) >= 2:
            mean_r = sum(log_rets) / len(log_rets)
            var_r  = sum((x - mean_r)**2 for x in log_rets) / (len(log_rets) - 1)
            hv = round(_math.sqrt(var_r * 252) * 100, 1)

    return {
        "buy_score":    buy_score,
        "sell_score":   sell_score,
        "net_score":    buy_score - sell_score,
        "buy_signals":  buy_sigs,
        "sell_signals": sell_sigs,
        "close":        closes[-1],
        "change_pct":   (closes[-1]-closes[-2])/closes[-2]*100 if len(closes)>=2 else 0,
        "volume_avg":   vol5,
        "hv":           hv,
    }


def _fetch_prime_stocks(conn) -> list:
    """JPX無料CSVからプライム市場銘柄を取得、DBにキャッシュ"""
    import io
    try:
        url = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"
        resp = _requests.get(url, timeout=30)
        resp.raise_for_status()
        df = pd.read_excel(io.BytesIO(resp.content), engine="xlrd")
        prime = df[df.iloc[:, 3].astype(str).str.contains("プライム", na=False)]
        stocks = []
        for _, row in prime.iterrows():
            code = str(row.iloc[1]).strip().zfill(4)
            name = str(row.iloc[2]).strip()
            sector = str(row.iloc[5]).strip() if len(row) > 5 else ""
            stocks.append({"code": code, "name": name, "sector": sector})
        with conn.cursor() as cur:
            for s in stocks:
                cur.execute(
                    "INSERT INTO prime_stocks (code,name,sector) VALUES (%s,%s,%s) "
                    "ON DUPLICATE KEY UPDATE name=%s, sector=%s",
                    (s["code"], s["name"], s["sector"], s["name"], s["sector"])
                )
        conn.commit()
        return stocks
    except Exception as e:
        with conn.cursor() as cur:
            cur.execute("SELECT code, name, sector FROM prime_stocks")
            rows = cur.fetchall()
        if rows:
            return [{"code": r["code"], "name": r["name"], "sector": r["sector"]} for r in rows]
        raise Exception(f"JPXデータ取得失敗: {e}")


def _run_screening_update():
    global _screening_status
    _screening_status.update({"running": True, "progress": 0, "total": 0, "error": None})
    try:
        from datetime import datetime as _dt3, timedelta as _td

        conn = get_conn()
        stocks = _fetch_prime_stocks(conn)
        codes = [s["code"] for s in stocks]
        stock_map = {s["code"]: s for s in stocks}
        symbols_t = [f"{c}.T" for c in codes]

        with conn.cursor() as cur:
            cur.execute(
                "SELECT symbol, MAX(candle_time) as latest FROM candles "
                "WHERE interval_type='1d' AND symbol IN %s GROUP BY symbol",
                (tuple(symbols_t),)
            ) if symbols_t else None
            latest_map = {r["symbol"]: str(r["latest"]) for r in (cur.fetchall() if symbols_t else [])}
        conn.close()

        today_str = _dt3.now().strftime("%Y-%m-%d")
        stale_codes = []; new_codes = []; fresh_count = 0

        for code in codes:
            symbol_t = f"{code}.T"
            latest = latest_map.get(symbol_t)
            if latest and latest >= today_str:
                fresh_count += 1
            elif latest:
                start = (_dt3.strptime(latest, "%Y-%m-%d") + _td(days=1)).strftime("%Y-%m-%d")
                stale_codes.append((code, start))
            else:
                new_codes.append(code)

        _screening_status["skipped"] = fresh_count
        fetch_count = len(stale_codes) + len(new_codes)
        _screening_status["total"] = fetch_count + len(codes)
        candle_data = {}
        batch_size = 100
        done = 0

        for b in range(0, len(stale_codes), batch_size):
            batch = stale_codes[b:b+batch_size]
            batch_codes = [c for c, _ in batch]; batch_starts = [s for _, s in batch]
            tickers = [f"{c}.T" for c in batch_codes]
            min_start = min(batch_starts)
            try:
                raw = yf.download(tickers, start=min_start, end=today_str,
                                  interval="1d", group_by="ticker",
                                  progress=False, threads=True, auto_adjust=True)
                for code, ticker in zip(batch_codes, tickers):
                    try:
                        df_t = raw if len(tickers) == 1 else (
                            raw[ticker] if ticker in raw.columns.get_level_values(0) else pd.DataFrame()
                        )
                        if df_t is None or df_t.empty: continue
                        df_t = df_t.dropna(subset=["Close"])
                        rows_yf = [{"candle_time": str(dt.date()),
                                    "open": float(row["Open"]), "high": float(row["High"]),
                                    "low": float(row["Low"]), "close": float(row["Close"]),
                                    "volume": float(row.get("Volume", 0) or 0)}
                                   for dt, row in df_t.iterrows()]
                        if rows_yf: candle_data[f"{code}.T"] = rows_yf
                    except Exception: pass
            except Exception: pass
            done += len(batch)
            _screening_status["progress"] = done

        for b in range(0, len(new_codes), batch_size):
            batch = new_codes[b:b+batch_size]
            tickers = [f"{c}.T" for c in batch]
            try:
                raw = yf.download(tickers, period="3mo", interval="1d",
                                  group_by="ticker", progress=False, threads=True, auto_adjust=True)
                for code, ticker in zip(batch, tickers):
                    try:
                        df_t = raw if len(tickers) == 1 else (
                            raw[ticker] if ticker in raw.columns.get_level_values(0) else pd.DataFrame()
                        )
                        if df_t is None or df_t.empty: continue
                        df_t = df_t.dropna(subset=["Close"])
                        rows_yf = [{"candle_time": str(dt.date()),
                                    "open": float(row["Open"]), "high": float(row["High"]),
                                    "low": float(row["Low"]), "close": float(row["Close"]),
                                    "volume": float(row.get("Volume", 0) or 0)}
                                   for dt, row in df_t.iterrows()]
                        if rows_yf: candle_data[f"{code}.T"] = rows_yf
                    except Exception: pass
            except Exception: pass
            done += len(batch)
            _screening_status["progress"] = done

        vix_latest = None
        vix_by_date_screen = {}
        for _vix_attempt in range(3):
            try:
                vix_df = yf.Ticker("^VIX").history(period="6mo")
                if not vix_df.empty:
                    vix_latest = float(vix_df["Close"].iloc[-1])
                    for _vi, _vr in vix_df.iterrows():
                        vix_by_date_screen[str(_vi)[:10]] = round(float(_vr['Close']), 1)
                    break
            except Exception: pass

        conn = get_conn()
        for code, new_rows in candle_data.items():
            try:
                with conn.cursor() as cur:
                    for r in new_rows:
                        cur.execute(
                            "INSERT INTO candles (symbol,interval_type,candle_time,open,high,low,close,volume) "
                            "VALUES (%s,'1d',%s,%s,%s,%s,%s,%s) "
                            "ON DUPLICATE KEY UPDATE open=%s,high=%s,low=%s,close=%s,volume=%s",
                            (code, r["candle_time"],
                             r["open"], r["high"], r["low"], r["close"], r["volume"],
                             r["open"], r["high"], r["low"], r["close"], r["volume"])
                        )
                conn.commit()
            except Exception: pass

        _screening_status["progress"] = fetch_count
        from datetime import datetime as _dt3
        for idx, code in enumerate(codes):
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT candle_time,open,high,low,close,volume FROM candles "
                        "WHERE symbol=%s AND interval_type='1d' "
                        "ORDER BY candle_time DESC LIMIT 65",
                        (f"{code}.T",)
                    )
                    db_rows = list(reversed(cur.fetchall()))
                if not db_rows: continue
                score = _calc_screening_score(db_rows, vix_latest=vix_latest)
                if score:
                    s = stock_map.get(code, {})

                    # 最終シグナルトリガー日のスコアを取得
                    candles_for_sig = [
                        {"time": r["candle_time"], "open": float(r["open"] or 0),
                         "high": float(r["high"] or 0), "low": float(r["low"] or 0),
                         "close": float(r["close"] or 0), "volume": r["volume"] or 0}
                        for r in db_rows
                    ]
                    sig_result = generate_rule_signals(
                        f"{code}.T", "1d",
                        _candles=candles_for_sig,
                        _vix_by_date=vix_by_date_screen
                    )
                    last_sig = sig_result.get("signals", [])[-1] if sig_result.get("signals") else None
                    if last_sig:
                        sig_date   = str(last_sig["time"])[:10]
                        sig_side   = last_sig["side"]
                        day_scores = sig_result.get("scores", {}).get(sig_date, {})
                        last_sig_buy  = day_scores.get("buy",  0)
                        last_sig_sell = day_scores.get("sell", 0)
                    else:
                        sig_date = sig_side = None
                        last_sig_buy = last_sig_sell = None

                    with conn.cursor() as cur:
                        cur.execute(
                            "INSERT INTO screening_cache "
                            "(code,name,sector,buy_score,sell_score,net_score,close_price,change_pct,"
                            "buy_signals,sell_signals,volume_avg,hv,"
                            "last_signal_side,last_signal_date,last_signal_buy_score,last_signal_sell_score) "
                            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                            "ON DUPLICATE KEY UPDATE name=%s,sector=%s,buy_score=%s,sell_score=%s,net_score=%s,"
                            "close_price=%s,change_pct=%s,buy_signals=%s,sell_signals=%s,"
                            "volume_avg=%s,hv=%s,"
                            "last_signal_side=%s,last_signal_date=%s,"
                            "last_signal_buy_score=%s,last_signal_sell_score=%s,updated_at=NOW()",
                            (code, s.get("name",""), s.get("sector",""),
                             score["buy_score"], score["sell_score"], score["net_score"],
                             score["close"], score["change_pct"],
                             json.dumps(score["buy_signals"], ensure_ascii=False),
                             json.dumps(score["sell_signals"], ensure_ascii=False),
                             score["volume_avg"], score["hv"],
                             sig_side, sig_date, last_sig_buy, last_sig_sell,
                             s.get("name",""), s.get("sector",""),
                             score["buy_score"], score["sell_score"], score["net_score"],
                             score["close"], score["change_pct"],
                             json.dumps(score["buy_signals"], ensure_ascii=False),
                             json.dumps(score["sell_signals"], ensure_ascii=False),
                             score["volume_avg"], score["hv"],
                             sig_side, sig_date, last_sig_buy, last_sig_sell)
                        )
                    conn.commit()
                if idx % 50 == 0:
                    _screening_status["progress"] = fetch_count + idx
            except Exception: pass

        _screening_status["progress"] = fetch_count + len(codes)
        conn.close()
        _screening_status.update({"running": False,
                                   "updated_at": _dt3.now().strftime("%Y-%m-%d %H:%M"),
                                   "error": None})
    except Exception as e:
        _screening_status.update({"running": False, "error": str(e)})


@app.get("/screening/status")
def screening_status():
    return _screening_status


@app.post("/screening/update")
def screening_update():
    if _screening_status["running"]:
        return {"message": "already running"}
    t = _threading.Thread(target=_run_screening_update, daemon=True)
    t.start()
    return {"message": "started"}


@app.get("/screening")
def get_screening(
    min_score: int = Query(1),
    sector: str = Query(""),
    sort: str = Query("net_score"),
):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM screening_cache")
            rows = cur.fetchall()
    finally:
        conn.close()

    results = []
    for r in rows:
        if r["net_score"] < min_score:
            continue
        if sector and sector not in (r["sector"] or ""):
            continue
        try:
            buy_sigs  = json.loads(r["buy_signals"]  or "[]")
            sell_sigs = json.loads(r["sell_signals"] or "[]")
        except Exception:
            buy_sigs = sell_sigs = []
        results.append({
            "code":        r["code"],
            "name":        r["name"],
            "sector":      r["sector"],
            "buy_score":   r["buy_score"],
            "sell_score":  r["sell_score"],
            "net_score":   r["net_score"],
            "close":       r["close_price"],
            "change_pct":  r["change_pct"],
            "buy_signals": buy_sigs,
            "sell_signals": sell_sigs,
            "volume_avg":  r.get("volume_avg"),
            "hv":          r.get("hv"),
            "updated_at":  str(r["updated_at"]),
            "last_signal_side":       r.get("last_signal_side"),
            "last_signal_date":       r.get("last_signal_date"),
            "last_signal_buy_score":  r.get("last_signal_buy_score"),
            "last_signal_sell_score": r.get("last_signal_sell_score"),
        })

    results.sort(key=lambda x: (x.get(sort) or 0), reverse=True)
    return results


@app.get("/screening/sectors")
def get_sectors():
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT sector FROM prime_stocks ORDER BY sector")
            rows = cur.fetchall()
    finally:
        conn.close()
    return [r["sector"] for r in rows if r["sector"]]
