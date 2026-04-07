from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List
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
    conn.commit()
    conn.close()


@app.on_event("startup")
def startup():
    init_db()


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

def fetch_from_yfinance(sym: str, interval: str) -> list:
    cfg = INTERVALS[interval]
    df = yf.Ticker(sym).history(period=cfg["period"], interval=interval, auto_adjust=True)
    if df.empty:
        return []
    result = []
    for ts, row in df.iterrows():
        if cfg["is_date"]:
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
    return result


def save_candles_to_db(conn, symbol: str, interval: str, candles: list):
    with conn.cursor() as cur:
        for c in candles:
            try:
                cur.execute(
                    """INSERT IGNORE INTO candles
                       (symbol, interval_type, candle_time, open, high, low, close, volume)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                    (symbol, interval, str(c["time"]),
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


def generate_rule_signals(symbol: str, interval: str) -> list:
    """ポイントスコアリング式ルールベースシグナル生成
    買い観点（各1pt）:
      TL=トレンドライン近傍, GC=ゴールデンクロス, IK3=一目三役好転,
      BB反転=BB下限から反転上昇, BBウォーク=BBバンドウォーク,
      抵抗ブレイク=直近高値ブレイク, 支持反転=直近安値で反転
    売り観点: DC=デッドクロス, BB↑=BB上限到達, IK↓=一目下抜け
    """
    try:
        from datetime import datetime as _dt, timezone, timedelta
        conn = get_conn()
        candles = load_candles_from_db(conn, symbol, interval)
        conn.close()
        if len(candles) < 30:
            return []

        closes = [c['close'] for c in candles]
        highs  = [c.get('high', c['close']) for c in candles]
        lows   = [c.get('low',  c['close']) for c in candles]

        def get_date(c):
            t = c['time']
            if isinstance(t, str): return t[:10]
            return _dt.fromtimestamp(int(t), tz=timezone.utc).strftime('%Y-%m-%d')

        # VIXデータ取得
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

        def _trendline_near(i, window=20, thresh=0.02):
            """直近安値のトレンドライン近傍（価格がトレンドライン±thresh%以内）"""
            if i < window: return False
            lw = lows[i-window:i]
            # 単純線形回帰でトレンドライン
            n = len(lw)
            xs = list(range(n))
            mx = sum(xs) / n
            my = sum(lw) / n
            denom = sum((x - mx)**2 for x in xs)
            if denom == 0: return False
            slope = sum((xs[j] - mx) * (lw[j] - my) for j in range(n)) / denom
            intercept = my - slope * mx
            tl_val = slope * n + intercept  # 現在位置のトレンドライン値
            return abs(closes[i] - tl_val) / tl_val <= thresh

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
            start = max(0, i - 19)
            recent_low = min(lows[start:i+1])
            sl = round(recent_low * 0.98, 1)
            sl = max(sl, round(price * 0.85, 1))
            sl = min(sl, round(price * 0.95, 1))
            return sl

        # 最低スコア閾値: 3pt以上で買いシグナル表示
        BUY_THRESHOLD = 3
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

            if _trendline_near(i):
                buy_tags.append("TL")
            if ma5_c and ma25_c and ma5_p and ma25_p and ma5_p <= ma25_p and ma5_c > ma25_c:
                buy_tags.append("GC")
            if _ik3_buy(i):
                buy_tags.append("IK3")
            if bb_c is not None and bb_c <= 30 and closes[i] > closes[i-1]:
                buy_tags.append("BB反転")
            if _bb_walk(i):
                buy_tags.append("BBウォーク")
            if _resistance_break(i):
                buy_tags.append("抵抗ブレイク")
            if _support_bounce(i):
                buy_tags.append("支持反転")
            if ik == "上抜け":
                buy_tags.append("IK↑")
            # VIX≤17: 安定水準
            if vix is not None and vix <= 17:
                buy_tags.append("VIX低")

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

            # 全日付のポイントを記録
            scores[date_key] = {"buy": len(buy_tags), "sell": sell_pts}

            # 買いシグナル: 0pt経由 または 直前に売りシグナルが出た後に3pt以上で許可
            buy_pt = len(buy_tags)
            if buy_pt == 0:
                buy_reset = True
            if buy_pt >= BUY_THRESHOLD and buy_reset:
                score_str = f"[+{buy_pt}pt]"
                signals.append({
                    "time":      date_key,
                    "side":      "buy",
                    "price":     round(price, 1),
                    "reason":    score_str + "・".join(buy_tags),
                    "stop_loss": stop_loss(i, price),
                })
                buy_reset = False

            # 売りシグナル（-2pt以下）: 0pt経由 または 直前に買いシグナルが出た後で許可
            if sell_pts == 0:
                sell_reset = True
            if sell_pts >= 2 and sell_reset:
                score_str = f"[-{sell_pts}pt]"
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

        # Yahoo Finance Japan RSS（銘柄別）
        try:
            code_only = symbol.replace(".T", "")
            feed = feedparser.parse(f"https://finance.yahoo.co.jp/rss/company/{code_only}")
            for entry in feed.entries[:15]:
                url = entry.get("link", "")
                if not url:
                    continue
                published = None
                if hasattr(entry, 'published_parsed') and entry.published_parsed:
                    published = cal_mod.timegm(entry.published_parsed) * 1000
                summary_raw = entry.get("summary", "")
                summary_ja = re2.sub('<[^>]+>', '', summary_raw)[:300] if summary_raw else None
                new_items.append({
                    "url": url,
                    "title": entry.get("title", ""),
                    "published": published,
                    "source": "Yahoo!ファイナンス",
                    "summary_ja": summary_ja,
                    "title_ja": entry.get("title", ""),
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
                    modelId="us.anthropic.claude-3-haiku-20240307-v1:0", body=body,
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

        # DBから直近1週間のデータを返す
        with conn.cursor() as cur:
            cur.execute(
                "SELECT url, title, title_ja, published, source, summary_ja FROM news_cache WHERE symbol=%s AND published >= %s ORDER BY published DESC LIMIT 30",
                (symbol, one_week_ago_ms)
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


@app.get("/financials")
def get_financials(symbol: str = Query(...)):
    """年次・四半期財務データ（売上・利益・前年比・将来予測）"""
    sym = f"{symbol}.T" if (symbol.isdigit() or (len(symbol) == 4 and symbol.isalnum())) else symbol

    EXACT_KEYS = {
        "revenue": ["Total Revenue", "Operating Revenue"],
        "gross":   ["Gross Profit"],
        "op_inc":  ["Operating Income", "Total Operating Income As Reported", "EBIT"],
        "net_inc": ["Net Income", "Net Income Common Stockholders",
                    "Net Income From Continuing Operation Net Minority Interest"],
    }

    def safe_exact(df, field, col):
        for key in EXACT_KEYS[field]:
            if key in df.index:
                try:
                    v = df.loc[key, col]
                    if pd.notna(v): return float(v)
                except Exception:
                    pass
        return None

    def build_rows(df, max_cols):
        cols = list(df.columns)[:max_cols]
        rows = []
        for i, col in enumerate(cols):
            revenue = safe_exact(df, "revenue", col)
            gross   = safe_exact(df, "gross",   col)
            op_inc  = safe_exact(df, "op_inc",  col)
            net_inc = safe_exact(df, "net_inc", col)
            rev_yoy = net_yoy = None
            if i + 1 < len(cols):
                prev  = cols[i + 1]
                rev_p = safe_exact(df, "revenue", prev)
                net_p = safe_exact(df, "net_inc", prev)
                if rev_p and rev_p != 0 and revenue is not None:
                    rev_yoy = round((revenue - rev_p) / abs(rev_p) * 100, 1)
                if net_p and net_p != 0 and net_inc is not None:
                    net_yoy = round((net_inc - net_p) / abs(net_p) * 100, 1)
            rows.append({
                "period": str(col)[:10], "revenue": revenue, "gross": gross,
                "op_inc": op_inc, "net_inc": net_inc,
                "rev_yoy": rev_yoy, "net_yoy": net_yoy, "is_estimate": False,
            })
        return rows

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT data, updated_at FROM financials_cache WHERE symbol=%s", (symbol,))
            row = cur.fetchone()
            if row and (datetime.utcnow() - row["updated_at"]).total_seconds() < 21600:
                return json.loads(row["data"])

        t = yf.Ticker(sym)
        result = {"annual": [], "quarterly": [], "estimates": [], "currency": ""}
        try:
            result["currency"] = (t.info or {}).get("currency", "")
        except Exception:
            pass

        try:
            af = t.income_stmt
            if af is None or af.empty: af = t.financials
            if af is not None and not af.empty:
                result["annual"] = build_rows(af, 5)
        except Exception:
            pass

        try:
            qf = t.quarterly_income_stmt
            if qf is None or qf.empty: qf = t.quarterly_financials
            if qf is not None and not qf.empty:
                result["quarterly"] = build_rows(qf, 12)
        except Exception:
            pass

        try:
            re_df = t.revenue_estimate
            ee_df = t.earnings_estimate
            if re_df is not None and not re_df.empty:
                for idx, erow in re_df.iterrows():
                    rev_avg = None
                    net_avg = None
                    try:
                        v = erow.get("avg") if hasattr(erow, "get") else erow.iloc[0]
                        if pd.notna(v): rev_avg = float(v)
                    except Exception:
                        pass
                    try:
                        if ee_df is not None and not ee_df.empty and idx in ee_df.index:
                            er = ee_df.loc[idx]
                            v2 = er.get("avg") if hasattr(er, "get") else er.iloc[0]
                            if pd.notna(v2): net_avg = float(v2)
                    except Exception:
                        pass
                    result["estimates"].append({
                        "period": str(idx), "revenue": rev_avg, "net_inc": net_avg,
                        "gross": None, "op_inc": None, "rev_yoy": None, "net_yoy": None,
                        "is_estimate": True,
                    })
        except Exception:
            pass

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
