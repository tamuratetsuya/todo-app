from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List
import yfinance as yf
import pandas as pd
import pymysql
import os
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


@app.get("/analyze")
def analyze_trades():
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT trade_date, code, name, side, qty, price, settlement FROM trades ORDER BY trade_date")
            rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        return {"analysis": "トレードデータがありません。CSVをインポートしてください。"}

    # Group by code and compute P&L
    by_code = defaultdict(list)
    for r in rows:
        by_code[r['code']].append(r)

    winners = []
    losers  = []
    holding = []

    for code, ts in by_code.items():
        name = ts[0]['name']
        buy_settl  = sum(t['settlement'] for t in ts if t['side'] == 'buy')
        sell_settl = sum(t['settlement'] for t in ts if t['side'] == 'sell')
        buy_qty    = sum(t['qty']        for t in ts if t['side'] == 'buy')
        sell_qty   = sum(t['qty']        for t in ts if t['side'] == 'sell')
        buy_trades  = [t for t in ts if t['side'] == 'buy']
        sell_trades = [t for t in ts if t['side'] == 'sell']

        if sell_qty == 0:
            holding.append(name)
            continue

        cost_basis = buy_settl * (sell_qty / buy_qty) if buy_qty > 0 else 0
        pnl = sell_settl - cost_basis

        avg_buy_price  = sum(t['price'] * t['qty'] for t in buy_trades)  / buy_qty  if buy_qty  > 0 else 0
        avg_sell_price = sum(t['price'] * t['qty'] for t in sell_trades) / sell_qty if sell_qty > 0 else 0

        buy_dates  = sorted(t['trade_date'] for t in buy_trades)
        sell_dates = sorted(t['trade_date'] for t in sell_trades)

        # Hold period (first buy → last sell)
        try:
            from datetime import datetime
            first_buy  = datetime.strptime(str(buy_dates[0])[:10],  '%Y-%m-%d')
            last_sell  = datetime.strptime(str(sell_dates[-1])[:10], '%Y-%m-%d')
            hold_days  = (last_sell - first_buy).days
        except Exception:
            hold_days = None

        entry = {
            "name": name,
            "code": code,
            "pnl": round(pnl),
            "pnl_pct": round(pnl / cost_basis * 100, 1) if cost_basis else 0,
            "avg_buy_price": round(avg_buy_price, 1),
            "avg_sell_price": round(avg_sell_price, 1),
            "buy_count": len(buy_trades),
            "sell_count": len(sell_trades),
            "hold_days": hold_days,
            "first_buy": str(buy_dates[0])[:10],
            "last_sell": str(sell_dates[-1])[:10],
        }
        if pnl >= 0:
            winners.append(entry)
        else:
            losers.append(entry)

    winners.sort(key=lambda x: x['pnl'], reverse=True)
    losers.sort(key=lambda x: x['pnl'])

    def fmt(entries):
        lines = []
        for e in entries:
            hold = f"保有期間{e['hold_days']}日" if e['hold_days'] is not None else ""
            lines.append(
                f"- {e['name']}({e['code']}): 損益{e['pnl']:+,}円({e['pnl_pct']:+.1f}%) "
                f"買均{e['avg_buy_price']:,.0f}円 → 売均{e['avg_sell_price']:,.0f}円 "
                f"買{e['buy_count']}回/売{e['sell_count']}回 {hold}"
            )
        return "\n".join(lines) if lines else "なし"

    prompt = f"""あなたは株式トレード分析の専門家です。以下のトレード履歴を分析し、勝ちトレードと負けトレードの傾向を日本語で教えてください。

## 利益トレード（{len(winners)}件）
{fmt(winners)}

## 損失トレード（{len(losers)}件）
{fmt(losers)}

{"## 保有中（未決済）" + chr(10) + chr(10).join(f"- {n}" for n in holding) if holding else ""}

以下の観点で簡潔に分析してください（各項目3〜5行程度）：
1. 勝ちトレードの共通パターン・特徴
2. 負けトレードの共通パターン・特徴
3. 改善のための具体的なアドバイス（2〜3点）"""

    bedrock = boto3.client("bedrock-runtime", region_name="us-east-1")
    body = json.dumps({
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 1500,
        "messages": [{"role": "user", "content": prompt}]
    })
    response = bedrock.invoke_model(
        modelId="anthropic.claude-3-haiku-20240307-v1:0",
        body=body,
        contentType="application/json",
        accept="application/json"
    )
    result = json.loads(response["body"].read())
    return {"analysis": result["content"][0]["text"]}


@app.get("/candles")
def get_candles(symbol: str = Query(...), interval: str = Query("1d")):
    if interval not in INTERVALS:
        raise HTTPException(400, "Invalid interval")
    sym = f"{symbol}.T" if symbol.isdigit() else symbol
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
