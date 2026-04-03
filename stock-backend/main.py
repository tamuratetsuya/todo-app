from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List
import yfinance as yf
import pandas as pd
import pymysql
import os
from dotenv import load_dotenv

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
