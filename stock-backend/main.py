from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import yfinance as yf
import pandas as pd

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

INTERVALS = {
    "1m":  {"period": "5d",  "is_date": False},
    "1h":  {"period": "60d", "is_date": False},
    "1d":  {"period": "2y",  "is_date": True},
    "1wk": {"period": "5y",  "is_date": True},
}

@app.get("/candles")
def get_candles(symbol: str = Query(...), interval: str = Query("1d")):
    if interval not in INTERVALS:
        raise HTTPException(400, "Invalid interval")
    cfg = INTERVALS[interval]
    sym = f"{symbol}.T" if symbol.isdigit() else symbol
    try:
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
                t = utc + 9 * 3600  # shift to JST display
            result.append({
                "time":   t,
                "open":   round(float(row["Open"]), 1),
                "high":   round(float(row["High"]), 1),
                "low":    round(float(row["Low"]),  1),
                "close":  round(float(row["Close"]),1),
                "volume": int(row["Volume"]) if not pd.isna(row["Volume"]) else 0,
            })
        return result
    except Exception as e:
        raise HTTPException(500, str(e))
