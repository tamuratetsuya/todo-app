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
        cur.execute("""
            CREATE TABLE IF NOT EXISTS analysis_history (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                analysis_text MEDIUMTEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
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
        try:
            cur.execute("ALTER TABLE signal_history ADD COLUMN stop_loss FLOAT DEFAULT NULL")
        except Exception:
            pass
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
        # 時間足ごとに送る本数を調整（トークン節約のため列は date,close,MA25,BB% に絞る）
        limits = {"1wk": len(candles), "1d": 400, "1h": 120}
        target = candles[-limits.get(interval, 400):]
        closes = [c['close'] for c in target]

        lines = ["日時,終値,MA25,BB%"]
        for i, c in enumerate(target):
            t = c['time'] if isinstance(c['time'], str) else str(c['time'])
            if not isinstance(c['time'], str):
                from datetime import datetime, timezone
                ts = int(c['time'])
                t = datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%Y-%m-%d')
            ma25 = round(sum(closes[max(0,i-24):i+1]) / min(i+1, 25), 1)
            bb_pct = ""
            if i >= 19:
                c20   = closes[i-19:i+1]
                mean  = sum(c20) / 20
                std   = (sum((x - mean)**2 for x in c20) / 20) ** 0.5
                upper = mean + 2 * std
                lower = mean - 2 * std
                bb_pct = round((c['close'] - lower) / (upper - lower) * 100, 1) if upper != lower else 50
            lines.append(f"{t},{c['close']},{ma25},{bb_pct}")
        return "\n".join(lines)
    except Exception:
        return ""


def parse_signals(text: str) -> list:
    """レスポンステキストから ---SIGNALS_START--- ブロックを抽出してパース"""
    import re
    m = re.search(r'---SIGNALS_START---\s*(.*?)\s*---SIGNALS_END---', text, re.DOTALL)
    if not m:
        return []
    try:
        raw = json.loads(m.group(1))
        signals = []
        for s in raw:
            if isinstance(s, dict) and 'date' in s and 'side' in s:
                sl = s.get('stop_loss', None)
                price = s.get('price', 0) or 0
                # stop_loss は必ずエントリー価格より低くなければならない
                if sl is not None and price > 0 and float(sl) >= float(price):
                    sl = None  # 不正な損切り価格は無効化
                signals.append({
                    "time":      s['date'],
                    "side":      s['side'],
                    "price":     price,
                    "reason":    s.get('reason', ''),
                    "stop_loss": sl,
                })
        return signals
    except Exception:
        return []


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
    auto_viewpoints   = req.get("auto_viewpoints", [])
    trend_viewpoints  = req.get("trend_viewpoints", [])
    custom_viewpoints = req.get("custom_viewpoints", [])
    signal_symbol     = req.get("symbol", "")
    signal_interval   = req.get("interval", "1d")

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

            sym = f"{code}.T" if code.isdigit() else code
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

        def fmt(entries):
            lines = []
            for e in entries:
                hold = f"保有{e['hold_days']}日" if e['hold_days'] is not None else ""
                buy_info  = f" [買い時: {e['buy_desc']}]"  if e['buy_desc']  else ""
                sell_info = f" [売り時: {e['sell_desc']}]" if e['sell_desc'] else ""
                lines.append(
                    f"- {e['name']}({e['code']}): 損益{e['pnl']:+,}円({e['pnl_pct']:+.1f}%) "
                    f"買均{e['avg_buy_price']:,.0f}円→売均{e['avg_sell_price']:,.0f}円 "
                    f"買{e['buy_count']}回/売{e['sell_count']}回 {hold}"
                    f"{buy_info}{sell_info}"
                )
            return "\n".join(lines) if lines else "なし"

        # シグナル生成用のローソク足サマリー（直近データがなければyfinanceから取得）
        candle_summary = ""
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
                    sym = f"{signal_symbol}.T" if signal_symbol.isdigit() else signal_symbol
                    fresh = fetch_from_yfinance(sym, signal_interval)
                    if fresh:
                        c2 = get_conn()
                        try:
                            save_candles_to_db(c2, signal_symbol, signal_interval, fresh)
                        finally:
                            c2.close()
            except Exception:
                pass
            candle_summary = build_candle_summary(signal_symbol, signal_interval)

        # 最終トレード日と最新ローソク足の日付を取得
        all_trade_dates = []
        for ts_list in by_code.values():
            for t in ts_list:
                all_trade_dates.append(str(t['trade_date'])[:10])
        last_trade_date = max(all_trade_dates) if all_trade_dates else ""

        signal_section = ""
        if candle_summary:
            rows = candle_summary.strip().split('\n')
            first_candle_date = rows[1].split(',')[0] if len(rows) > 1 else ""
            last_candle_date  = rows[-1].split(',')[0]
            signal_section = f"""

## {signal_symbol} のローソク足データ（{signal_interval} / {first_candle_date}〜{last_candle_date}）
{candle_summary}

---

上記のトレード分析と{signal_symbol}のローソク足データを踏まえ、分析結果と整合した推奨売買シグナルを10〜15個生成してください。

【重要ルール】
- 「日時」列に記載されている実際の日付のみ使用すること（存在しない日付は絶対に使わない）
- **シグナルをデータ全期間（{first_candle_date}〜{last_candle_date}）に均等に分散させること**（直近に集中させないこと）
- 期間を前半・中盤・後半の3つに分け、各期間に3〜5個ずつ配置すること
- 最終トレード日（{last_trade_date}）以降にも必ず3個以上含めること
- 勝ちトレードで判明したエントリー条件（MA・BB）および上記の重点分析観点が揃っているタイミングを買いシグナルとする
- 利確・損切りルールに基づくタイミングを売りシグナルとする
- reasonは具体的な指標の状態を日本語で記述すること（例:「MA25上抜け・BB下限(BB%=18)から反発」）
- 買いシグナルには必ず stop_loss（損切り価格）を設定すること。スイングトレードを前提に、直近の明確な安値・サポートラインの少し下（その水準を明確に下回ったら損切り）を根拠とし、エントリー価格の5〜15%下を目安とする。2〜3%など狭い損切りは設定しないこと

必ず以下のブロックを分析テキストの末尾に出力すること（ブロック内はJSON配列のみ、他の文字を含めないこと）：
---SIGNALS_START---
[{{"date":"YYYY-MM-DD","side":"buy","reason":"理由","stop_loss":数値}},{{"date":"YYYY-MM-DD","side":"sell","reason":"理由"}}]
---SIGNALS_END---"""

        # トレンド観点（チャート由来）
        trend_section = ""
        if trend_viewpoints:
            items = '\n'.join(f'- {v}' for v in trend_viewpoints)
            trend_section = f"\n\n## 現在のチャートトレンド\n{items}"

        # カスタム観点（ユーザー追加）を個別セクションとして構築
        extra_section = ""
        if custom_viewpoints:
            items = '\n'.join(f'- {v}' for v in custom_viewpoints)
            extra_section = f"""

### 4. 追加観点の分析（必須）
以下の観点について、このトレード履歴の結果・勝ち負けのパターンを踏まえて**それぞれ個別に**具体的に言及してください：
{items}"""

        # シグナル生成での観点反映指示を signal_section に追記
        if signal_section and custom_viewpoints:
            vp_list = '・'.join(custom_viewpoints)
            signal_section = signal_section.replace(
                '- 勝ちトレードで判明したエントリー条件（MA・BB）および上記の重点分析観点が揃っているタイミングを買いシグナルとする',
                '- 上記の分析（勝ちトレードのエントリー条件・チャートトレンド・追加観点）が揃っているタイミングを買いシグナルとする'
                f'\n- 特に追加観点（{vp_list}）が確認できるタイミングを優先すること'
            )
        elif signal_section:
            signal_section = signal_section.replace(
                '- 勝ちトレードで判明したエントリー条件（MA・BB）および上記の重点分析観点が揃っているタイミングを買いシグナルとする',
                '- 上記の分析（勝ちトレードのエントリー条件・チャートトレンド）が揃っているタイミングを買いシグナルとする'
            )

        holding_section = ("## 保有中（未決済）\n" + "\n".join(f"- {n}" for n in holding)) if holding else ""

        # 出力フォーマット指示（カスタム観点がある場合はセクション4を必須化）
        if custom_viewpoints:
            vp_items = '\n'.join(f'  - {v}' for v in custom_viewpoints)
            output_format = f"""
あなたの回答は必ず以下のセクション構成で出力してください：

### 1. 勝ちトレードの傾向
### 2. 負けトレードの傾向
### 3. 具体的な改善アドバイス
### 4. 追加観点の分析
（以下の観点それぞれについて、このトレード履歴の結果を踏まえて具体的に記述すること）
{vp_items}"""
        else:
            output_format = """
あなたの回答は必ず以下のセクション構成で出力してください：

### 1. 勝ちトレードの傾向
### 2. 負けトレードの傾向
### 3. 具体的な改善アドバイス"""

        prompt = f"""あなたはスイングトレード（数日〜数週間の中期保有）の専門アナリストです。
以下のトレード履歴と、各トレード時点での技術指標の状況（MA5/MA25乖離、ボリンジャーバンド位置、一目均衡表の転換線/基準線の関係、出来高動向）を基に、詳細な分析をしてください。
{trend_section}
## 利益トレード（{len(winners)}件）
{fmt(winners)}

## 損失トレード（{len(losers)}件）
{fmt(losers)}

{holding_section}
{output_format}{signal_section}"""

    finally:
        conn.close()

    bedrock = boto3.client("bedrock-runtime", region_name="us-east-1")
    bedrock_body = json.dumps({
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 3000,
        "messages": [{"role": "user", "content": prompt}]
    })
    response = bedrock.invoke_model(
        modelId="anthropic.claude-3-haiku-20240307-v1:0",
        body=bedrock_body,
        contentType="application/json",
        accept="application/json"
    )
    result = json.loads(response["body"].read())
    full_text = result["content"][0]["text"]

    # シグナルブロックを分析テキストから除去し、パース
    import re
    analysis_text = re.sub(r'\s*---SIGNALS_START---.*?---SIGNALS_END---', '', full_text, flags=re.DOTALL).strip()
    signals = parse_signals(full_text)

    return {"analysis": analysis_text, "signals": signals}


@app.post("/history/analysis")
def save_analysis(data: dict):
    symbol = data.get("symbol", "")
    label  = data.get("label", symbol)
    if not symbol:
        raise HTTPException(400, "No symbol")
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO analysis_history (analysis_text) VALUES (%s)", (label,))
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
                "SELECT id, LEFT(analysis_text, 100) as preview, created_at "
                "FROM analysis_history ORDER BY created_at DESC LIMIT 20"
            )
            rows = cur.fetchall()
        return [{"id": r["id"], "symbol": r["preview"], "created_at": str(r["created_at"])} for r in rows]
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
