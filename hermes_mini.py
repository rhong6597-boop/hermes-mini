#!/usr/bin/env python3
"""
Hermes Mini Agent v2.1 — DeepSeek接管 BTC+ETH+XAU 三品种
Gate永续合约 | OHLCV桥接 | 全自动
"""
import asyncio, aiohttp, json, os, time, hmac, hashlib, csv
from datetime import datetime
import numpy as np

# ═══════════════ 配置 ═══════════════
DEEPSEEK_API_KEY = "sk-16250e8ef8a84648ad364785cfcb89e9"
GATE_KEY    = "d6ec417eadeb422de0230aef365d6d2d"
GATE_SECRET = "049c3b1887a9e4bcb1e369f034081a753a28dd9110ed9fe5370d066bd2cb3f92"
TG_TOKEN = "8793068207:AAH9dN-LOJNlY9oh7bgkyLiiKKiPvniZiA"
TG_CHAT  = "7150312022"

AUTO_TRADE = True
POSITION_SIZE = 15
DEFAULT_LEVERAGE = 5
SYMBOLS = ["BTC", "ETH", "XAU"]
OHLCV_1H = "/root/hermes/strategies/attack_backtest/gateio_contract_1h.csv"
FACTOR_CSV = "/root/hermes/strategies/attack_backtest/paper_logs/factor_signals.csv"

# ═══════════════ 指标计算 ═══════════════
def ema(arr, period):
    alpha = 2.0/(period+1); r = np.zeros_like(arr); r[0]=arr[0]
    for i in range(1,len(arr)): r[i]=alpha*arr[i]+(1-alpha)*r[i-1]
    return r

def rsi(close, period=14):
    d = np.diff(close, prepend=close[0])
    g = np.where(d>0,d,0); l = np.where(d<0,-d,0)
    ag = np.zeros_like(close); al = np.zeros_like(close)
    for i in range(period,len(close)):
        ag[i]=np.mean(g[i-period+1:i+1]); al[i]=np.mean(l[i-period+1:i+1])
    rs = np.divide(ag,al,out=np.zeros_like(ag),where=al!=0)
    return 100-(100/(1+rs))

def atr(high, low, close, period=14):
    tr = np.maximum(high[1:]-low[1:], np.maximum(np.abs(high[1:]-close[:-1]), np.abs(low[1:]-close[:-1])))
    a = np.zeros(len(close)); a[period]=np.mean(tr[:period])
    for i in range(period+1,len(close)): a[i]=(a[i-1]*(period-1)+tr[i-1])/period
    return a

def load_ohlcv(path, symbol, limit=100):
    rows = []
    try:
        with open(path) as f:
            for row in csv.DictReader(f):
                if row["symbol"]==symbol: rows.append(row)
        rows = rows[-limit:]
        if not rows: return None
        return {"o":np.array([float(r["open"]) for r in rows]),
                "h":np.array([float(r["high"]) for r in rows]),
                "l":np.array([float(r["low"]) for r in rows]),
                "c":np.array([float(r["close"]) for r in rows]),
                "v":np.array([float(r["volume"]) for r in rows]), "n":len(rows)}
    except: return None

def compute_indicators(df):
    c, h, l, v = df["c"], df["h"], df["l"], df["v"]
    e20 = ema(c,20); e50 = ema(c,50) if len(c)>=50 else e20
    r = rsi(c,14); a = atr(h,l,c,14)
    vol_ma20 = np.mean(v[-20:]) if len(v)>=20 else np.mean(v)
    high_20, low_20 = np.max(h[-20:]), np.min(l[-20:])
    rng = high_20-low_20
    pos = (c[-1]-low_20)/rng*100 if rng>0 else 50
    chg5 = (c[-1]-c[-6])/c[-6]*100 if len(c)>=6 else 0
    return {
        "price":f"{c[-1]:.2f}","ema20":f"{e20[-1]:.2f}","ema50":f"{e50[-1]:.2f}",
        "ema_trend":"bullish" if e20[-1]>e50[-1] else "bearish",
        "rsi14":f"{r[-1]:.0f}","atr14":f"{a[-1]:.2f}",
        "vol_ratio":f"{v[-1]/vol_ma20:.1f}x" if vol_ma20>0 else "?",
        "resistance":f"{high_20:.2f}","support":f"{low_20:.2f}",
        "pos_in_range":f"{pos:.0f}%","chg_5bar":f"{chg5:+.2f}%","bars":len(c)}

# ═══════════════ Gate API ═══════════════
def gate_sign(method, path, body=""):
    ts=str(int(time.time()))
    ph=hashlib.sha512(body.encode()).hexdigest()
    ss=f"{method}\n{path}\n{body}\n{ph}\n{ts}" if body else f"{method}\n{path}\n\n{ph}\n{ts}"
    sig=hmac.new(GATE_SECRET.encode(),ss.encode(),hashlib.sha512).hexdigest()
    return {"KEY":GATE_KEY,"SIGN":sig,"Timestamp":ts,"Content-Type":"application/json"}

async def fetch_price(session, symbol):
    url=f"https://api.gateio.ws/api/v4/futures/usdt/tickers?contract={symbol}_USDT"
    async with session.get(url,timeout=8) as r:
        d=await r.json(); return float(d[0]["last"]) if d else 0

async def fetch_funding(session, symbol):
    url=f"https://api.gateio.ws/api/v4/futures/usdt/contracts/{symbol}_USDT"
    async with session.get(url,timeout=8) as r:
        d=await r.json(); return float(d.get("funding_rate",0))

async def fetch_klines(session, symbol, limit=100):
    """Gate 1h K线"""
    url=f"https://api.gateio.ws/api/v4/futures/usdt/candlesticks?contract={symbol}_USDT&interval=1h&limit={limit}"
    async with session.get(url,timeout=10) as r:
        data=await r.json()
        if data:
            return {"o":np.array([float(d["o"]) for d in data]),
                    "h":np.array([float(d["h"]) for d in data]),
                    "l":np.array([float(d["l"]) for d in data]),
                    "c":np.array([float(d["c"]) for d in data]),
                    "v":np.array([float(d["v"]) for d in data]),"n":len(data)}
    return None

async def gate_positions(session):
    path="/api/v4/futures/usdt/positions"
    async with session.get("https://api.gateio.ws"+path,headers=gate_sign("GET",path),timeout=8) as r:
        return [p for p in await r.json() if float(p.get("size",0))!=0]

async def gate_place(session, symbol, direction, amount, leverage=5):
    path="/api/v4/futures/usdt/orders"
    size=amount if direction=="long" else -amount
    order={"contract":f"{symbol}_USDT","size":str(size),"price":"0","tif":"ioc","reduce_only":False}
    body=json.dumps(order)
    async with session.post("https://api.gateio.ws"+path,json=order,headers=gate_sign("POST",path,body),timeout=10) as r:
        return await r.json()

async def gate_close(session, symbol, size):
    path="/api/v4/futures/usdt/orders"
    order={"contract":f"{symbol}_USDT","size":str(-size),"price":"0","tif":"ioc","reduce_only":True}
    body=json.dumps(order)
    async with session.post("https://api.gateio.ws"+path,json=order,headers=gate_sign("POST",path,body),timeout=10) as r:
        return await r.json()

# ═══════════════ Telegram ═══════════════
async def tg_send(session, msg):
    try:
        async with session.post(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                               json={"chat_id":TG_CHAT,"text":msg,"parse_mode":"Markdown"},timeout=5) as r:
            return await r.json()
    except: return None

# ═══════════════ DeepSeek ═══════════════
async def ask_deepseek(session, market, indicators) -> dict:
    lines = [f"你管理 BTC/ETH/XAU 三个品种的永续合约。基于指标做决策。\n{datetime.now().strftime('%Y-%m-%d %H:%M UTC')}\n"]
    for sym in ["BTC","ETH","XAU"]:
        ind = indicators.get(sym,{})
        fund = market.get(f"{sym.lower()}_funding","?")
        lines.append(f"═══ {sym}_USDT ═══\n价格:{ind.get('price','?')} | EMA20/50:{ind.get('ema20','?')}/{ind.get('ema50','?')} | 趋势:{ind.get('ema_trend','?')}\nRSI:{ind.get('rsi14','?')} | ATR:{ind.get('atr14','?')} | 量:{ind.get('vol_ratio','?')}\n阻力:{ind.get('resistance','?')} 支撑:{ind.get('support','?')} | 位:{ind.get('pos_in_range','?')} | 动量:{ind.get('chg_5bar','?')}\n费率:{fund}\n")
    lines.append('选最优品种+方向。严格JSON:\n{"action":"long/short/hold","symbol":"BTC/ETH/XAU","confidence":"A/B/C","leverage":5-8,"reason":"一句"}')

    headers={"Authorization":f"Bearer {DEEPSEEK_API_KEY}","Content-Type":"application/json"}
    payload={"model":"deepseek-chat","messages":[{"role":"user","content":"\n".join(lines)}],"temperature":0.2,"max_tokens":300}
    try:
        async with session.post("https://api.deepseek.com/v1/chat/completions",json=payload,headers=headers,timeout=30) as r:
            content=(await r.json())["choices"][0]["message"]["content"]
            return json.loads(content.strip().strip("```json").strip("```").strip())
    except Exception as e:
        print(f"DS异常:{e}"); return {"action":"hold","confidence":"C"}

# ═══════════════ 主循环 ═══════════════
async def main():
    print(f"[{datetime.now():%H:%M:%S}] v2.1 启动 | BTC+ETH+XAU | 实盘={AUTO_TRADE}")
    async with aiohttp.ClientSession() as s:
        await tg_send(s,"🚀 *Hermes v2.1*\nDeepSeek接管 BTC+ETH+XAU\nOHLCV桥接 | 间隔30s")

        while True:
            try:
                # ── 1. 拉数据 ──
                prices = {sym: await fetch_price(s,sym) for sym in SYMBOLS}
                fundings = {sym: await fetch_funding(s,sym) for sym in SYMBOLS}
                positions = await gate_positions(s)

                kline_tasks = [fetch_klines(s,sym,100) for sym in SYMBOLS]
                kline_results = await asyncio.gather(*kline_tasks)

                # 优先用CSV做指标，XAU用API K线
                indicators = {}
                for sym, df in zip(SYMBOLS, kline_results):
                    csv_df = load_ohlcv(OHLCV_1H, f"{sym}_USDT", 100) if sym != "XAU" else None
                    src = csv_df or df
                    if src:
                        indicators[sym] = compute_indicators(src)

                # ── 2. 持仓管理 ──
                if positions:
                    for pos in positions:
                        contract = pos["contract"].replace("_USDT","")
                        side = "short" if int(float(pos["size"]))<0 else "long"
                        entry = float(pos.get("entry_price",0))
                        current = prices.get(contract,0)
                        pnl_pct = (entry-current)/entry*100 if side=="short" else (current-entry)/entry*100

                        ts = datetime.now().strftime("%H:%M:%S")
                        print(f"[{ts}] 📊 {contract} {side} | {entry:.2f}→{current:.2f} | PnL:{pnl_pct:+.2f}%")

                        if pnl_pct <= -1.5 or pnl_pct >= 3.0:
                            r = await gate_close(s, contract, int(float(pos["size"])))
                            reason = "🛑止损" if pnl_pct<=-1.5 else "🎯止盈"
                            msg = f"{reason} {contract} {side} @{current:.2f} PnL:{pnl_pct:+.2f}%"
                            print(msg); await tg_send(s,msg)
                            await asyncio.sleep(5); continue
                    await asyncio.sleep(30); continue

                # ── 3. AI决策 ──
                market = {f"{sym.lower()}_funding":f"{fundings[sym]:.6f}" for sym in SYMBOLS}
                decision = await ask_deepseek(s, market, indicators)

                action = decision.get("action","hold")
                conf = decision.get("confidence","C")
                sym = decision.get("symbol","BTC")
                reason = decision.get("reason","")
                emoji = {"long":"🟢","short":"🔴"}.get(action,"⚪")

                ts = datetime.now().strftime("%H:%M:%S")
                print(f"[{ts}] {emoji} {sym} {action} {conf}级 | {reason}")

                if AUTO_TRADE and conf in ("A","B") and action!="hold":
                    lev = min(decision.get("leverage",DEFAULT_LEVERAGE), 8)
                    result = await gate_place(s, sym, action, POSITION_SIZE, lev)
                    status = "✅" if result and "id" in str(result) else f"❌{result}"
                    await tg_send(s,f"{emoji} *{sym} {action} {conf}级*\n💰{POSITION_SIZE}U {lev}x\n📝{reason}\n{status}")
                    await asyncio.sleep(60)

                await asyncio.sleep(30)

            except KeyboardInterrupt: print("⏹"); break
            except Exception as e: print(f"异常:{e}"); await asyncio.sleep(10)

if __name__ == "__main__":
    asyncio.run(main())
