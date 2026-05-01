#!/usr/bin/env python3
"""
Free News Guard v2 — 消息面风控守门员
多源并发( GDELT / RSS / CryptoPanic ) → risk_block → news_state.json
不交易，不放哨，只做一件事：告诉交易系统"现在能不能开仓"
"""
import asyncio, aiohttp, json, os, sys, time, sqlite3, hashlib
from datetime import datetime, timezone
from dataclasses import dataclass, asdict
from typing import Optional
from xml.etree import ElementTree as ET

# ═══════════════ 配置 ═══════════════
DB_PATH = os.path.join(os.path.dirname(__file__), "data", "news_guard.sqlite3")
JSONL_PATH = os.path.join(os.path.dirname(__file__), "data", "news_events.jsonl")
STATE_PATH = os.path.join(os.path.dirname(__file__), "data", "news_state.json")
CACHE_TTL = 300  # 5分钟内不重复抓

# 高风险关键词
HIGH_RISK_KEYWORDS = [
    "fomc", "federal reserve", "rate hike", "rate cut", "interest rate",
    "cpi", "inflation data", "nfp", "nonfarm", "unemployment",
    "sec", "cftc", "regulation", "ban", "crackdown",
    "hack", "exploit", "breach", "stolen", "drain",
    "war", "missile", "sanction", "tariff", "trade war",
    "emergency meeting", "circuit breaker", "halt", "suspension",
    "china ban", "china crackdown", "pboc", "renminbi"
]

MEDIUM_RISK_KEYWORDS = [
    "etf", "approval", "rejection", "filing",
    "lawsuit", "sec lawsuit", "doj",
    "liquidated", "liquidation cascade",
    "whale", "large transfer", "mt gox",
    "defi hack", "bridge hack", "oracle manipulation"
]

# ═══════════════ 数据结构 ═══════════════
@dataclass
class NewsEvent:
    source: str
    title: str
    url: str
    published: str
    risk_keywords: list
    risk_score: int  # 0-100

class NewsState:
    risk_block: bool = False
    risk_score: int = 0
    risk_level: str = "LOW"
    active_threats: list = None
    last_update: str = ""
    news_count: int = 0

    def to_dict(self):
        return {
            "risk_block": self.risk_block,
            "risk_score": self.risk_score,
            "risk_level": self.risk_level,
            "active_threats": self.active_threats or [],
            "last_update": self.last_update,
            "news_count": self.news_count
        }

# ═══════════════ 数据库 ═══════════════
def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""CREATE TABLE IF NOT EXISTS news_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source TEXT, title TEXT, url TEXT UNIQUE, published TEXT,
        risk_keywords TEXT, risk_score INTEGER, created_at TEXT)""")
    conn.commit()
    return conn

# ═══════════════ 风险评分 ═══════════════
def score_risk(title: str) -> tuple:
    t = title.lower()
    kw = []
    score = 0

    for k in HIGH_RISK_KEYWORDS:
        if k in t:
            kw.append(k)
            score += 25
    for k in MEDIUM_RISK_KEYWORDS:
        if k in t:
            kw.append(k)
            score += 10

    return min(score, 100), kw

def risk_level(score: int) -> str:
    if score >= 75: return "EXTREME"
    if score >= 50: return "HIGH"
    if score >= 25: return "MEDIUM"
    return "LOW"

# ═══════════════ 新闻源 ═══════════════
async def fetch_gdelt(session) -> list:
    """GDELT 全球事件数据库"""
    url = "https://api.gdeltproject.org/api/v2/doc/doc?query=cryptocurrency%20OR%20bitcoin%20OR%20ethereum%20OR%20fed%20OR%20central%20bank&mode=artlist&maxrecords=10&format=json"
    try:
        async with session.get(url, timeout=15) as r:
            data = await r.json()
            events = []
            for art in data.get("articles", [])[:5]:
                score, kw = score_risk(art.get("title", ""))
                events.append(NewsEvent(
                    source="GDELT", title=art.get("title",""),
                    url=art.get("url",""), published=art.get("seendate",""),
                    risk_keywords=kw, risk_score=score))
            return events
    except: return []

async def fetch_cryptopanic(session) -> list:
    """CryptoPanic 免费RSS"""
    url = "https://cryptopanic.com/api/v1/posts/?auth_token=&public=true&filter=important"
    try:
        async with session.get(url, timeout=10) as r:
            data = await r.json()
            events = []
            for post in data.get("results", [])[:10]:
                title = post.get("title","")
                score, kw = score_risk(title)
                if score >= 10:  # 只保留有风险信号的
                    events.append(NewsEvent(
                        source="CryptoPanic", title=title,
                        url=post.get("url",""),
                        published=post.get("published_at",""),
                        risk_keywords=kw, risk_score=score))
            return events
    except: return []

async def fetch_coindesk_rss(session) -> list:
    """CoinDesk RSS"""
    try:
        async with session.get("https://www.coindesk.com/arc/outboundfeeds/rss/", timeout=10) as r:
            text = await r.text()
            root = ET.fromstring(text)
            events = []
            for item in root.findall(".//item")[:10]:
                title = item.findtext("title","")
                score, kw = score_risk(title)
                if score >= 10:
                    events.append(NewsEvent(
                        source="CoinDesk", title=title,
                        url=item.findtext("link",""),
                        published=item.findtext("pubDate",""),
                        risk_keywords=kw, risk_score=score))
            return events
    except: return []

# ═══════════════ 主逻辑 ═══════════════
async def run_once(session, dry_run=False):
    """执行一次新闻扫描"""
    # 并发抓取
    results = await asyncio.gather(
        fetch_gdelt(session), fetch_cryptopanic(session), fetch_coindesk_rss(session),
        return_exceptions=True
    )

    all_events = []
    for r in results:
        if isinstance(r, list):
            all_events.extend(r)

    if not all_events:
        return NewsState()

    # 去重
    seen = set()
    unique = []
    for e in all_events:
        h = hashlib.md5(e.title.encode()).hexdigest()
        if h not in seen:
            seen.add(h)
            unique.append(e)

    # 计算风险状态
    max_score = max(e.risk_score for e in unique) if unique else 0
    high_events = [e for e in unique if e.risk_score >= 25]
    threats = [f"[{e.source}] {e.title[:80]}" for e in high_events[:5]]

    state = NewsState()
    state.risk_score = max_score
    state.risk_level = risk_level(max_score)
    state.risk_block = max_score >= 50
    state.active_threats = threats
    state.last_update = datetime.now(timezone.utc).isoformat()
    state.news_count = len(unique)

    if not dry_run:
        # 写 SQLite
        conn = init_db()
        now = datetime.now().isoformat()
        for e in unique:
            conn.execute("INSERT OR IGNORE INTO news_events (source,title,url,published,risk_keywords,risk_score,created_at) VALUES(?,?,?,?,?,?,?)",
                        [e.source, e.title, e.url, e.published, json.dumps(e.risk_keywords), e.risk_score, now])
        conn.commit(); conn.close()

        # 写 JSONL
        for e in unique:
            with open(JSONL_PATH, "a") as f:
                f.write(json.dumps(asdict(e), ensure_ascii=False) + "\n")

        # 写状态文件
        with open(STATE_PATH, "w") as f:
            json.dump(state.to_dict(), f, indent=2)

    return state

# ═══════════════ CLI ═══════════════
async def main():
    dry = "--dry-run" in sys.argv
    once = "--once" in sys.argv
    print_all = "--print-all" in sys.argv

    async with aiohttp.ClientSession() as session:
        if once:
            state = await run_once(session, dry_run=dry)
            print(json.dumps(state.to_dict(), indent=2))
            if print_all and not dry:
                try:
                    with open(JSONL_PATH) as f:
                        for line in f.readlines()[-10:]:
                            print(line.strip())
                except: pass
        else:
            print(f"[{datetime.now():%H:%M:%S}] News Guard 持续监控 | 间隔{CACHE_TTL}s | state→{STATE_PATH}")
            while True:
                try:
                    state = await run_once(session)
                    level_emoji = {"LOW":"🟢","MEDIUM":"🟡","HIGH":"🟠","EXTREME":"🔴"}
                    print(f"[{datetime.now():%H:%M:%S}] {level_emoji.get(state.risk_level,'⚪')} {state.risk_level} | score={state.risk_score} | block={state.risk_block} | {state.news_count}条")
                    await asyncio.sleep(CACHE_TTL)
                except KeyboardInterrupt:
                    print("⏹ 停止"); break
                except Exception as e:
                    print(f"异常: {e}"); await asyncio.sleep(60)

if __name__ == "__main__":
    asyncio.run(main())
