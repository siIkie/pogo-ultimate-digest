#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Resilient PvE attacker scraper with cloudscraper + optional paid-scraper fallback.

Keeps previous heuristics for pokebattler, gamepress, gohub and a pokemondb fallback.
Writes both outputs/attackers.json and pogo_library/attackers/index.json for CI compatibility.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

# optional (best-effort) import for bypassing simple Cloudflare protections
try:
    import cloudscraper
except Exception:
    cloudscraper = None

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# realistic UAs
USER_AGENTS = [
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]
DEFAULT_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}

DEFAULT_TIMEOUT = 20
SLEEP_BETWEEN_REQUESTS = 1.0
ISO_Z = "%Y-%m-%dT%H:%M:%SZ"


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime(ISO_Z)


def make_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"],
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.mount("http://", HTTPAdapter(max_retries=retries))
    return s


def http_get(url: str, params: Optional[Dict[str, Any]] = None, referer: Optional[str] = None) -> Optional[str]:
    """
    Robust GET that tries, in order:
      1. cloudscraper (if installed) with UA rotation
      2. requests.Session with UA rotation
      3. (optional) paid scraping provider if env SCRAPER_API_PROVIDER + SCRAPER_API_KEY is set
    Returns response text or None.
    """
    params = dict(params or {})
    params["_"] = int(time.time())

    # Try cloudscraper first (if available)
    if cloudscraper is not None:
        try:
            for i, ua in enumerate(USER_AGENTS):
                try:
                    scr = cloudscraper.create_scraper(browser={"custom": ua})
                    headers = dict(DEFAULT_HEADERS)
                    headers["User-Agent"] = ua
                    if referer:
                        headers["Referer"] = referer
                    resp = scr.get(url, headers=headers, params=params, timeout=DEFAULT_TIMEOUT, allow_redirects=True)
                    if resp.status_code == 200:
                        return resp.text
                    if resp.status_code == 403:
                        print(f"[warn] cloudscraper GET {url} -> 403 (attempt {i+1})", file=sys.stderr)
                        time.sleep(0.5 + i)
                        continue
                    if resp.status_code >= 400:
                        print(f"[warn] cloudscraper GET {url} -> {resp.status_code}", file=sys.stderr)
                        return None
                except Exception as e:
                    print(f"[warn] cloudscraper attempt failed: {e}", file=sys.stderr)
                    time.sleep(0.5)
            print(f"[warn] cloudscraper GET {url} -> exhausted attempts", file=sys.stderr)
        except Exception as e:
            print(f"[warn] cloudscraper top-level error: {e}", file=sys.stderr)

    # Fallback to requests with retries
    session = make_session()
    for attempt in range(4):
        ua = USER_AGENTS[attempt % len(USER_AGENTS)]
        headers = dict(DEFAULT_HEADERS)
        headers["User-Agent"] = ua
        if referer:
            headers["Referer"] = referer
        try:
            r = session.get(url, headers=headers, params=params, timeout=DEFAULT_TIMEOUT, allow_redirects=True)
        except Exception as e:
            print(f"[warn] GET {url} try#{attempt+1} failed: {e}", file=sys.stderr)
            time.sleep(1 + attempt)
            continue

        if r.status_code == 200:
            return r.text
        if r.status_code == 403:
            print(f"[warn] GET {url} -> 403 (attempt {attempt+1}), trying alternate headers", file=sys.stderr)
            time.sleep(1 + attempt)
            continue
        if r.status_code >= 400:
            print(f"[warn] GET {url} -> {r.status_code}", file=sys.stderr)
            return None

    # Final: optional paid scraping provider fallback if configured via env
    provider = os.environ.get("SCRAPER_API_PROVIDER", "").strip().lower()
    key = os.environ.get("SCRAPER_API_KEY", "").strip()
    if provider and key:
        try:
            if provider in ("scraperapi", "scraper_api", "scraper-api"):
                api_url = f"https://api.scraperapi.com/?api_key={key}&url={requests.utils.requote_uri(url)}"
                r = requests.get(api_url, timeout=DEFAULT_TIMEOUT)
                if r.status_code == 200:
                    return r.text
                print(f"[warn] SCRAPER_API -> {r.status_code}", file=sys.stderr)
            elif provider in ("scrapingbee", "scraping-bee"):
                api_url = f"https://app.scrapingbee.com/api/v1?api_key={key}&url={requests.utils.requote_uri(url)}"
                r = requests.get(api_url, timeout=DEFAULT_TIMEOUT)
                if r.status_code == 200:
                    return r.text
                print(f"[warn] SCRAPINGBEE -> {r.status_code}", file=sys.stderr)
            else:
                print(f"[warn] Unknown scraper provider: {provider}", file=sys.stderr)
        except Exception as e:
            print(f"[warn] paid-scraper attempt failed: {e}", file=sys.stderr)

    print(f"[warn] GET {url} -> exhausted retries", file=sys.stderr)
    return None


def soupify(html: Optional[str]) -> Optional[BeautifulSoup]:
    if not html:
        return None
    try:
        return BeautifulSoup(html, "lxml")
    except Exception:
        return BeautifulSoup(html, "html.parser")


def text(node) -> str:
    if not node:
        return ""
    return " ".join(node.get_text(" ", strip=True).split())


def norm_space(s: str) -> str:
    return " ".join((s or "").split())


def guess_form(name: str) -> str:
    n = (name or "").lower()
    if "shadow" in n:
        return "Shadow"
    if "mega" in n:
        return "Mega"
    if "primal" in n:
        return "Primal"
    if "gmax" in n or "gigantamax" in n:
        return "Gigantamax"
    return "Standard"


def extract_movestring(s: str) -> Tuple[str, str]:
    s = (s or "").replace("/", "+")
    m = re.split(r"\s*\+\s*", s)
    fast, charge = "", ""
    if len(m) >= 2:
        fast, charge = m[0].strip(), m[1].strip()
    return fast, charge


def parse_float_safe(s: str) -> Optional[float]:
    s = re.sub(r"[^0-9\.\-]", "", s or "")
    try:
        return float(s) if s else None
    except Exception:
        return None


def to_type_bucket(name: str, hint: str = "") -> str:
    h = (hint or "").lower()
    if not h:
        return ""
    TYPES = [
        "bug", "dark", "dragon", "electric", "fairy", "fighting", "fire", "flying",
        "ghost", "grass", "ground", "ice", "normal", "poison", "psychic", "rock", "steel", "water"
    ]
    for t in TYPES:
        if re.search(rf"\b{t}\b", h):
            return t
    return ""


@dataclasses.dataclass
class AttackerRow:
    name: str
    form: str
    type_bucket: str
    fast_move: str
    charge_move: str
    source: str
    rank: Optional[int]
    score: Optional[float]
    score_kind: str
    notes: str
    url: str
    ts: str

    def key(self) -> Tuple[str, str, str, str]:
        return (
            (self.name or "").strip().lower(),
            (self.fast_move or "").strip().lower(),
            (self.charge_move or "").strip().lower(),
            (self.type_bucket or "").strip().lower(),
        )


def dedupe_best(rows: List[AttackerRow]) -> List[AttackerRow]:
    best: Dict[Tuple[str, str, str, str], AttackerRow] = {}
    for r in rows:
        k = r.key()
        if k not in best:
            best[k] = r
            continue
        prev = best[k]
        prev_rank = prev.rank if prev.rank is not None else 999999
        this_rank = r.rank if r.rank is not None else 999999
        if this_rank < prev_rank:
            best[k] = r
        elif this_rank == prev_rank:
            prev_score = prev.score if prev.score is not None else -1e9
            this_score = r.score if r.score is not None else -1e9
            if this_score > prev_score:
                best[k] = r
    return list(best.values())


def as_dict(r: AttackerRow) -> Dict[str, Any]:
    return dataclasses.asdict(r)


# ------------------------
# Pokebattler scraper (heuristic)
# ------------------------

def scrape_pokebattler(types: Iterable[str]) -> List[AttackerRow]:
    base = "https://www.pokebattler.com/raids"
    out: List[AttackerRow] = []
    ts = now_iso()
    for t in types:
        html = http_get(base, referer="https://www.google.com/")
        time.sleep(SLEEP_BETWEEN_REQUESTS)
        s = soupify(html)
        if not s:
            continue

        sections = []
        for h in s.select("h1, h2, h3, h4"):
            title = text(h)
            if re.search(rf"\b{t}\b", title.lower()) and re.search(r"best|attackers|counters", title.lower()):
                container = h.find_parent() or h.parent
                sections.append((title, container))

        for title, container in sections:
            type_bucket = to_type_bucket("", hint=title)
            items = container.select("li, .card, .list-item, .counter, tr")
            rank_ctr = 0
            for it in items:
                txt = text(it)
                m_rank = re.match(r"^\s*(\d+)[\.\)]\s+(.*)$", txt)
                rank = None
                body = txt
                if m_rank:
                    rank = int(m_rank.group(1))
                    body = m_rank.group(2)

                parts = re.split(r"\s+[-—–]\s+", body, maxsplit=1)
                name = parts[0].strip()
                moves = parts[1].strip() if len(parts) > 1 else ""
                fast, charge = extract_movestring(moves)

                if not name or len(name) < 3:
                    continue
                if re.search(r"(best|attackers|counters|type|guide|ranking)", name.lower()):
                    continue

                score = None
                score_kind = ""
                m_score = re.search(r"(DPS|Score|Rating)\s*[:=]\s*([0-9]+(\.[0-9]+)?)", body, re.IGNORECASE)
                if m_score:
                    score_kind = m_score.group(1).lower()
                    score = parse_float_safe(m_score.group(2))

                if rank is None:
                    rank_ctr += 1
                    rank = rank_ctr

                out.append(
                    AttackerRow(
                        name=name,
                        form=guess_form(name),
                        type_bucket=type_bucket or t.lower(),
                        fast_move=fast,
                        charge_move=charge,
                        source="pokebattler",
                        rank=rank,
                        score=score,
                        score_kind=score_kind,
                        notes=title,
                        url=base,
                        ts=ts,
                    )
                )
    return out


# ------------------------
# GamePress scraper
# ------------------------

def scrape_gamepress(types: Iterable[str]) -> List[AttackerRow]:
    base = "https://gamepress.gg/pokemongo"
    ts = now_iso()
    out: List[AttackerRow] = []

    html = http_get(base, referer="https://www.google.com/")
    time.sleep(SLEEP_BETWEEN_REQUESTS)
    s = soupify(html)
    if not s:
        return out

    links = []
    for a in s.select("a"):
        label = text(a)
        if not label:
            continue
        if re.search(r"best .* attackers|type.*attackers|raid attackers", label.lower()):
            href = a.get("href") or ""
            if href.startswith("/"):
                href = "https://gamepress.gg" + href
            links.append((label, href))

    seen = set()
    wanted = {t.lower() for t in types}
    filtered = []
    for label, href in links:
        tb = to_type_bucket("", hint=label.lower())
        if tb and tb in wanted and href not in seen:
            seen.add(href)
            filtered.append((tb, label, href))

    for tb, label, href in filtered:
        page = http_get(href, referer=base)
        time.sleep(SLEEP_BETWEEN_REQUESTS)
        sp = soupify(page)
        if not sp:
            continue

        candidates = sp.select("table tr, ol li, ul li, .view-content .node, .ranking-list .row, .card")
        rank = 0
        for c in candidates:
            ttext = text(c)
            if not ttext or len(ttext) < 5:
                continue
            if re.search(r"(tier|overview|guide|meta|info|introduction)", ttext.lower()):
                continue

            name = ""
            a = c.find("a")
            if a and text(a):
                name = text(a)
            if not name:
                m = re.match(r"^\s*(\d+[\.\)])?\s*([A-Za-z0-9' \-\.\:]+)", ttext)
                if m:
                    name = norm_space(m.group(2))

            fast, charge = extract_movestring(ttext)
            if not name or len(name) < 3:
                continue

            rank += 1
            score = None
            score_kind = ""
            m_score = re.search(r"(DPS|Score|Rating|TTW)\s*[:=]\s*([0-9]+(\.[0-9]+)?)", ttext, re.IGNORECASE)
            if m_score:
                score_kind = m_score.group(1).lower()
                score = parse_float_safe(m_score.group(2))

            out.append(
                AttackerRow(
                    name=name,
                    form=guess_form(name),
                    type_bucket=tb,
                    fast_move=fast,
                    charge_move=charge,
                    source="gamepress",
                    rank=rank,
                    score=score,
                    score_kind=score_kind,
                    notes=label,
                    url=href,
                    ts=ts,
                )
            )
    return out


# ------------------------
# GO Hub scraper
# ------------------------

def scrape_gohub(types: Iterable[str]) -> List[AttackerRow]:
    base = "https://pokemongohub.net/category/guides/"
    ts = now_iso()
    out: List[AttackerRow] = []

    html = http_get(base, referer="https://www.google.com/")
    time.sleep(SLEEP_BETWEEN_REQUESTS)
    s = soupify(html)
    if not s:
        return out

    candidates = []
    for a in s.select("a"):
        label = text(a)
        if not label:
            continue
        if re.search(r"best .*attacker|best .*type|best .*attackers", label.lower()):
            href = a.get("href") or ""
            if href.startswith("/"):
                href = "https://pokemongohub.net" + href
            candidates.append((label, href))

    wanted = {t.lower() for t in types}
    filtered = []
    for label, href in candidates:
        tb = to_type_bucket("", hint=label.lower())
        if tb and tb in wanted:
            filtered.append((tb, label, href))

    seen = set()
    for tb, label, href in filtered:
        if href in seen:
            continue
        seen.add(href)
        page = http_get(href, referer=base)
        time.sleep(SLEEP_BETWEEN_REQUESTS)
        sp = soupify(page)
        if not sp:
            continue

        items = sp.select("ol li, ul li, table tr, .elementor-widget-container li, .entry-content li")
        rank = 0
        for it in items:
            ttext = text(it)
            if not ttext or len(ttext) < 5:
                continue
            if re.search(r"(overview|intro|guide|about|sources|disclaimer|update)", ttext.lower()):
                continue

            name = ""
            a = it.find("a")
            if a and text(a):
                name = text(a)
            if not name:
                b = it.find("strong")
                if b and text(b):
                    name = text(b)
            if not name:
                m = re.match(r"^([A-Za-z0-9' \-\.\:]+)", ttext)
                if m:
                    name = norm_space(m.group(1))

            fast, charge = extract_movestring(ttext)
            if not name or len(name) < 3:
                continue

            rank += 1
            score = None
            score_kind = ""
            m_score = re.search(r"(DPS|Score|Rating|TTW)\s*[:=]\s*([0-9]+(\.[0-9]+)?)", ttext, re.IGNORECASE)
            if m_score:
                score_kind = m_score.group(1).lower()
                score = parse_float_safe(m_score.group(2))

            out.append(
                AttackerRow(
                    name=name,
                    form=guess_form(name),
                    type_bucket=tb,
                    fast_move=fast,
                    charge_move=charge,
                    source="gohub",
                    rank=rank,
                    score=score,
                    score_kind=score_kind,
                    notes=label,
                    url=href,
                    ts=ts,
                )
            )
    return out


# ------------------------
# Fallback: Pokemondb names
# ------------------------

def scrape_pokemondb(types: Iterable[str], limit: int = 400) -> List[AttackerRow]:
    url = "https://pokemondb.net/pokedex/all"
    ts = now_iso()
    out: List[AttackerRow] = []
    html = http_get(url, referer="https://www.google.com/")
    time.sleep(SLEEP_BETWEEN_REQUESTS)
    s = soupify(html)
    if not s:
        return out

    table = s.select_one("table#pokedex")
    names: List[str] = []
    if table:
        for tr in table.select("tbody tr")[:limit]:
            td = tr.select_one("td.cell-name a")
            if not td:
                td = tr.select_one("td:first-child a")
            if td:
                names.append(text(td))
    else:
        for a in s.select("a"):
            label = text(a)
            if label and re.match(r"^[A-Za-z]+(?:[-' ]?[A-Za-z]+)*$", label) and len(label) < 30:
                names.append(label)
        names = list(dict.fromkeys(names))[:limit]

    types_low = {t.lower() for t in types}
    default_bucket = next(iter(types_low), "") if types_low else ""
    for i, n in enumerate(names, start=1):
        out.append(
            AttackerRow(
                name=n,
                form=guess_form(n),
                type_bucket=default_bucket,
                fast_move="",
                charge_move="",
                source="pokemondb",
                rank=i,
                score=None,
                score_kind="",
                notes="fallback: pokemondb names",
                url=url,
                ts=ts,
            )
        )
    return out


# ------------------------
# Orchestration
# ------------------------

def normalize_types_arg(raw: Optional[str]) -> List[str]:
    # Default to all 18 Pokemon GO types
    if not raw:
        return [
            "bug", "dark", "dragon", "electric", "fairy", "fighting", "fire", "flying",
            "ghost", "grass", "ground", "ice", "normal", "poison", "psychic", "rock", "steel", "water"
        ]
    out = []
    for part in raw.split(","):
        p = part.strip().lower()
        if p:
            out.append(p)
    return out


def main():
    ap = argparse.ArgumentParser(description="Scrape PvE attacker rankings")
    ap.add_argument("-o", "--out", default="outputs/attackers.json", help="Output JSON path")
    ap.add_argument("--types", default="", help="Comma-separated type buckets (fire,water,rock)")
    args = ap.parse_args()
    types = normalize_types_arg(args.types)

    rows: List[AttackerRow] = []
    try:
        rows.extend(scrape_pokebattler(types))
    except Exception as e:
        print(f"[warn] pokebattler scrape failed: {e}", file=sys.stderr)
    try:
        rows.extend(scrape_gamepress(types))
    except Exception as e:
        print(f"[warn] gamepress scrape failed: {e}", file=sys.stderr)
    try:
        rows.extend(scrape_gohub(types))
    except Exception as e:
        print(f"[warn] gohub scrape failed: {e}", file=sys.stderr)

    # If few rows collected, try pokemondb fallback
    if len(rows) < 80:
        try:
            fallback = scrape_pokemondb(types, limit=400)
            if fallback:
                print(f"[info] Using pokemondb fallback: added {len(fallback)} rows", file=sys.stderr)
                rows.extend(fallback)
        except Exception as e:
            print(f"[warn] pokemondb fallback failed: {e}", file=sys.stderr)

    if not rows:
        print("[warn] No attacker rows extracted from any source.", file=sys.stderr)

    unique_rows = dedupe_best(rows)
    unique_rows.sort(key=lambda r: (r.type_bucket or "~", r.rank if r.rank is not None else 999999, r.name.lower()))

    payload = {
        "_meta": {
            "generated_at": now_iso(),
            "sources": ["pokebattler", "gamepress", "gohub", "pokemondb"],
            "types_requested": types,
            "rows_total": len(rows),
            "rows_unique": len(unique_rows),
        },
        "attackers": [as_dict(r) for r in unique_rows],
    }

    out_path = args.out
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    alt_dir = "pogo_library/attackers"
    os.makedirs(alt_dir, exist_ok=True)
    alt_path = os.path.join(alt_dir, "index.json")
    with open(alt_path, "w", encoding="utf-8") as f2:
        json.dump(payload, f2, ensure_ascii=False, indent=2)

    print(f"[ok] Wrote {out_path} and {alt_path} with {len(unique_rows)} unique rows")


if __name__ == "__main__":
    main()