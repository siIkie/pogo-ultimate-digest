#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Scrape external event & raid boss info (LeekDuck) and normalize.

Outputs:
  outputs/events_external.json

Shape:
{
  "_meta": {...},
  "events": [ ... ],          # event title, date(s), summary, url
  "raid_bosses": [ ... ]      # boss name, tier, shiny, regionals, url
}

Notes:
- HTML parsers are best-effort. We try multiple selector patterns and regex fallbacks.
- Dates are kept as strings; your normalize step can parse to YYYY-MM-DD where possible.
"""

from __future__ import annotations
import json, os, re, sys, time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

ISO_Z = "%Y-%m-%dT%H:%M:%SZ"
HEADERS = {"User-Agent":"Mozilla/5.0 (X11; Linux x86_64) Chrome/124 Safari/537.36"}
TIMEOUT = 30
SLEEP = 0.7

def now_iso() -> str:
    return datetime.now(timezone.utc).strftime(ISO_Z)

def get_html(url: str) -> Optional[str]:
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        if r.status_code >= 400:
            print(f"[warn] GET {url} -> {r.status_code}", file=sys.stderr)
            return None
        return r.text
    except Exception as e:
        print(f"[warn] GET {url} failed: {e}", file=sys.stderr)
        return None

def soupify(html: Optional[str]) -> Optional[BeautifulSoup]:
    if not html: return None
    try:
        return BeautifulSoup(html, "lxml")
    except Exception:
        return BeautifulSoup(html, "html.parser")

def text(n) -> str:
    if not n: return ""
    return " ".join(n.get_text(" ", strip=True).split())

def parse_event_cards(s: BeautifulSoup) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    # Common LeekDuck patterns: article cards in .cards or .posts; headers and date ribbons
    cards = s.select(".cards .card, article.post, .post, .event, .event-item, .card")
    if not cards:
        # fallback: any link blocks
        cards = s.select("a, li")
    for c in cards:
        title = ""
        tnode = c.select_one("h2, h3, .title, .card-title, header h2, header h3, .entry-title")
        if tnode:
            title = text(tnode)
        if not title:
            # fallback: first strong text in the block
            b = c.find("strong")
            if b: title = text(b)
        # skip noise
        if not title or len(title) < 4:
            continue
        # Date text heuristics
        raw_dates = []
        for dn in c.select(".date, .dates, time, .event-date, .meta, .meta-date"):
            dtxt = text(dn)
            if dtxt and len(dtxt) >= 4:
                raw_dates.append(dtxt)
        raw_dates = list(dict.fromkeys(raw_dates))  # dedupe
        # summary/snippet
        summary = ""
        sn = c.select_one(".summary, .excerpt, .content, .entry-content, p")
        if sn:
            summary = text(sn)[:400]
        # link
        href = ""
        a = c.find("a")
        if a and a.get("href"):
            href = a["href"]
            if href.startswith("/"):
                href = "https://leekduck.com" + href
        events.append({
            "title": title,
            "raw_dates": raw_dates,
            "summary": summary,
            "source": "leekduck",
            "url": href
        })
    return events

def parse_raid_bosses(s: BeautifulSoup) -> List[Dict[str, Any]]:
    bosses: List[Dict[str, Any]] = []
    # LeekDuck boss page groups by tier w/ images + captions or tables
    sections = s.select("h2, h3, .tier-title, .entry-content h2")
    current_tier = ""
    for sec in sections:
        tl = text(sec).lower()
        if re.search(r"(tier|mega|shadow|five|one|three)", tl):
            current_tier = text(sec)
        # next siblings might contain lists
        container = sec.find_next_sibling()
        sweep = []
        if container:
            sweep.append(container)
            sweep.extend(container.select("*"))
        for node in sweep:
            if not getattr(node, "name", None): continue
            # candidate boss item
            if node.name in ("li","div","p","tr"):
                t = text(node)
                if not t or len(t) < 3: 
                    continue
                # name:
                name = ""
                im = node.find("img")
                if im and im.get("alt"): name = im["alt"]
                if not name:
                    a = node.find("a")
                    if a: name = text(a)
                if not name:
                    # fallback from t
                    m = re.match(r"^([A-Za-z0-9' \.\-:]+)", t)
                    if m: name = m.group(1).strip()
                if not name or len(name) < 3:
                    continue
                # flags
                shiny = bool(re.search(r"shiny", t.lower()))
                regional = bool(re.search(r"regional", t.lower()))
                bosses.append({
                    "name": name,
                    "tier_label": current_tier,
                    "shiny": shiny,
                    "regional": regional,
                    "source": "leekduck",
                    "url": "https://leekduck.com/boss/"
                })
    # dedupe by (name,tier_label)
    seen = set()
    out = []
    for b in bosses:
        k = (b["name"].lower(), (b.get("tier_label") or "").lower())
        if k in seen: continue
        seen.add(k)
        out.append(b)
    return out

def main():
    events_url = "https://leekduck.com/events/"
    bosses_url = "https://leekduck.com/boss/"
    events_html = get_html(events_url); time.sleep(SLEEP)
    bosses_html = get_html(bosses_url); time.sleep(SLEEP)

    events = []
    if events_html:
        s = soupify(events_html)
        if s: events = parse_event_cards(s)

    raid_bosses = []
    if bosses_html:
        s = soupify(bosses_html)
        if s: raid_bosses = parse_raid_bosses(s)

    payload = {
        "_meta": {"generated_at": now_iso(), "sources": ["leekduck"]},
        "events": events,
        "raid_bosses": raid_bosses
    }
    os.makedirs("outputs", exist_ok=True)
    with open("outputs/events_external.json", "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f"[ok] wrote outputs/events_external.json events={len(events)} bosses={len(raid_bosses)}")

if __name__ == "__main__":
    main()