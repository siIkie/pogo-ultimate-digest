#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Scrape Field Research tasks (LeekDuck) and normalize to schemas/research.schema.json.

Outputs:
  outputs/research.json

We parse the visible table/cards; if layout changes, heuristics keep it non-fatal.
"""

from __future__ import annotations
import json, os, re, sys, time
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

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
    try: return BeautifulSoup(html, "lxml")
    except Exception: return BeautifulSoup(html, "html.parser")

def text(n) -> str:
    if not n: return ""
    return " ".join(n.get_text(" ", strip=True).split())

def parse_tasks(s: BeautifulSoup) -> List[Dict[str, Any]]:
    tasks: List[Dict[str, Any]] = []
    # attempt to find table rows or list items grouping research tasks
    rows = s.select("table tr, .tasks li, .entry-content li, .card, .task")
    for r in rows:
        t = text(r)
        if not t or len(t) < 5: 
            continue
        # Heuristic: split "Task — Reward" or "Task: Reward"
        parts = re.split(r"\s+[—\-:]\s+", t, maxsplit=1)
        task = parts[0].strip()
        reward = parts[1].strip() if len(parts) > 1 else ""
        # Encounters:
        enc = ""
        m = re.search(r"(encounter|reward:)\s*([A-Za-z0-9' \-]+)", t, re.IGNORECASE)
        if m: enc = m.group(2).strip()
        # Category guesses:
        cat = ""
        for h in r.parents:
            if not getattr(h, "name", None): continue
            if h.name in ("section","div","article"):
                hdr = h.find_previous_sibling("h2") or h.find("h2") or h.find("h3")
                if hdr:
                    cat = text(hdr)
                    if cat: break
        shiny = bool(re.search(r"shiny", t.lower()))
        tasks.append({
            "category": cat,
            "task": task,
            "reward": reward,
            "encounter": enc,
            "shiny": shiny,
            "notes": "",
            "source": "leekduck",
            "url": "https://leekduck.com/research/"
        })
    # dedupe by (task,reward,encounter)
    seen=set(); out=[]
    for x in tasks:
        k=(x["task"].lower(), x.get("reward","").lower(), x.get("encounter","").lower())
        if k in seen: continue
        seen.add(k); out.append(x)
    return out

def main():
    url="https://leekduck.com/research/"
    html=get_html(url); time.sleep(SLEEP)
    tasks=[]
    if html:
        s=soupify(html)
        if s:
            tasks=parse_tasks(s)
    payload={"_meta":{"generated_at":now_iso(),"source":"leekduck"},"tasks":tasks}
    os.makedirs("outputs",exist_ok=True)
    with open("outputs/research.json","w",encoding="utf-8") as f:
        json.dump(payload,f,ensure_ascii=False,indent=2)
    print(f"[ok] wrote outputs/research.json tasks={len(tasks)}")

if __name__=="__main__":
    main()