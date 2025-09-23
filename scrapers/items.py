#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Scrape items/bonuses (Niantic Help Center), with a static seed list as safety net.

Outputs:
  outputs/items.json

Approach:
- Crawl the Help Center category page and pull article titles that look like items/bonuses.
- Fallback to a curated static list (so the pipeline never fails).
"""

from __future__ import annotations
import json, os, re, sys, time
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

import requests
from bs4 import BeautifulSoup

ISO_Z="%Y-%m-%dT%H:%M:%SZ"
HEADERS={"User-Agent":"Mozilla/5.0 (X11; Linux x86_64) Chrome/124 Safari/537.36"}
TIMEOUT=30
SLEEP=0.7

HELP_ROOT = "https://niantic.helpshift.com/a/pokemon-go/"
# Some Help Center pages are client-rendered; but the index still exposes article links.

STATIC_SEED = [
    {"name":"Lucky Egg",      "category":"bonus","effects":["Double XP for 30 minutes"], "stacking": None, "notes":""},
    {"name":"Incense",        "category":"bonus","effects":["Increased spawns near you"], "stacking": None, "notes":""},
    {"name":"Star Piece",     "category":"bonus","effects":["+50% Stardust for 30 minutes"], "stacking": None, "notes":""},
    {"name":"Lure Module",    "category":"lure","effects":["Increased spawns at a PokéStop"], "stacking": None, "notes":""},
    {"name":"Premium Raid Pass","category":"pass","effects":["Access in-person raid"], "stacking": None, "notes":""},
    {"name":"Remote Raid Pass","category":"pass","effects":["Access remote raid"], "stacking": None, "notes":""},
]

def now_iso()->str: return datetime.now(timezone.utc).strftime(ISO_Z)

def http_html(url:str)->Optional[str]:
    try:
        r=requests.get(url,headers=HEADERS,timeout=TIMEOUT)
        if r.status_code>=400:
            print(f"[warn] GET {url} -> {r.status_code}", file=sys.stderr); return None
        return r.text
    except Exception as e:
        print(f"[warn] GET {url} failed: {e}",file=sys.stderr); return None

def soupify(html: Optional[str])->Optional[BeautifulSoup]:
    if not html: return None
    try: return BeautifulSoup(html,"lxml")
    except Exception: return BeautifulSoup(html,"html.parser")

def text(n)->str:
    if not n: return ""
    return " ".join(n.get_text(" ",strip=True).split())

def parse_helpcenter_items(s: BeautifulSoup)->List[Dict[str,Any]]:
    items=[]
    # look for anchor lists in the category index
    anchors=s.select("a")
    for a in anchors:
        lbl=text(a).strip()
        if not lbl or len(lbl)<3: continue
        # Heuristic: include “Incense”, “Lucky Egg”, “Star Piece”, “Lure”, “Raid Pass”, etc.
        if re.search(r"(incense|lure|lucky egg|star piece|raid pass|remote raid|premium raid|incubator|magnetic lure|mossy lure|glacial lure|golden lure)", lbl, re.IGNORECASE):
            href=a.get("href") or ""
            if href.startswith("/"):
                href="https://niantic.helpshift.com"+href
            items.append({
                "name": lbl,
                "category": guess_category(lbl),
                "effects": [],
                "stacking": None,
                "notes": "",
                "source": "niantic_help",
                "url": href,
                "ts": now_iso()
            })
    # dedupe by name
    seen=set(); out=[]
    for it in items:
        k=it["name"].lower()
        if k in seen: continue
        seen.add(k); out.append(it)
    return out

def guess_category(lbl: str)->str:
    s=lbl.lower()
    if "lure" in s: return "lure"
    if "pass" in s: return "pass"
    if "egg" in s: return "bonus"
    if "incense" in s: return "bonus"
    if "star piece" in s: return "bonus"
    if "incubator" in s: return "item"
    return "item"

def main():
    html=http_html(HELP_ROOT); time.sleep(SLEEP)
    items=[]
    if html:
        s=soupify(html)
        if s:
            items=parse_helpcenter_items(s)

    if not items:
        # Fallback to static seeds so pipeline always emits usable data
        ts=now_iso()
        items=[{
            "name": it["name"],
            "category": it["category"],
            "effects": it["effects"],
            "stacking": it["stacking"],
            "notes": it["notes"],
            "source":"static",
            "url":"",
            "ts": ts
        } for it in STATIC_SEED]

    payload={"_meta":{"generated_at":now_iso(),"sources":["niantic_help","static"]},"items":items}
    os.makedirs("outputs",exist_ok=True)
    with open("outputs/items.json","w",encoding="utf-8") as f:
        json.dump(payload,f,ensure_ascii=False,indent=2)
    print(f"[ok] wrote outputs/items.json items={len(items)}")

if __name__=="__main__":
    main()