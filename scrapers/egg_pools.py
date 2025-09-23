#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Scrape egg hatch pools (LeekDuck) and normalize to schemas/eggs.schema.json.

Outputs:
  outputs/eggs.json

We try multiple patterns: headings by egg distance + lists/tables of Pokémon.
"""

from __future__ import annotations
import json, os, re, sys, time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests
from bs4 import BeautifulSoup

ISO_Z="%Y-%m-%dT%H:%M:%SZ"
HEADERS={"User-Agent":"Mozilla/5.0 (X11; Linux x86_64) Chrome/124 Safari/537.36"}
TIMEOUT=30
SLEEP=0.7

def now_iso() -> str:
    return datetime.now(timezone.utc).strftime(ISO_Z)

def get_html(url:str)->Optional[str]:
    try:
        r=requests.get(url,headers=HEADERS,timeout=TIMEOUT)
        if r.status_code>=400:
            print(f"[warn] GET {url} -> {r.status_code}",file=sys.stderr); return None
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

def parse_distance(h: str) -> Optional[float]:
    m=re.search(r"(\d+(?:\.\d+)?)\s*km", h.lower())
    return float(m.group(1)) if m else None

def parse_egg_pools(s: BeautifulSoup)->List[Dict[str,Any]]:
    pools=[]
    # Find sections by header containing "km Eggs"
    headers=s.select("h2, h3, .entry-content h2, .entry-content h3")
    for h in headers:
        ttl=text(h)
        if not ttl or "egg" not in ttl.lower(): 
            continue
        dist=parse_distance(ttl)
        if dist is None: 
            continue
        # items under this header
        entries=[]
        # search following siblings until next header
        nxt=h.find_next_sibling()
        while nxt and nxt.name not in ("h2","h3"):
            # list items, images with alt, table rows
            for it in nxt.select("li, .mon, tr, .pokemon, .entry, .card"):
                t=text(it)
                if not t or len(t)<2: 
                    continue
                name=""
                a=it.find("a"); im=it.find("img")
                if a and text(a): name=text(a)
                if not name and im and im.get("alt"): name=im["alt"]
                if not name:
                    m=re.match(r"^([A-Za-z0-9' \-\.]+)", t)
                    if m: name=m.group(1).strip()
                if not name or len(name)<2: 
                    continue
                shiny=bool(re.search(r"shiny", t.lower()))
                rarity=None
                if re.search(r"(rare|common|uncommon|tier\s*\d)", t.lower()):
                    m2=re.search(r"(rare|uncommon|common|tier\s*\d)", t.lower())
                    if m2: rarity=m2.group(1)
                entries.append({"pokemon":name,"form":"","shiny":shiny,"rarity":rarity,"notes":t})
            nxt=nxt.find_next_sibling()
        if entries:
            pools.append({"distance_km":dist,"category":"standard","entries":dedupe_entries(entries),"source":"leekduck","url":"https://leekduck.com/eggs/","ts":now_iso()})
    return pools

def dedupe_entries(entries: List[Dict[str,Any]])->List[Dict[str,Any]]:
    seen=set(); out=[]
    for e in entries:
        k=(e["pokemon"].lower(), (e.get("form") or "").lower())
        if k in seen: continue
        seen.add(k); out.append(e)
    return out

def main():
    url="https://leekduck.com/eggs/"
    html=get_html(url); time.sleep(SLEEP)
    pools=[]
    if html:
        s=soupify(html)
        if s: pools=parse_egg_pools(s)
    payload={"_meta":{"generated_at":now_iso(),"source":"leekduck"},"pools":pools}
    os.makedirs("outputs",exist_ok=True)
    with open("outputs/eggs.json","w",encoding="utf-8") as f:
        json.dump(payload,f,ensure_ascii=False,indent=2)
    print(f"[ok] wrote outputs/eggs.json pools={len(pools)}")

if __name__=="__main__":
    main()