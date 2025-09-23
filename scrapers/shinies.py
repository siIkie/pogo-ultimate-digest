#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Scrape shiny availability:
- Primary: community-maintained JSON (GitHub)
- Fallback: LeekDuck shiny page (HTML heuristic)

Outputs:
  outputs/shinies.json
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
SLEEP=0.6

PRIMARY_URL = "https://raw.githubusercontent.com/pokemongo-dev-contrib/shiny-checklist/main/shinies.json"
FALLBACK_URL = "https://leekduck.com/shiny/"

def now_iso()->str: return datetime.now(timezone.utc).strftime(ISO_Z)

def http_json(url:str)->Optional[List[Dict[str,Any]]]:
    try:
        r=requests.get(url,headers=HEADERS,timeout=TIMEOUT)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"[warn] GET JSON {url} failed: {e}",file=sys.stderr); return None

def http_html(url:str)->Optional[str]:
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

def from_primary()->List[Dict[str,Any]]:
    data=http_json(PRIMARY_URL)
    if not data: return []
    out=[]
    ts=now_iso()
    for mon in data:
        out.append({
            "pokemon": mon.get("name") or mon.get("pokemon") or "",
            "form": mon.get("form",""),
            "available": bool(mon.get("shiny", mon.get("available", False))),
            "date_released": mon.get("releaseDate") or mon.get("released"),
            "methods": mon.get("methods", []) if isinstance(mon.get("methods", []), list) else [],
            "notes": mon.get("notes",""),
            "source": "github-shiny-checklist",
            "url": PRIMARY_URL,
            "ts": ts
        })
    return out

def from_fallback()->List[Dict[str,Any]]:
    html=http_html(FALLBACK_URL); time.sleep(SLEEP)
    if not html: return []
    s=soupify(html)
    if not s: return []
    out=[]; ts=now_iso()
    # heuristic: shinies listed as cards/list with names + icons
    items=s.select(".card, .shiny, li, .entry-content li, .pokemon")
    for it in items:
        t=text(it)
        if not t or len(t)<2: continue
        name=""
        img=it.find("img"); a=it.find("a"); b=it.find("strong")
        if img and img.get("alt"): name=img["alt"]
        if not name and a and text(a): name=text(a)
        if not name and b and text(b): name=text(b)
        if not name:
            m=re.match(r"^([A-Za-z0-9' \-\.]+)", t); 
            if m: name=m.group(1).strip()
        if not name or len(name)<2: continue
        out.append({
            "pokemon": name,
            "form": "",
            "available": True,   # page usually lists shinies that exist
            "date_released": None,
            "methods": [],
            "notes": t,
            "source": "leekduck",
            "url": FALLBACK_URL,
            "ts": ts
        })
    # dedupe by pokemon+form
    seen=set(); ded=[]
    for r in out:
        k=(r["pokemon"].lower(), r.get("form","").lower())
        if k in seen: continue
        seen.add(k); ded.append(r)
    return ded

def main():
    shinies = from_primary()
    if not shinies:
        print("[warn] primary shiny dataset unavailable; using fallback", file=sys.stderr)
        shinies = from_fallback()
    payload={"_meta":{"generated_at":now_iso(),"sources":["github","leekduck"]},"shinies":shinies}
    os.makedirs("outputs",exist_ok=True)
    with open("outputs/shinies.json","w",encoding="utf-8") as f:
        json.dump(payload,f,ensure_ascii=False,indent=2)
    print(f"[ok] wrote outputs/shinies.json shinies={len(shinies)}")

if __name__=="__main__":
    main()