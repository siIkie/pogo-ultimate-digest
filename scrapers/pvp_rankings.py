#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Scrape PvP rankings from PvPoke (multi-league, optional cups) and normalize to schemas/pvp.schema.json.

Outputs:
  outputs/pvp.json

Usage:
  python scrapers/pvp_rankings.py -o outputs/pvp.json \
    --leagues great,ultra,master \
    --cups overall

Notes:
- PvPoke publishes structured JSON per league/cup. We fetch the canonical "overall" for Great/Ultra/Master by default.
- You can add more cups with --cups (comma-separated). If a cup path 404s, we skip gracefully.
"""

from __future__ import annotations
import argparse, json, os, sys, time
from datetime import datetime, timezone
from typing import Dict, List, Any

import requests

ISO_Z = "%Y-%m-%dT%H:%M:%SZ"
HEADERS = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) Chrome/124 Safari/537.36"}
TIMEOUT = 30
SLEEP = 0.7

LEAGUE_CP = {
    "little": 500,
    "great": 1500,
    "ultra": 2500,
    "master": 10000
}

def now_iso() -> str:
    return datetime.now(timezone.utc).strftime(ISO_Z)

def get_json(url: str) -> Dict[str, Any]:
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"[warn] GET {url} failed: {e}", file=sys.stderr)
        return {}

def build_url(league: str, cup: str) -> str:
    """pvpoke: /data/rankings/<cup>/overall/rankings-<cp>.json
       canonical overall:   /data/rankings/all/overall/rankings-1500.json
       cups example:        /data/rankings/ultra/kanto/rankings-2500.json
    """
    cp = LEAGUE_CP.get(league, 1500)
    if cup in ("all", "overall", ""):
        return f"https://pvpoke.com/data/rankings/all/overall/rankings-{cp}.json"
    # cup may be like 'fantasy', 'element', 'remix-xy', etc.
    return f"https://pvpoke.com/data/rankings/{league}/{cup}/rankings-{cp}.json"

def scrape_pvpoke(league: str, cup: str) -> List[Dict[str, Any]]:
    url = build_url(league, cup)
    data = get_json(url)
    if not data:
        return []
    rows = []
    ts = now_iso()
    # pvpoke JSON commonly has {"rankings":[{speciesName, score, moves:{fastMoves,chargedMoves}, types, ...}]}
    rlist = data.get("rankings") or data  # some dumps may be just a list
    if isinstance(rlist, dict):
        rlist = rlist.get("rankings", [])
    if not isinstance(rlist, list):
        print(f"[warn] Unexpected JSON shape for {url}", file=sys.stderr)
        return []
    for i, row in enumerate(rlist, 1):
        species = row.get("speciesName") or row.get("pokemon") or ""
        form = row.get("formName") or row.get("form") or ""
        typing = row.get("types", [])
        moves = row.get("moves", {})
        # fallback if flat
        fast = []
        charge = []
        if isinstance(moves, dict):
            fast = [m.get("moveId", m) for m in moves.get("fastMoves", [])]
            charge = [m.get("moveId", m) for m in moves.get("chargedMoves", [])]
        else:
            # sometimes it's just lists:
            fast = row.get("fastMoves", []) or []
            charge = row.get("chargedMoves", []) or []
        rows.append({
            "league": league.lower(),
            "cup": cup.lower() if cup else "overall",
            "pokemon": species,
            "form": form,
            "typing": typing if isinstance(typing, list) else [],
            "fast_moves": fast,
            "charge_moves": charge,
            "score": row.get("score"),
            "rank": i,
            "source": "pvpoke",
            "url": url,
            "ts": ts
        })
    return rows

def main():
    ap = argparse.ArgumentParser(description="Scrape PvP rankings (PvPoke)")
    ap.add_argument("-o", "--out", default="outputs/pvp.json", help="Output JSON path")
    ap.add_argument("--leagues", default="great,ultra,master", help="Comma-separated leagues (little,great,ultra,master)")
    ap.add_argument("--cups", default="overall", help="Comma-separated cups per league (e.g., overall,remix,fantasy)")
    args = ap.parse_args()

    leagues = [x.strip().lower() for x in args.leagues.split(",") if x.strip()]
    cups = [x.strip().lower() for x in args.cups.split(",") if x.strip()]

    all_rows: List[Dict[str, Any]] = []
    for lg in leagues:
        for cup in cups:
            rows = scrape_pvpoke(lg, cup)
            print(f"[info] {lg}/{cup}: {len(rows)} rows", file=sys.stderr)
            all_rows.extend(rows)
            time.sleep(SLEEP)

    payload = {
        "_meta": {"generated_at": now_iso(), "source": "pvpoke", "rows": len(all_rows)},
        "rankings": all_rows
    }
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f"[ok] wrote {args.out} with {len(all_rows)} rows")

if __name__ == "__main__":
    main()