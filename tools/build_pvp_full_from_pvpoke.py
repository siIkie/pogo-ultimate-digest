#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Combine PvPoke per-league rankings into one JSON:
  { "little": [...], "great": [...], "ultra": [...], "master": [...] }

Input (from a local clone of https://github.com/pvpoke/pvpoke after `node build.js`):
  <root>/data/rankings/all/500/<cup>.json
  <root>/data/rankings/all/1500/<cup>.json
  <root>/data/rankings/all/2500/<cup>.json
  <root>/data/rankings/all/10000/<cup>.json

Default cup: "overall" (matches open leagues).

Output:
  pvp_full.json (league-keyed dict of normalized rows)
"""

from __future__ import annotations

import argparse, json, os, sys, time
from datetime import datetime, timezone
from typing import Any, Dict, List

ISO_Z = "%Y-%m-%dT%H:%M:%SZ"

LEAGUE_CP = {
    "little": 500,
    "great": 1500,
    "ultra": 2500,
    "master": 10000,  # PvPoke uses 10000 for open Master
}

def now_iso() -> str:
    return datetime.now(timezone.utc).strftime(ISO_Z)

def read_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def first_str(x) -> str:
    if isinstance(x, list) and x:
        return str(x[0])
    if isinstance(x, str):
        return x
    return ""

def second_str(x) -> str:
    if isinstance(x, list) and len(x) >= 2:
        return str(x[1])
    return ""

def norm_row(e: Dict[str, Any], league_key: str, cp_cap: int, url: str, rank: int) -> Dict[str, Any]:
    """
    Normalize a PvPoke row into the fields our pipeline expects.
    We’re defensive about field names; PvPoke variations are handled.
    """
    name = (
        e.get("speciesName")
        or e.get("name")
        or e.get("pokemon")
        or e.get("speciesId")
        or ""
    )
    form = e.get("form") or ""
    rating = e.get("rating") or e.get("score") or None

    # Moves can appear under different shapes; try common ones:
    fast_move = (
        e.get("fastMove")
        or e.get("fast_move")
        or (e.get("moves", {}).get("fast") if isinstance(e.get("moves"), dict) else None)
        or first_str(e.get("fastMoves"))
        or first_str(e.get("fast_moves"))
        or ""
    )
    charged_list = (
        (e.get("moves", {}).get("charged") if isinstance(e.get("moves"), dict) else None)
        or e.get("chargedMoves")
        or e.get("charged_moves")
        or []
    )
    charge_move_1 = first_str(charged_list)
    charge_move_2 = second_str(charged_list)

    # Optional notes if present
    notes = e.get("notes") or ""

    return {
        "name": str(name),
        "form": str(form),
        "league": league_key,
        "cp_cap": int(cp_cap),
        "fast_move": str(fast_move),
        "charge_move_1": str(charge_move_1),
        "charge_move_2": str(charge_move_2),
        "source": "pvpoke",
        "rank": int(rank),
        "score": float(rating) if isinstance(rating, (int, float)) else None,
        "score_kind": "rating" if rating is not None else "",
        "notes": notes,
        "url": url,
        "ts": now_iso(),
    }

def build_for_league(root: str, cp_cap: int, cup: str, league_key: str) -> List[Dict[str, Any]]:
    rel = os.path.join("data", "rankings", "all", str(cp_cap), f"{cup}.json")
    path = os.path.join(root, rel)
    if not os.path.exists(path):
        print(f"[warn] missing: {path}", file=sys.stderr)
        return []

    url_hint = f"https://pvpoke.com/rankings/all/{cp_cap}/{cup}/"
    data = read_json(path)

    # PvPoke typical shape: list of entries. Be defensive if it's wrapped.
    if isinstance(data, dict) and "data" in data and isinstance(data["data"], list):
        rows = data["data"]
    elif isinstance(data, list):
        rows = data
    else:
        print(f"[warn] unexpected JSON shape at {path}", file=sys.stderr)
        rows = []

    out: List[Dict[str, Any]] = []
    rank = 0
    for e in rows:
        if not isinstance(e, dict):
            continue
        rank += 1
        out.append(norm_row(e, league_key, cp_cap, url_hint, rank))

    return out

def main():
    ap = argparse.ArgumentParser(description="Combine PvPoke league JSONs into a single pvp_full.json")
    ap.add_argument("--root", required=True, help="Path to local pvpoke repo (after running `node build.js`)")
    ap.add_argument("--cup", default="overall", help="Cup key (default: overall)")
    ap.add_argument("-o", "--out", default="pvp_full.json", help="Output JSON path")
    args = ap.parse_args()

    leagues = {
        "little": LEAGUE_CP["little"],
        "great":  LEAGUE_CP["great"],
        "ultra":  LEAGUE_CP["ultra"],
        "master": LEAGUE_CP["master"],
    }

    result: Dict[str, List[Dict[str, Any]]] = {k: [] for k in leagues.keys()}

    total = 0
    for lg, cp in leagues.items():
        rows = build_for_league(args.root, cp, args.cup, lg)
        result[lg] = rows
        total += len(rows)
        print(f"[info] {lg} ({cp}) -> {len(rows)} rows", file=sys.stderr)

    # Minimal sanity check: at least one league populated
    if not any(len(v) >= 1 for v in result.values()):
        raise SystemExit("No leagues produced any rows. Did you run `node build.js` in the PvPoke repo?")

    # Add a simple _meta
    payload = {
        "_meta": {
            "generated_at": now_iso(),
            "source": "pvpoke (local build)",
            "cup": args.cup,
            "rows_total": total,
        },
        **result,
    }

    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f"[ok] wrote {args.out} (total rows: {total})")

if __name__ == "__main__":
    main()