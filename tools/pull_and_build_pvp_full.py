#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
End-to-end builder for PvPoke JSON -> single pvp_full.json

What it does:
  1) Clone pvpoke repo (unless --root provided)
  2) npm ci && node build.js   (unless --skip-build)
  3) Combine per-league JSONs into one: {little,great,ultra,master}

Usage examples:
  # Full auto: clone, build, combine 'overall' into pvp_full.json
  python tools/pull_and_build_pvp_full.py -o pvp_full.json

  # Multiple cups (overall + halloween)
  python tools/pull_and_build_pvp_full.py --cups overall,halloween -o pvp_full.json

  # Use an existing local pvpoke checkout and skip building
  python tools/pull_and_build_pvp_full.py --root /path/to/pvpoke --skip-build -o pvp_full.json
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

ISO_Z = "%Y-%m-%dT%H:%M:%SZ"

LEAGUE_CP = {
    "little": 500,
    "great": 1500,
    "ultra": 2500,
    "master": 10000,  # PvPoke uses 10000 for open Master
}

DEFAULT_REPO = "https://github.com/pvpoke/pvpoke.git"
DEFAULT_BRANCH = "master"

# -------------------- utils --------------------

def now_iso() -> str:
    return datetime.now(timezone.utc).strftime(ISO_Z)

def run(cmd: List[str], cwd: Optional[str] = None, check: bool = True) -> subprocess.CompletedProcess:
    print(f"[cmd] {' '.join(cmd)} (cwd={cwd or os.getcwd()})", file=sys.stderr)
    return subprocess.run(cmd, cwd=cwd, check=check)

def which(bin_name: str) -> Optional[str]:
    return shutil.which(bin_name)

def ensure_node_tools_available() -> None:
    if not which("node"):
        raise SystemExit("Node.js not found. Please install Node (>=16) and re-run.")
    if not which("npm"):
        raise SystemExit("npm not found. Please install npm and re-run.")

def clone_repo(repo_url: str, branch: str) -> str:
    tmpdir = tempfile.mkdtemp(prefix="pvpoke_clone_")
    try:
        run(["git", "clone", "--depth", "1", "-b", branch, repo_url, tmpdir])
    except subprocess.CalledProcessError:
        shutil.rmtree(tmpdir, ignore_errors=True)
        raise
    return tmpdir

def npm_ci_and_build(root: str) -> None:
    run(["npm", "ci"], cwd=root)
    run(["node", "build.js"], cwd=root)

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

def norm_row(e: Dict[str, Any], league_key: str, cp_cap: int, url: str, rank: int, cup: str) -> Dict[str, Any]:
    """
    Normalize a PvPoke row to the pipeline fields. Defensive to field variations.
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

    # Moves come in a few shapes; try common variants:
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

    # Optional notes; add cup name so you can tell combined sources apart
    notes = e.get("notes") or f"cup: {cup}"

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

def build_for_league_and_cup(root: str, cp_cap: int, cup: str, league_key: str) -> List[Dict[str, Any]]:
    rel = os.path.join("data", "rankings", "all", str(cp_cap), f"{cup}.json")
    path = os.path.join(root, rel)
    if not os.path.exists(path):
        print(f"[warn] missing file (league={league_key} cp={cp_cap} cup={cup}): {path}", file=sys.stderr)
        return []

    url_hint = f"https://pvpoke.com/rankings/all/{cp_cap}/{cup}/"
    data = read_json(path)

    # Typical shape: list of entries. Be defensive if it's wrapped.
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
        out.append(norm_row(e, league_key, cp_cap, url_hint, rank, cup))
    return out

def combine_pvpoke(root: str, cups: List[str]) -> Dict[str, List[Dict[str, Any]]]:
    result: Dict[str, List[Dict[str, Any]]] = {k: [] for k in LEAGUE_CP.keys()}
    total = 0
    for cup in cups:
        for lg, cp in LEAGUE_CP.items():
            rows = build_for_league_and_cup(root, cp, cup, lg)
            result[lg].extend(rows)
            total += len(rows)
            print(f"[info] {lg} ({cp})/{cup} -> +{len(rows)} (cum {sum(len(v) for v in result.values())})", file=sys.stderr)
    # Sort within leagues by rank then name
    for lg in result:
        result[lg].sort(key=lambda r: (r.get("rank") or 999999, r.get("name","").lower()))
    print(f"[info] total rows (all leagues): {total}", file=sys.stderr)
    return result

# -------------------- main --------------------

def main():
    ap = argparse.ArgumentParser(description="Clone/build PvPoke and produce a single pvp_full.json")
    ap.add_argument("--repo", default=DEFAULT_REPO, help=f"PvPoke git repo URL (default: {DEFAULT_REPO})")
    ap.add_argument("--branch", default=DEFAULT_BRANCH, help=f"PvPoke branch (default: {DEFAULT_BRANCH})")
    ap.add_argument("--root", default="", help="Existing local PvPoke checkout (skips clone)")
    ap.add_argument("--skip-build", action="store_true", help="Skip npm build step (use if data already generated)")
    ap.add_argument("--cups", default="overall", help="Comma-separated cups to include (e.g., overall,halloween)")
    ap.add_argument("-o", "--out", default="pvp_full.json", help="Output JSON path (default: pvp_full.json)")
    ap.add_argument("--keep-clone", action="store_true", help="Keep temp clone directory (debug)")
    args = ap.parse_args()

    cups = [c.strip() for c in args.cups.split(",") if c.strip()]
    if not cups:
        cups = ["overall"]

    temp_dir = None
    root = args.root.strip()

    if not root:
        # Need node/npm to build if we're cloning fresh
        ensure_node_tools_available()
        print("[info] cloning pvpoke repo...", file=sys.stderr)
        root = clone_repo(args.repo, args.branch)
        temp_dir = root
    else:
        print(f"[info] using existing pvpoke root: {root}", file=sys.stderr)

    # Build (unless skipped)
    if not args.skip_build:
        ensure_node_tools_available()
        print("[info] running npm ci && node build.js ...", file=sys.stderr)
        npm_ci_and_build(root)
    else:
        print("[info] skipping build step (--skip-build)", file=sys.stderr)

    # Combine per-league JSONs
    result = combine_pvpoke(root, cups)

    # Minimal sanity check
    if not any(len(v) >= 1 for v in result.values()):
        if temp_dir and not args.keep_clone:
            shutil.rmtree(temp_dir, ignore_errors=True)
        raise SystemExit("No leagues produced any rows. Did build.js complete successfully?")

    payload = {
        "_meta": {
            "generated_at": now_iso(),
            "source": "pvpoke (local build)",
            "cups": cups,
            "rows_total": sum(len(v) for v in result.values()),
        },
        **result,
    }

    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f"[ok] wrote {args.out}  (great={len(result['great'])}  ultra={len(result['ultra'])}  master={len(result['master'])}  little={len(result['little'])})")

    # Cleanup clone if we created one
    if temp_dir and not args.keep_clone:
        shutil.rmtree(temp_dir, ignore_errors=True)

if __name__ == "__main__":
    main()