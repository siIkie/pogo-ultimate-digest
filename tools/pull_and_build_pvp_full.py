#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
pull_and_build_pvp_full.py

Clones PvPoke into a temp dir, installs deps, runs PvPoke's Node build,
and combines ALL league/cup JSONs into one outputs/pvp_full.json.

Output (league-keyed lists):
{
  "_meta": {...},
  "little": [ { name, form, league, cp_cap, fast_move, charge_move_1, charge_move_2,
                source: "pvpoke", rank, score, score_kind, notes: "cup: <cup>", url, ts }, ... ],
  "great":  [ ... ],
  "ultra":  [ ... ],
  "master": [ ... ]
}
"""

import json
import pathlib
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from typing import Any, Dict, List

REPO_URL = "https://github.com/pvpoke/pvpoke.git"

LEAGUE_CP = {
    "little": 500,
    "great": 1500,
    "ultra": 2500,
    "master": 10000,  # PvPoke uses 10000 for open Master
}

# --------------- utils ----------------

def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def run(cmd, cwd=None):
    print("[cmd]", " ".join(cmd))
    subprocess.run(cmd, cwd=cwd, check=True)

def read_json(path: pathlib.Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))

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

# --------------- build PvPoke ----------------

def build_pvpoke(tmpdir: str) -> str:
    """
    Clone PvPoke into tmpdir/pvpoke and run npm install + build.
    Returns the repo path that contains package.json.
    """
    repo_dir = pathlib.Path(tmpdir) / "pvpoke"
    print("[info] Cloning PvPoke…")
    run(["git", "clone", "--depth=1", REPO_URL, str(repo_dir)])

    pkg = repo_dir / "package.json"
    if not pkg.exists():
        print("[error] package.json not found in:", repo_dir)
        try:
            print("[debug] ls of repo_dir:")
            for p in repo_dir.iterdir():
                print("  -", p.name)
        except Exception:
            pass
        raise SystemExit("PvPoke clone missing package.json — aborting.")

    print("[info] Installing PvPoke deps…")
    lock = repo_dir / "package-lock.json"
    if lock.exists():
        try:
            run(["npm", "ci"], cwd=str(repo_dir))
        except subprocess.CalledProcessError:
            print("[warn] npm ci failed; falling back to npm install")
            run(["npm", "install"], cwd=str(repo_dir))
    else:
        run(["npm", "install"], cwd=str(repo_dir))

    print("[info] Building PvPoke (node build.js)…")
    run(["node", "build.js"], cwd=str(repo_dir))
    return str(repo_dir)

# --------------- normalize rows ----------------

def norm_row(e: Dict[str, Any], league_key: str, cp_cap: int, url: str, rank: int, cup: str) -> Dict[str, Any]:
    """Normalize a PvPoke row into pipeline fields (defensive on field names)."""
    name = (
        e.get("speciesName")
        or e.get("name")
        or e.get("pokemon")
        or e.get("speciesId")
        or ""
    )
    form = e.get("form") or ""
    rating = e.get("rating") or e.get("score") or None

    # Moves (handle common shapes)
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
        "notes": f"cup: {cup}",
        "url": url,
        "ts": now_iso(),
    }

# --------------- collect cups/leagues ----------------

def collect_all_cups_for_league(pvpoke_root: pathlib.Path, league: str, cp_cap: int) -> List[Dict[str, Any]]:
    """
    PvPoke stores JSON: data/rankings/all/<CP>/<cup>.json
    We'll ingest ALL *.json cups (including 'overall.json') for this league.
    """
    cup_dir = pvpoke_root / "data" / "rankings" / "all" / str(cp_cap)
    if not cup_dir.exists():
        print(f"[warn] league dir missing: {cup_dir}")
        return []

    rows_out: List[Dict[str, Any]] = []
    files = sorted([p for p in cup_dir.glob("*.json") if p.is_file()],
                   key=lambda p: (p.name != "overall.json", p.name))

    for jf in files:
        cup = jf.stem  # 'overall', 'halloween', etc.
        url_hint = f"https://pvpoke.com/rankings/all/{cp_cap}/{cup}/"
        raw = read_json(jf)
        if isinstance(raw, dict) and isinstance(raw.get("data"), list):
            entries = raw["data"]
        elif isinstance(raw, list):
            entries = raw
        else:
            print(f"[warn] unexpected JSON shape at {jf}")
            continue

        rank = 0
        for e in entries:
            if not isinstance(e, dict):
                continue
            rank += 1
            rows_out.append(norm_row(e, league, cp_cap, url_hint, rank, cup))

        print(f"[ok] {league}/{cup}: +{len(entries)} rows")

    rows_out.sort(key=lambda r: (r.get("notes", ""), r.get("rank", 999999), r.get("name", "").lower()))
    return rows_out

def combine_all_leagues(pvpoke_root: str) -> Dict[str, List[Dict[str, Any]]]:
    root = pathlib.Path(pvpoke_root)
    combined: Dict[str, List[Dict[str, Any]]] = {k: [] for k in LEAGUE_CP.keys()}
    total = 0
    for lg, cp in LEAGUE_CP.items():
        league_rows = collect_all_cups_for_league(root, lg, cp)
        combined[lg] = league_rows
        total += len(league_rows)
    print(f"[info] total combined rows across leagues: {total}")
    return combined

# --------------- main ----------------

def main():
    # parse --output
    out_idx = sys.argv.index("--output") + 1 if "--output" in sys.argv else -1
    out_path = sys.argv[out_idx] if out_idx > 0 else "outputs/pvp_full.json"

    tmpdir = tempfile.mkdtemp(prefix="pvpoke-")
    try:
        repo_path = build_pvpoke(tmpdir)               # returns path with package.json
        combined = combine_all_leagues(repo_path)      # use repo_path, not tmpdir

        if not any(len(v) for v in combined.values()):
            raise SystemExit("No leagues produced any rows. Did build.js complete successfully?")

        payload = {
            "_meta": {
                "generated_at": now_iso(),
                "source": "pvpoke (auto build)",
                "leagues": list(LEAGUE_CP.keys()),
                "cups": "all",
            },
            **combined,
        }

        out_file = pathlib.Path(out_path)
        out_file.parent.mkdir(parents=True, exist_ok=True)
        out_file.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[done] wrote {out_file}  "
              f"(great={len(combined['great'])} ultra={len(combined['ultra'])} "
              f"master={len(combined['master'])} little={len(combined['little'])})")
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)

if __name__ == "__main__":
    main()