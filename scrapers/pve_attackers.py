#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Scrape and normalize PvE (raid) attacker rankings from multiple sources.

Sources (best-effort HTML parsing; resilient to minor layout shifts):
  - Pokebattler  : https://www.pokebattler.com/raids
  - GamePress    : https://gamepress.gg/pokemongo/
  - GO Hub       : https://pokemongohub.net/

Output (JSON):
  outputs/attackers.json

Normalized record schema (one row per (pokemon, fast_move, charge_move, type_bucket) tuple):
{
  "name": "Reshiram",
  "form": "Standard" | "Shadow" | "Mega" | "Primal" | "Gigantamax" | "",
  "type_bucket": "fire",   # primary element bucket if known (lowercase)
  "fast_move": "Fire Fang",
  "charge_move": "Fusion Flare",
  "source": "pokebattler" | "gamepress" | "gohub",
  "rank": 1,               # best rank observed in that source/list
  "score": 95.2,           # arbitrary score if present (DPS/TDO, ranking score, etc.)
  "score_kind": "rating" | "dps" | "composite" | "",
  "notes": "Top fire attacker",
  "url": "https://…",
  "ts": "2025-09-23T00:00:00Z"
}

CLI:
  python scrapers/pve_attackers.py -o outputs/attackers.json [--types fire,water,...]

Notes:
- This is a best-effort scraper (sites can change). It errs on being permissive:
  - Gracefully continues if a source fails or returns unexpected HTML.
  - Tries multiple selector patterns and regex fallbacks.
- If you want to adjust selectors, see the SOURCE_PATTERNS dict at the bottom.
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

import requests
from bs4 import BeautifulSoup

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
HEADERS = {"User-Agent": USER_AGENT, "Accept": "text/html,application/xhtml+xml"}

DEFAULT_TIMEOUT = 20
SLEEP_BETWEEN_REQUESTS = 1.0  # polite delay between sites

ISO_Z = "%Y-%m-%dT%H:%M:%SZ"

# ------------------------
# Utilities
# ------------------------

def now_iso() -> str:
    return datetime.now(timezone.utc).strftime(ISO_Z)

def http_get(url: str, params: Optional[Dict[str, Any]] = None) -> Optional[str]:
    try:
        r = requests.get(url, headers=HEADERS, params=params, timeout=DEFAULT_TIMEOUT)
        if r.status_code >= 400:
            print(f"[warn] GET {url} -> {r.status_code}", file=sys.stderr)
            return None
        # Some sites block non-browser UA; we set a realistic UA, still respect robots and be gentle.
        return r.text
    except Exception as e:
        print(f"[warn] GET {url} failed: {e}", file=sys.stderr)
        return None

def soupify(html: Optional[str]) -> Optional[BeautifulSoup]:
    if not html:
        return None
    try:
        return BeautifulSoup(html, "lxml")
    except Exception:
        # fallback parser
        return BeautifulSoup(html, "html.parser")

def text(node) -> str:
    if not node:
        return ""
    return " ".join(node.get_text(" ", strip=True).split())

def norm_space(s: str) -> str:
    return " ".join((s or "").split())

def guess_form(name: str) -> str:
    n = name.lower()
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
    """
    Try to pull (fast, charge) moves from strings like:
      'Fire Fang + Fusion Flare' OR 'Fire Spin / Blast Burn'
    """
    s = s.replace("/", "+")
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
    """
    Very light type bucket guesser by name hints.
    The PvE sources often publish type-siloed lists (e.g., "Best Fire Attackers").
    """
    h = (hint or "").lower()
    if not h:
        return ""
    # pick the first type-like word we see in the hint
    TYPES = [
        "bug","dark","dragon","electric","fairy","fighting","fire","flying","ghost",
        "grass","ground","ice","normal","poison","psychic","rock","steel","water"
    ]
    for t in TYPES:
        if re.search(rf"\b{t}\b", h):
            return t
    return ""

# ------------------------
# Normalized record
# ------------------------

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
            self.name.strip().lower(),
            self.fast_move.strip().lower(),
            self.charge_move.strip().lower(),
            self.type_bucket.strip().lower(),
        )

def dedupe_best(rows: List[AttackerRow]) -> List[AttackerRow]:
    """
    Keep the 'best' (lowest rank, then highest score) per (name, fast, charge, type_bucket).
    """
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
            # tie-break on score
            prev_score = prev.score if prev.score is not None else -1e9
            this_score = r.score if r.score is not None else -1e9
            if this_score > prev_score:
                best[k] = r
    return list(best.values())

def as_dict(r: AttackerRow) -> Dict[str, Any]:
    return dataclasses.asdict(r)

# ------------------------
# Pokebattler
# ------------------------

def scrape_pokebattler(types: Iterable[str]) -> List[AttackerRow]:
    """
    Scrape best attackers by type from Pokebattler raid pages.
    This uses a heuristic based on visible lists/cards commonly found on those pages.
    """
    base = "https://www.pokebattler.com/raids"
    out: List[AttackerRow] = []
    ts = now_iso()

    for t in types:
        url = base  # landing aggregates multiple types; we’ll still capture per-type hints
        html = http_get(url)
        time.sleep(SLEEP_BETWEEN_REQUESTS)
        s = soupify(html)
        if not s:
            continue

        # Strategy:
        # - Find sections/cards with headings like "Best Fire Type Attackers"
        # - Within, capture rows/items with (name, moves, maybe rank/score)
        sections = []
        for h in s.select("h1, h2, h3, h4"):
            title = text(h)
            if re.search(rf"\b{t}\b", title.lower()) and re.search(r"best|attackers|counters", title.lower()):
                # capture a nearby container
                container = h.find_parent() or h.parent
                sections.append((title, container))

        for title, container in sections:
            type_bucket = to_type_bucket("", hint=title)
            # item candidates
            items = container.select("li, .card, .list-item, .counter, tr")
            rank_ctr = 0
            for it in items:
                txt = text(it)
                # look for typical patterns "1. Reshiram — Fire Fang + Fusion Flare (Score: 95)"
                m_rank = re.match(r"^\s*(\d+)[\.\)]\s+(.*)$", txt)
                rank = None
                body = txt
                if m_rank:
                    rank = int(m_rank.group(1))
                    body = m_rank.group(2)

                # extract possible name and moves
                # naive split on "—" or "-" or "–"
                parts = re.split(r"\s+[-—–]\s+", body, maxsplit=1)
                name = parts[0].strip()
                moves = parts[1].strip() if len(parts) > 1 else ""
                fast, charge = extract_movestring(moves)

                if not name or len(name) < 3:
                    continue
                # filter out obvious headers/noise
                if re.search(r"(best|attackers|counters|type|guide|ranking)", name.lower()):
                    continue

                # detect score
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
                        url=url,
                        ts=ts,
                    )
                )

    return out

# ------------------------
# GamePress
# ------------------------

def scrape_gamepress(types: Iterable[str]) -> List[AttackerRow]:
    """
    Scrape GamePress for 'Best X Attackers' style lists (type pages / meta).
    """
    base = "https://gamepress.gg/pokemongo"
    ts = now_iso()
    out: List[AttackerRow] = []

    html = http_get(base)
    time.sleep(SLEEP_BETWEEN_REQUESTS)
    s = soupify(html)
    if not s:
        return out

    # Find links to type guides from the homepage tiles or nav
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

    # De-dup, keep per-type that matches the requested set
    seen = set()
    wanted = {t.lower() for t in types}
    filtered = []
    for label, href in links:
        # guess type from label
        tb = to_type_bucket("", hint=label.lower())
        if tb and tb in wanted and href not in seen:
            seen.add(href)
            filtered.append((tb, label, href))

    for tb, label, href in filtered:
        page = http_get(href)
        time.sleep(SLEEP_BETWEEN_REQUESTS)
        sp = soupify(page)
        if not sp:
            continue

        # Try to extract list rows from tables or ranked lists
        candidates = sp.select("table tr, ol li, ul li, .view-content .node, .ranking-list .row, .card")
        rank = 0
        for c in candidates:
            # name + moves heuristic
            t = text(c)
            if not t or len(t) < 5:
                continue
            # Skip if the row is clearly a header
            if re.search(r"(tier|overview|guide|meta|info|introduction)", t.lower()):
                continue

            # Name: try bold/link
            name = ""
            a = c.find("a")
            if a and text(a):
                name = text(a)
            # fallback
            if not name:
                m = re.match(r"^\s*(\d+[\.\)])?\s*([A-Za-z0-9' \-\.\:]+)", t)
                if m:
                    name = norm_space(m.group(2))

            # Moves
            fast, charge = extract_movestring(t)

            if not name or len(name) < 3:
                continue

            rank += 1

            # optional score extraction
            score = None
            score_kind = ""
            m_score = re.search(r"(DPS|Score|Rating|TTW)\s*[:=]\s*([0-9]+(\.[0-9]+)?)", t, re.IGNORECASE)
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
# GO Hub
# ------------------------

def scrape_gohub(types: Iterable[str]) -> List[AttackerRow]:
    """
    Scrape GO Hub guides that list best attackers per type.
    We'll find likely 'Best X attackers' pages from the guides index.
    """
    base = "https://pokemongohub.net/category/guides/"
    ts = now_iso()
    out: List[AttackerRow] = []

    html = http_get(base)
    time.sleep(SLEEP_BETWEEN_REQUESTS)
    s = soupify(html)
    if not s:
        return out

    # Find articles that look like "Best Fire-type Attackers in Pokémon GO"
    candidates = []
    for a in s.select("a"):
        label = text(a)
        if not label:
            continue
        if re.search(r"best .*attacker", label.lower()) or re.search(r"best .*type", label.lower()):
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

    # Visit and parse lists
    seen = set()
    for tb, label, href in filtered:
        if href in seen:
            continue
        seen.add(href)
        page = http_get(href)
        time.sleep(SLEEP_BETWEEN_REQUESTS)
        sp = soupify(page)
        if not sp:
            continue

        items = sp.select("ol li, ul li, table tr, .elementor-widget-container li, .entry-content li")
        rank = 0
        for it in items:
            t = text(it)
            if not t or len(t) < 5:
                continue
            if re.search(r"(overview|intro|guide|about|sources|disclaimer|update)", t.lower()):
                continue

            # Name first, moves later
            name = ""
            a = it.find("a")
            if a and text(a):
                name = text(a)
            if not name:
                # Try bold/strong
                b = it.find("strong")
                if b and text(b):
                    name = text(b)
            if not name:
                # fallback to first phrase-ish
                m = re.match(r"^([A-Za-z0-9' \-\.\:]+)", t)
                if m:
                    name = norm_space(m.group(1))

            fast, charge = extract_movestring(t)
            if not name or len(name) < 3:
                continue

            rank += 1

            score = None
            score_kind = ""
            m_score = re.search(r"(DPS|Score|Rating|TTW)\s*[:=]\s*([0-9]+(\.[0-9]+)?)", t, re.IGNORECASE)
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
# Orchestration
# ------------------------

def normalize_types_arg(raw: Optional[str]) -> List[str]:
    if not raw:
        # sensible default: cover common query types up front
        return ["fire", "water", "grass", "electric", "rock", "ice", "dragon", "ghost", "fighting", "fairy"]
    out = []
    for part in raw.split(","):
        p = part.strip().lower()
        if p:
            out.append(p)
    return out

def main():
    ap = argparse.ArgumentParser(description="Scrape PvE attacker rankings (Pokebattler, GamePress, GO Hub)")
    ap.add_argument("-o", "--out", default="outputs/attackers.json", help="Output JSON path")
    ap.add_argument("--types", default="", help="Comma-separated type buckets to target (e.g., fire,water,rock)")
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

    if not rows:
        print("[warn] No attacker rows extracted from any source.", file=sys.stderr)

    # Deduplicate across sources; keep best rank/score per (name, fast, charge, type_bucket)
    unique_rows = dedupe_best(rows)

    # Sort by type_bucket then rank
    unique_rows.sort(key=lambda r: (r.type_bucket or "~", r.rank if r.rank is not None else 999999, r.name.lower()))

    # Serialize
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    payload = {
        "_meta": {
            "generated_at": now_iso(),
            "sources": ["pokebattler", "gamepress", "gohub"],
            "types_requested": types,
            "rows_total": len(rows),
            "rows_unique": len(unique_rows),
        },
        "attackers": [as_dict(r) for r in unique_rows],
    }
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f"[ok] Wrote {args.out} with {len(unique_rows)} unique rows")

if __name__ == "__main__":
    main()