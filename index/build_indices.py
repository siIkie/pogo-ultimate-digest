# index/build_indices.py
# Robust index builder for POGO Ultimate Digest
# - Creates TF-IDF and BM25 indices per domain
# - Skips empty/stopword-only docs gracefully
# - Never fails the whole run due to one sparse domain

import os, json, re, glob
from typing import List, Dict, Any, Tuple

# deps: scikit-learn, joblib, rank-bm25, numpy
import numpy as np
from joblib import dump
from sklearn.feature_extraction.text import TfidfVectorizer
from rank_bm25 import BM25Okapi

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

OUT_DIR = os.path.join(ROOT, "index_artifacts")
os.makedirs(OUT_DIR, exist_ok=True)

# Where to read the normalized data your pipeline writes
API_DIR = os.path.join(ROOT, "api")
LIB_DIR = os.path.join(ROOT, "pogo_library")

# Utility ----------------------------------------------------------------------

_whitespace_re = re.compile(r"\s+")

def norm_text(s: Any) -> str:
    if s is None:
        return ""
    s = str(s)
    s = s.replace("\u00A0", " ")  # nbsp
    s = _whitespace_re.sub(" ", s)
    return s.strip()

def join_parts(*parts: str) -> str:
    return " | ".join([p for p in parts if p])

def read_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def try_read(path: str) -> Any:
    try:
        return read_json(path)
    except Exception:
        return None

def ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)

def tokenize_for_bm25(text: str) -> List[str]:
    # super simple tokenization: lowercase words with letters/numbers
    return re.findall(r"[a-z0-9]+", text.lower())

# Domain loaders ---------------------------------------------------------------

def load_events() -> List[Dict[str, Any]]:
    path = os.path.join(API_DIR, "events.json")
    data = try_read(path) or []
    rows = []
    for r in data:
        title = norm_text(r.get("Event Name"))
        cat = norm_text(r.get("Category"))
        src = norm_text(r.get("Source"))
        desc = join_parts(title, cat, src)
        rows.append({
            "id": f"event:{title}",
            "title": title,
            "text": desc
        })
    return rows

def load_features() -> List[Dict[str, Any]]:
    path = os.path.join(API_DIR, "features.json")
    data = try_read(path) or []
    rows = []
    for r in data:
        title = norm_text(r.get("Title") or r.get("Feature") or r.get("Event Name"))
        summary = norm_text(r.get("Summary") or r.get("Body") or "")
        src = norm_text(r.get("Source"))
        rows.append({
            "id": f"feature:{title}",
            "title": title,
            "text": join_parts(title, summary, src)
        })
    return rows

def load_balance() -> List[Dict[str, Any]]:
    path = os.path.join(API_DIR, "balance.json")
    data = try_read(path) or []
    rows = []
    for r in data:
        what = norm_text(r.get("What") or r.get("Move") or r.get("Pokemon") or r.get("Change"))
        detail = norm_text(r.get("Detail") or r.get("Notes") or "")
        src = norm_text(r.get("Source"))
        rows.append({
            "id": f"balance:{what}",
            "title": what,
            "text": join_parts(what, detail, src)
        })
    return rows

def load_wiki() -> List[Dict[str, Any]]:
    path = os.path.join(API_DIR, "wiki.json")
    data = try_read(path) or []
    rows = []
    for r in data:
        title = norm_text(r.get("Title"))
        body = norm_text(r.get("Text") or r.get("Body") or "")
        src = norm_text(r.get("Source"))
        rows.append({
            "id": f"wiki:{title}",
            "title": title,
            "text": join_parts(title, body, src)
        })
    return rows

def load_attackers() -> List[Dict[str, Any]]:
    # support either a top-level attackers.json or a nested file in library
    candidates = [
        os.path.join(API_DIR, "attackers.json"),
        os.path.join(LIB_DIR, "attackers", "index.json"),
    ]
    data = None
    for c in candidates:
        data = try_read(c)
        if data:
            break
    data = data or []
    rows = []
    for r in data:
        name = norm_text(r.get("name") or r.get("pokemon") or "")
        atype = norm_text(r.get("type") or r.get("typing") or "")
        dps = norm_text(r.get("dps") or r.get("DPS") or "")
        moves = norm_text(", ".join([m for m in r.get("moves", [])]))
        rows.append({
            "id": f"attacker:{name}",
            "title": name,
            "text": join_parts(name, atype, moves, f"DPS {dps}")
        })
    return rows

def load_generic_from_api(filename: str, id_prefix: str, title_fields: List[str], body_fields: List[str]) -> List[Dict[str, Any]]:
    path = os.path.join(API_DIR, filename)
    data = try_read(path) or []
    rows = []
    for r in data:
        title = ""
        for f in title_fields:
            if r.get(f):
                title = norm_text(r[f]); break
        body_parts = []
        for f in body_fields:
            v = r.get(f)
            if v is None:
                continue
            if isinstance(v, (list, tuple)):
                body_parts.append(", ".join(map(norm_text, v)))
            else:
                body_parts.append(norm_text(v))
        rows.append({
            "id": f"{id_prefix}:{title or 'row'}",
            "title": title or id_prefix,
            "text": join_parts(title, *body_parts)
        })
    return rows

def load_items() -> List[Dict[str, Any]]:
    return load_generic_from_api("items.json", "item", ["Name", "Item", "Title"], ["Description", "Effect", "Notes"])

def load_eggs() -> List[Dict[str, Any]]:
    return load_generic_from_api("eggs.json", "egg", ["Mon", "Pokemon", "Title"], ["Tier", "Pool", "Notes"])

def load_pvp() -> List[Dict[str, Any]]:
    return load_generic_from_api("pvp.json", "pvp", ["League", "Cup", "Title"], ["Rules", "Bans", "Dates", "Notes"])

def load_research() -> List[Dict[str, Any]]:
    return load_generic_from_api("research.json", "research", ["Task", "Title"], ["Reward", "Notes"])

def load_shinies() -> List[Dict[str, Any]]:
    return load_generic_from_api("shinies.json", "shiny", ["Pokemon", "Title", "Name"], ["Available From", "Notes"])

# Index building ---------------------------------------------------------------

def filter_docs(rows: List[Dict[str, Any]], min_tokens: int = 3) -> Tuple[List[Dict[str, Any]], List[List[str]]]:
    """Return (kept_rows, tokenized_for_bm25) after dropping texts with < min_tokens."""
    kept = []
    tokenized = []
    for r in rows:
        text = norm_text(r.get("text", ""))
        toks = tokenize_for_bm25(text)
        if len(toks) >= min_tokens:
            kept.append(r)
            tokenized.append(toks)
    return kept, tokenized

def build_tfidf(texts: List[str]) -> Tuple[TfidfVectorizer, Any]:
    vectorizer = TfidfVectorizer(
        stop_words="english",
        lowercase=True,
        max_df=0.9,
        min_df=1,
        ngram_range=(1, 2),
        token_pattern=r"(?u)\b[a-zA-Z0-9]{2,}\b",
        dtype=np.float32
    )
    X = vectorizer.fit_transform(texts)
    return vectorizer, X

def build_bm25(tokenized_docs: List[List[str]]) -> BM25Okapi:
    return BM25Okapi(tokenized_docs)

def write_meta(domain: str, rows: List[Dict[str, Any]], skipped: int) -> None:
    meta = {
        "domain": domain,
        "num_rows": len(rows),
        "skipped_rows": skipped
    }
    with open(os.path.join(OUT_DIR, f"{domain}_meta.json"), "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

def save_index(domain: str, rows: List[Dict[str, Any]], tokenized: List[List[str]]) -> None:
    ensure_dir(OUT_DIR)
    texts = [r["text"] for r in rows]

    # TF-IDF (skip if no data)
    if len(texts) > 0:
        try:
            vec, X = build_tfidf(texts)
            dump({"vectorizer": vec, "matrix": X, "ids": [r["id"] for r in rows], "titles": [r["title"] for r in rows]}, 
                 os.path.join(OUT_DIR, f"{domain}_tfidf.joblib"))
        except Exception as e:
            # Don't fail the pipeline – just record a meta file
            with open(os.path.join(OUT_DIR, f"{domain}_tfidf.error.txt"), "w", encoding="utf-8") as f:
                f.write(str(e))

    # BM25 (skip if no data)
    if len(tokenized) > 0:
        try:
            bm25 = build_bm25(tokenized)
            dump({"bm25": bm25, "ids": [r["id"] for r in rows], "titles": [r["title"] for r in rows], "tokens": tokenized}, 
                 os.path.join(OUT_DIR, f"{domain}_bm25.joblib"))
        except Exception as e:
            with open(os.path.join(OUT_DIR, f"{domain}_bm25.error.txt"), "w", encoding="utf-8") as f:
                f.write(str(e))

def build_domain_index(domain: str, loader_fn):
    rows = loader_fn() or []
    kept, tokenized = filter_docs(rows, min_tokens=3)
    skipped = len(rows) - len(kept)
    write_meta(domain, kept, skipped)
    save_index(domain, kept, tokenized)

def main():
    domains = [
        ("events",    load_events),
        ("features",  load_features),
        ("balance",   load_balance),
        ("wiki",      load_wiki),
        ("attackers", load_attackers),
        ("items",     load_items),
        ("eggs",      load_eggs),
        ("pvp",       load_pvp),
        ("research",  load_research),
        ("shinies",   load_shinies),
    ]
    for name, fn in domains:
        try:
            build_domain_index(name, fn)
        except Exception as e:
            # Defensive: never fail the entire run for one domain
            errp = os.path.join(OUT_DIR, f"{name}_fatal.error.txt")
            with open(errp, "w", encoding="utf-8") as f:
                f.write(str(e))

if __name__ == "__main__":
    main()