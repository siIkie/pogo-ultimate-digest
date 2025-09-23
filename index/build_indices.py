#!/usr/bin/env python3
"""
Build lightweight NDJSON indices for RAG from the normalized library.

Robustness goals:
- Never crash when a domain has zero usable texts (empty or only stop-words).
- Still emit an NDJSON file (possibly empty) and a small sidecar stats JSON.
- Keep dependencies minimal (uses scikit-learn only for TF-IDF when data exists).

Outputs (created/overwritten every run):
  index/events.ndjson
  index/features.ndjson
  index/balance.ndjson
  index/wiki.ndjson
  index/_stats.json            # summary of counts per domain

Input sources (created by earlier build steps):
  pogo_library/events/index.json
  pogo_library/features/index.json
  pogo_library/balance/index.json
  pogo_library/wiki/index.json
"""

import os
import json
import pathlib
from typing import List, Dict, Any

import numpy as np  # used only to check matrix shape safely

# scikit-learn imports (installed via requirements.txt)
from sklearn.feature_extraction.text import TfidfVectorizer


ROOT = pathlib.Path(".").resolve()
LIB_DIR = ROOT / "pogo_library"
OUT_DIR = ROOT / "index"
OUT_DIR.mkdir(parents=True, exist_ok=True)


# --------------------------
# Helpers
# --------------------------

def load_json_list(path: pathlib.Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8") as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError:
            # If upstream emitted an empty file for some reason, treat as empty list.
            return []
    if isinstance(data, list):
        return data
    # Some producers may wrap in {"items":[...]} — be forgiving.
    if isinstance(data, dict) and "items" in data and isinstance(data["items"], list):
        return data["items"]
    return []


def ensure_str(x: Any) -> str:
    if x is None:
        return ""
    if isinstance(x, (list, tuple, set)):
        return " ".join(ensure_str(v) for v in x)
    return str(x)


def pick(d: Dict[str, Any], *keys: str) -> Dict[str, Any]:
    return {k: d.get(k) for k in keys if k in d}


def to_text_joined(row: Dict[str, Any], fields: List[str]) -> str:
    # Join multiple fields to form the search text.
    parts = []
    for f in fields:
        v = row.get(f)
        if v is None:
            continue
        s = ensure_str(v).strip()
        if s:
            parts.append(s)
    return " | ".join(parts)


def write_ndjson(path: pathlib.Path, rows: List[Dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


# --------------------------
# Domain-specific loading
# --------------------------

def load_domain(domain: str) -> List[Dict[str, Any]]:
    """
    Load normalized rows for a given domain.
    Domain ∈ {'events','features','balance','wiki'}
    """
    path = LIB_DIR / domain / "index.json"
    return load_json_list(path)


def make_doc_rows(domain: str, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Convert normalized rows to NDJSON 'doc' rows suitable for simple RAG.
    We keep the full original row as 'meta' for maximum downstream usefulness.
    """
    out = []
    if domain == "events":
        text_fields = [
            "Event Name",
            "Category",
            "Category (CD / CD Classic / Raid / Mega / Shadow Raid / Spotlight / Research / Other)",
            "Featured", "Bonuses", "Region",
            "Source", "Source URL",
            "Start Date", "End Date",
            "Date Parse Status", "Category Normalized"
        ]
        id_field = "Event Name"
    elif domain == "features":
        text_fields = [
            "Title", "Summary", "Categories",
            "Source", "Source URL", "Published"
        ]
        id_field = "Title"
    elif domain == "balance":
        text_fields = [
            "Title", "Change Type", "Targets", "Summary",
            "Source", "Source URL", "Published"
        ]
        id_field = "Title"
    else:  # wiki
        text_fields = ["Title", "Summary", "Tags", "Source", "Source URL"]
        id_field = "Title"

    for i, r in enumerate(rows):
        text = to_text_joined(r, text_fields).strip()
        if not text:
            # Keep doc but mark empty text – it won't participate in vectorization.
            doc_id = ensure_str(r.get(id_field)) or f"{domain}-{i}"
            out.append({
                "id": doc_id,
                "domain": domain,
                "text": "",
                "meta": r
            })
            continue

        doc_id = ensure_str(r.get(id_field)) or f"{domain}-{i}"
        out.append({
            "id": doc_id,
            "domain": domain,
            "text": text,
            "meta": r
        })
    return out


def fit_tfidf_safe(texts: List[str]) -> Dict[str, Any]:
    """
    Fit a TF-IDF vectorizer when possible.
    Returns a dict with optional keys: vocab, df, n_docs, n_terms.
    Never raises 'empty vocabulary' – we guard and return {} in that case.
    """
    # Filter out truly empty strings first.
    docs = [t for t in (ensure_str(t).strip() for t in texts) if t]
    if not docs:
        return {}

    # Use no built-in stopwords to avoid dropping everything by accident.
    vec = TfidfVectorizer(stop_words=None, lowercase=True, min_df=1, token_pattern=r"(?u)\b\w+\b")

    try:
        X = vec.fit_transform(docs)
    except ValueError as e:
        # Typical case: "empty vocabulary; perhaps the documents only contain stop words"
        return {}

    # Double-check matrix shape.
    if not hasattr(X, "shape") or X.shape[1] == 0:
        return {}

    vocab = sorted(vec.vocabulary_.keys())
    return {
        "n_docs": int(X.shape[0]),
        "n_terms": int(X.shape[1]),
        "vocab_sample": vocab[:1000],  # keep artifact small; not used for ranking directly
    }


def build_domain_index(domain: str, out_path: pathlib.Path) -> Dict[str, Any]:
    rows = load_domain(domain)
    docs = make_doc_rows(domain, rows)
    write_ndjson(out_path, docs)

    # Build TF-IDF summary only for non-empty texts (no persistence of matrix needed here).
    texts = [d["text"] for d in docs if d["text"]]
    tfidf_info = fit_tfidf_safe(texts)

    return {
        "domain": domain,
        "docs": len(docs),
        "docs_with_text": len(texts),
        **tfidf_info
    }


def main():
    stats = {}
    stats["events"] = build_domain_index("events", OUT_DIR / "events.ndjson")
    stats["features"] = build_domain_index("features", OUT_DIR / "features.ndjson")
    stats["balance"] = build_domain_index("balance", OUT_DIR / "balance.ndjson")
    stats["wiki"] = build_domain_index("wiki", OUT_DIR / "wiki.ndjson")

    with (OUT_DIR / "_stats.json").open("w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)

    # Console hint for CI logs
    print("[index] Built NDJSON indices. Stats:")
    print(json.dumps(stats, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()