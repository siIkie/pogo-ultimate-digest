#!/usr/bin/env python3
"""
Normalize & validate events for the POGO Ultimate pipeline.

Key points:
  - Accept ANY source category label in 'Category' (schema relaxed).
  - Derive a stable 'Category Normalized' for downstream queries/reports.
  - Preserve the original verbose category in 'Category (raw)' when present.
  - Ensure 'Sources' array, boolean 'Has Valid Dates', valid 'Source URL', and clean dates.
  - Validate against schemas/events.schema.json.
  - Write back to pogo_library/events/index.json (+ index.normalized.json).
"""

import os
import json
import re
import sys
from typing import List, Dict, Any

import pandas as pd

# ------- Paths -------
LIB_EVENTS_JSON = os.path.join("pogo_library", "events", "index.json")
LIB_EVENTS_JSON_NORM = os.path.join("pogo_library", "events", "index.normalized.json")
DIGEST_CSV = "POGO_Digest.csv"
DIGEST_XLSX = "POGO_Digest.xlsx"
SCHEMA_PATH = os.path.join("schemas", "events.schema.json")

# Upstream verbose header sometimes present
VERBOSE_CATEGORY_KEY = "Category (CD / CD Classic / Raid / Mega / Shadow Raid / Spotlight / Research / Other)"
SHORT_CATEGORY_KEY = "Category"

# Normalized buckets we publish (optional field, not required by schema)
NORMALIZED_ENUM = [
    "CD",
    "CD Classic",
    "Raid",
    "Mega",
    "Shadow Raid",
    "Spotlight",
    "Research",
    "Other",
    "Event/News"
]

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
DATE_STATUS_ALLOWED = {"parsed", "missing", "inferred", "invalid", ""}


def load_events() -> pd.DataFrame:
    """Load events from JSON/CSV/XLSX (first found)."""
    if os.path.exists(LIB_EVENTS_JSON):
        try:
            with open(LIB_EVENTS_JSON, "r", encoding="utf-8") as f:
                data = json.load(f)
            df = pd.DataFrame(data)
            if not df.empty:
                return df
        except Exception as e:
            print(f"[warn] Failed reading {LIB_EVENTS_JSON}: {e}", file=sys.stderr)

    if os.path.exists(DIGEST_CSV):
        try:
            df = pd.read_csv(DIGEST_CSV)
            if not df.empty:
                return df
        except Exception as e:
            print(f"[warn] Failed reading {DIGEST_CSV}: {e}", file=sys.stderr)

    if os.path.exists(DIGEST_XLSX):
        try:
            xls = pd.ExcelFile(DIGEST_XLSX)
            sheet = "Events" if "Events" in xls.sheet_names else xls.sheet_names[0]
            df = pd.read_excel(DIGEST_XLSX, sheet_name=sheet)
            if not df.empty:
                return df
        except Exception as e:
            print(f"[warn] Failed reading {DIGEST_XLSX}: {e}", file=sys.stderr)

    return pd.DataFrame(columns=[
        "Start Date", "End Date", "Event Name",
        VERBOSE_CATEGORY_KEY, SHORT_CATEGORY_KEY,
        "Source", "Source URL", "Sources",
        "Has Valid Dates", "Date Parse Status"
    ])


def _as_str(x) -> str:
    if x is None:
        return ""
    try:
        if pd.isna(x):
            return ""
    except Exception:
        pass
    return str(x)


def _valid_date(s: str) -> bool:
    return bool(DATE_RE.match(s or ""))


def _normalize_category_label(raw: str) -> str:
    """
    Map arbitrary source labels to stable buckets (for 'Category Normalized').
    This does NOT affect the free-form 'Category' saved from the source.
    """
    s = _as_str(raw).strip().lower()

    # direct hits
    if s in {"cd", "community day"}:
        return "CD"
    if s in {"cd classic", "community day classic"}:
        return "CD Classic"
    if "shadow raid" in s:
        return "Shadow Raid"
    if "spotlight" in s:
        return "Spotlight"
    if "research" in s:
        return "Research"
    if "mega" in s:
        return "Mega"
    if "raid" in s:
        return "Raid"
    if "event" in s or "news" in s:
        return "Event/News"
    # default
    return "Other"


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # --- Category keys ---
    if VERBOSE_CATEGORY_KEY in df.columns:
        df["Category (raw)"] = df[VERBOSE_CATEGORY_KEY]
        if SHORT_CATEGORY_KEY not in df.columns:
            df[SHORT_CATEGORY_KEY] = df[VERBOSE_CATEGORY_KEY]
        else:
            empty_short = df[SHORT_CATEGORY_KEY].isna() | (df[SHORT_CATEGORY_KEY].astype(str).str.strip() == "")
            df.loc[empty_short, SHORT_CATEGORY_KEY] = df.loc[empty_short, VERBOSE_CATEGORY_KEY]
    if SHORT_CATEGORY_KEY not in df.columns:
        df[SHORT_CATEGORY_KEY] = ""

    # --- Ensure expected columns exist ---
    needed = [
        "Start Date", "End Date", "Event Name",
        SHORT_CATEGORY_KEY, "Source", "Source URL",
        "Has Valid Dates", "Date Parse Status"
    ]
    for c in needed:
        if c not in df.columns:
            df[c] = ""

    # --- Coerce text fields ---
    for c in ["Event Name", SHORT_CATEGORY_KEY, "Source", "Source URL", "Date Parse Status"]:
        df[c] = df[c].map(_as_str)

    # --- Dates: keep only YYYY-MM-DD or blank ---
    for c in ["Start Date", "End Date"]:
        df[c] = df[c].map(_as_str)
        df.loc[~df[c].map(_valid_date) & (df[c] != ""), c] = ""

    # --- Has Valid Dates: strict boolean (default False) ---
    def _to_bool(v):
        if isinstance(v, bool):
            return v
        s = _as_str(v).strip().lower()
        if s in {"true", "1", "yes", "y"}:
            return True
        if s in {"false", "0", "no", "n"}:
            return False
        return False
    df["Has Valid Dates"] = df["Has Valid Dates"].map(_to_bool)
    df.loc[df["Start Date"] == "", "Has Valid Dates"] = False

    # --- Sources array from Source if missing ---
    if "Sources" not in df.columns:
        df["Sources"] = None

    def _make_sources(row):
        cur = row.get("Sources", None)
        if isinstance(cur, list) and all(isinstance(x, str) for x in cur):
            return cur
        src = _as_str(row.get("Source", "")).strip()
        return [src] if src else []
    df["Sources"] = df.apply(_make_sources, axis=1)

    # --- Normalize derived bucket (optional column) ---
    df["Category Normalized"] = df[SHORT_CATEGORY_KEY].map(_normalize_category_label)

    # --- Date Parse Status to allowed set ---
    def _norm_status(v: str) -> str:
        s = _as_str(v).strip().lower()
        if s in DATE_STATUS_ALLOWED:
            return s
        if s in {"ok", "single", "end_only"}:
            return "parsed"
        if s in {"none", "unknown", "n/a"}:
            return ""
        return "invalid"
    df["Date Parse Status"] = df["Date Parse Status"].map(_norm_status)

    # --- Source URL fallback to valid URI ---
    df["Source URL"] = df["Source URL"].apply(lambda s: _as_str(s).strip() or "about:blank")

    # Column order
    preferred = [
        "Start Date", "End Date", "Event Name",
        SHORT_CATEGORY_KEY, "Category Normalized", "Category (raw)",
        "Source", "Source URL", "Sources",
        "Has Valid Dates", "Date Parse Status"
    ]
    cols = preferred + [c for c in df.columns if c not in preferred]
    df = df[cols]

    return df


def validate_against_schema(rows: List[Dict[str, Any]], schema_path: str) -> None:
    try:
        import jsonschema  # type: ignore
    except Exception:
        print("[warn] jsonschema not installed; skipping validation.", file=sys.stderr)
        return

    with open(schema_path, "r", encoding="utf-8") as f:
        schema = json.load(f)

    import jsonschema
    try:
        jsonschema.validate(instance=rows, schema={"type": "array", "items": schema})
    except Exception as e:
        # Print more detail to logs to speed up debugging
        print("Error:  Schema validation failed. Exception message:", file=sys.stderr)
        print(repr(e), file=sys.stderr)
        # Try to find and print the first offending row if possible
        try:
            from jsonschema import Draft202012Validator
            v = Draft202012Validator({"type": "array", "items": schema})
            for idx, err in enumerate(v.iter_errors(rows)):
                print(f"First error at item index {err.path[0] if err.path else 'unknown'}:", file=sys.stderr)
                print(err.message, file=sys.stderr)
                break
        except Exception:
            pass
        raise


def save_events(rows: List[Dict[str, Any]]) -> None:
    os.makedirs(os.path.dirname(LIB_EVENTS_JSON), exist_ok=True)
    with open(LIB_EVENTS_JSON, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)
    with open(LIB_EVENTS_JSON_NORM, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)


def main():
    df = load_events()
    before_cols = list(df.columns)
    df = normalize_columns(df)
    after_cols = list(df.columns)

    rows: List[Dict[str, Any]] = df.to_dict(orient="records")

    if os.path.exists(SCHEMA_PATH):
        validate_against_schema(rows, SCHEMA_PATH)

    save_events(rows)

    print(f"[done] Normalized events written to:\n"
          f"  - {LIB_EVENTS_JSON}\n"
          f"  - {LIB_EVENTS_JSON_NORM}")
    print(f"[info] Columns before: {before_cols}")
    print(f"[info] Columns after : {after_cols}")


if __name__ == "__main__":
    main()