#!/usr/bin/env python3
"""
Normalize & validate events for the POGO Ultimate pipeline.

What this does:
  - Loads events (prefers pogo_library/events/index.json; falls back to CSV/XLSX).
  - Renames the verbose category column
        "Category (CD / CD Classic / Raid / Mega / Shadow Raid / Spotlight / Research / Other)"
    to the canonical short key "Category". The original value is preserved in "Category (raw)".
  - Ensures standard columns exist with safe types.
  - Validates the resulting rows against schemas/events.schema.json (jsonschema).
  - Saves the normalized result back to pogo_library/events/index.json
    and an additional copy pogo_library/events/index.normalized.json

This script is intentionally idempotent and safe to run multiple times.
"""

import os
import json
import sys
from typing import List, Dict, Any

import pandas as pd

# ------- Paths -------
LIB_EVENTS_JSON = os.path.join("pogo_library", "events", "index.json")
LIB_EVENTS_JSON_NORM = os.path.join("pogo_library", "events", "index.normalized.json")
DIGEST_CSV = "POGO_Digest.csv"
DIGEST_XLSX = "POGO_Digest.xlsx"
SCHEMA_PATH = os.path.join("schemas", "events.schema.json")

# The verbose header we see in scraped / earlier stages
VERBOSE_CATEGORY_KEY = "Category (CD / CD Classic / Raid / Mega / Shadow Raid / Spotlight / Research / Other)"
SHORT_CATEGORY_KEY = "Category"


# ------- Utils -------
def load_events() -> pd.DataFrame:
    """Load events from JSON/CSV/XLSX (first available)."""
    # 1) Preferred: library JSON
    if os.path.exists(LIB_EVENTS_JSON):
        try:
            with open(LIB_EVENTS_JSON, "r", encoding="utf-8") as f:
                data = json.load(f)
            df = pd.DataFrame(data)
            if not df.empty:
                return df
        except Exception as e:
            print(f"[warn] Failed reading {LIB_EVENTS_JSON}: {e}", file=sys.stderr)

    # 2) CSV
    if os.path.exists(DIGEST_CSV):
        try:
            df = pd.read_csv(DIGEST_CSV)
            if not df.empty:
                return df
        except Exception as e:
            print(f"[warn] Failed reading {DIGEST_CSV}: {e}", file=sys.stderr)

    # 3) XLSX
    if os.path.exists(DIGEST_XLSX):
        try:
            xls = pd.ExcelFile(DIGEST_XLSX)
            sheet = "Events" if "Events" in xls.sheet_names else xls.sheet_names[0]
            df = pd.read_excel(DIGEST_XLSX, sheet_name=sheet)
            if not df.empty:
                return df
        except Exception as e:
            print(f"[warn] Failed reading {DIGEST_XLSX}: {e}", file=sys.stderr)

    # Empty fallback with expected columns
    return pd.DataFrame(columns=[
        "Start Date", "End Date", "Event Name",
        VERBOSE_CATEGORY_KEY, SHORT_CATEGORY_KEY,
        "Source", "Source URL",
        "Has Valid Dates", "Date Parse Status"
    ])


def _as_str(x):
    if x is None:
        return ""
    try:
        if pd.isna(x):
            return ""
    except Exception:
        pass
    return str(x)


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize keys and ensure canonical 'Category' is present."""
    df = df.copy()

    # If we have the verbose category, preserve it and map to short key
    if VERBOSE_CATEGORY_KEY in df.columns:
        # Preserve original for transparency
        df["Category (raw)"] = df[VERBOSE_CATEGORY_KEY]
        # Create/overwrite the short key from the verbose one where missing/empty
        if SHORT_CATEGORY_KEY not in df.columns:
            df[SHORT_CATEGORY_KEY] = df[VERBOSE_CATEGORY_KEY]
        else:
            # Fill only where SHORT is missing but verbose has a value
            short_is_empty = df[SHORT_CATEGORY_KEY].isna() | (df[SHORT_CATEGORY_KEY].astype(str).str.strip() == "")
            df.loc[short_is_empty, SHORT_CATEGORY_KEY] = df.loc[short_is_empty, VERBOSE_CATEGORY_KEY]

    # If neither exists, create a blank category column
    if SHORT_CATEGORY_KEY not in df.columns:
        df[SHORT_CATEGORY_KEY] = ""

    # Ensure standard columns exist
    need = [
        "Start Date", "End Date", "Event Name",
        SHORT_CATEGORY_KEY, "Source", "Source URL",
        "Has Valid Dates", "Date Parse Status"
    ]
    for c in need:
        if c not in df.columns:
            df[c] = ""

    # String-coerce common text fields
    for c in ["Event Name", SHORT_CATEGORY_KEY, "Source", "Source URL", "Date Parse Status"]:
        df[c] = df[c].map(_as_str)

    # Dates stay as strings (YYYY-MM-DD) or empty
    for c in ["Start Date", "End Date"]:
        df[c] = df[c].map(_as_str)

    # Has Valid Dates to bool-ish; allow "", "true"/"false" pass-through too
    if "Has Valid Dates" in df.columns:
        def _to_boolish(v):
            if isinstance(v, bool):
                return v
            s = _as_str(v).strip().lower()
            if s in ("true", "1", "yes", "y"):
                return True
            if s in ("false", "0", "no", "n"):
                return False
            return ""  # leave blank if unknown
        df["Has Valid Dates"] = df["Has Valid Dates"].map(_to_boolish)

    # Order columns: put canonical first, then everything else
    preferred = [
        "Start Date", "End Date", "Event Name",
        SHORT_CATEGORY_KEY, "Category (raw)",
        "Source", "Source URL",
        "Has Valid Dates", "Date Parse Status"
    ]
    cols = preferred + [c for c in df.columns if c not in preferred]
    df = df[cols]

    return df


def validate_against_schema(rows: List[Dict[str, Any]], schema_path: str) -> None:
    """Validate rows with jsonschema (if available). Raises on first error."""
    try:
        import jsonschema  # type: ignore
    except Exception:
        print("[warn] jsonschema not installed; skipping validation.", file=sys.stderr)
        return
    # Load schema
    with open(schema_path, "r", encoding="utf-8") as f:
        schema = json.load(f)

    # Validate all rows; raise on first error for clear CI signal
    jsonschema.validate(instance=rows, schema={"type": "array", "items": schema})


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

    # Validate if schema exists
    if os.path.exists(SCHEMA_PATH):
        try:
            validate_against_schema(rows, SCHEMA_PATH)
            print(f"[ok] Schema validation passed for {len(rows)} rows.")
        except Exception as e:
            # Print a helpful snippet before raising
            print("[error] Schema validation failed. First offending row:")
            try:
                import itertools
                print(json.dumps(next(itertools.islice((r for r in rows), 0, 1)), ensure_ascii=False, indent=2))
            except Exception:
                pass
            raise

    save_events(rows)

    print(f"[done] Normalized events written to:\n"
          f"  - {LIB_EVENTS_JSON}\n"
          f"  - {LIB_EVENTS_JSON_NORM}")
    print(f"[info] Columns before: {before_cols}")
    print(f"[info] Columns after : {after_cols}")


if __name__ == "__main__":
    main()