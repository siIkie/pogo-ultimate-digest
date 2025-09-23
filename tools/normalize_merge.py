#!/usr/bin/env python3
"""
Normalize & validate events for the POGO Ultimate pipeline.

This script:
  - Loads events (prefers pogo_library/events/index.json; falls back to CSV/XLSX).
  - Renames the verbose category column
        "Category (CD / CD Classic / Raid / Mega / Shadow Raid / Spotlight / Research / Other)"
    to the canonical short key "Category" (preserves original as "Category (raw)").
  - Ensures standard columns exist and are coerced to safe types.
  - Ensures "Sources" (array) exists; if only "Source" exists, creates Sources = [Source].
  - Validates against schemas/events.schema.json (if jsonschema is installed).
  - Writes back the normalized rows to pogo_library/events/index.json and
    a safety copy pogo_library/events/index.normalized.json.
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

# The verbose header we sometimes see
VERBOSE_CATEGORY_KEY = "Category (CD / CD Classic / Raid / Mega / Shadow Raid / Spotlight / Research / Other)"
SHORT_CATEGORY_KEY = "Category"


# ------- Utilities -------
def load_events() -> pd.DataFrame:
    """Load events from JSON/CSV/XLSX (whichever exists first)."""
    # 1) Preferred library JSON
    if os.path.exists(LIB_EVENTS_JSON):
        try:
            with open(LIB_EVENTS_JSON, "r", encoding="utf-8") as f:
                data = json.load(f)
            df = pd.DataFrame(data)
            if not df.empty:
                return df
        except Exception as e:
            print(f"[warn] Failed reading {LIB_EVENTS_JSON}: {e}", file=sys.stderr)

    # 2) CSV (fallback)
    if os.path.exists(DIGEST_CSV):
        try:
            df = pd.read_csv(DIGEST_CSV)
            if not df.empty:
                return df
        except Exception as e:
            print(f"[warn] Failed reading {DIGEST_CSV}: {e}", file=sys.stderr)

    # 3) Excel (fallback)
    if os.path.exists(DIGEST_XLSX):
        try:
            xls = pd.ExcelFile(DIGEST_XLSX)
            sheet = "Events" if "Events" in xls.sheet_names else xls.sheet_names[0]
            df = pd.read_excel(DIGEST_XLSX, sheet_name=sheet)
            if not df.empty:
                return df
        except Exception as e:
            print(f"[warn] Failed reading {DIGEST_XLSX}: {e}", file=sys.stderr)

    # Empty frame with expected structure
    return pd.DataFrame(columns=[
        "Start Date", "End Date", "Event Name",
        VERBOSE_CATEGORY_KEY, SHORT_CATEGORY_KEY,
        "Source", "Source URL", "Sources",
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
    """Standardize keys and ensure canonical 'Category' and 'Sources' exist."""
    df = df.copy()

    # Map verbose category -> short key; preserve original
    if VERBOSE_CATEGORY_KEY in df.columns:
        df["Category (raw)"] = df[VERBOSE_CATEGORY_KEY]
        if SHORT_CATEGORY_KEY not in df.columns:
            df[SHORT_CATEGORY_KEY] = df[VERBOSE_CATEGORY_KEY]
        else:
            short_is_empty = df[SHORT_CATEGORY_KEY].isna() | (df[SHORT_CATEGORY_KEY].astype(str).str.strip() == "")
            df.loc[short_is_empty, SHORT_CATEGORY_KEY] = df.loc[short_is_empty, VERBOSE_CATEGORY_KEY]

    if SHORT_CATEGORY_KEY not in df.columns:
        df[SHORT_CATEGORY_KEY] = ""

    # Ensure minimal set of columns exists
    need = [
        "Start Date", "End Date", "Event Name",
        SHORT_CATEGORY_KEY, "Source", "Source URL",
        "Has Valid Dates", "Date Parse Status"
    ]
    for c in need:
        if c not in df.columns:
            df[c] = ""

    # String-coerce text fields
    for c in ["Event Name", SHORT_CATEGORY_KEY, "Source", "Source URL", "Date Parse Status"]:
        df[c] = df[c].map(_as_str)

    # Dates remain strings (YYYY-MM-DD) or empty
    for c in ["Start Date", "End Date"]:
        df[c] = df[c].map(_as_str)

    # Normalize Has Valid Dates to bool-ish (or empty)
    if "Has Valid Dates" in df.columns:
        def _to_boolish(v):
            if isinstance(v, bool):
                return v
            s = _as_str(v).strip().lower()
            if s in ("true", "1", "yes", "y"):
                return True
            if s in ("false", "0", "no", "n"):
                return False
            return ""  # keep blank if unknown
        df["Has Valid Dates"] = df["Has Valid Dates"].map(_to_boolish)

    # Ensure "Sources" is an ARRAY; if missing, synthesize from "Source"
    if "Sources" not in df.columns:
        df["Sources"] = None

    # Build Sources from Source where needed
    def _make_sources(row):
        current = row.get("Sources", None)
        if isinstance(current, list) and all(isinstance(x, str) for x in current):
            return current
        src = _as_str(row.get("Source", "")).strip()
        return [src] if src else []

    df["Sources"] = df.apply(_make_sources, axis=1)

    # Column order – canonical first, then the rest
    preferred = [
        "Start Date", "End Date", "Event Name",
        SHORT_CATEGORY_KEY, "Category (raw)",
        "Source", "Source URL", "Sources",
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

    with open(schema_path, "r", encoding="utf-8") as f:
        schema = json.load(f)

    # Validate array of rows
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
            # Print the first offending row to help debugging
            print("Error:  Schema validation failed. First offending row:", file=sys.stderr)
            try:
                print(json.dumps(rows[0], ensure_ascii=False, indent=2), file=sys.stderr)
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