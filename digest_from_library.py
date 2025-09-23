#!/usr/bin/env python3
"""
Digest builder: produce ICS calendar and make sure Excel/JSON exports exist,
while PRESERVING all rows. Only filter when generating ICS or "Events" sheet.

Outputs:
  - POGO_Events.ics                              (dated rows only)
  - POGO_Digest.xlsx with sheets:
        * All      (all rows, unfiltered)
        * Events   (dated rows only)
        * Undated  (no/invalid dates)
  - POGO_Digest.csv                              (all rows)
  - POGO_Digest.json                             (all rows)
  - POGO_Digest_undated.json                     (undated rows only)

Input (first that exists is used):
  1) pogo_library/events/index.json
  2) POGO_Digest.csv
  3) POGO_Digest.xlsx (sheet "Events" if present, else first sheet)
"""

import os
import json
from datetime import datetime, timedelta
from typing import List

import pandas as pd

EVENTS_JSON = "pogo_library/events/index.json"
DIGEST_CSV  = "POGO_Digest.csv"
DIGEST_XLSX = "POGO_Digest.xlsx"
ICS_PATH    = "POGO_Events.ics"

# ----------------- IO helpers -----------------

def read_events_df() -> pd.DataFrame:
    """Load events from JSON/CSV/XLSX (first available)."""
    # 1) JSON
    if os.path.exists(EVENTS_JSON):
        try:
            with open(EVENTS_JSON, "r", encoding="utf-8") as f:
                data = json.load(f)
            df = pd.DataFrame(data)
            if not df.empty:
                return df
        except Exception:
            pass

    # 2) CSV
    if os.path.exists(DIGEST_CSV):
        try:
            df = pd.read_csv(DIGEST_CSV)
            if not df.empty:
                return df
        except Exception:
            pass

    # 3) XLSX
    if os.path.exists(DIGEST_XLSX):
        try:
            xls = pd.ExcelFile(DIGEST_XLSX)
            sheet = "Events" if "Events" in xls.sheet_names else xls.sheet_names[0]
            df = pd.read_excel(DIGEST_XLSX, sheet_name=sheet)
            if not df.empty:
                return df
        except Exception:
            pass

    # Nothing found: return empty with expected columns
    return pd.DataFrame(columns=[
        "Start Date", "End Date", "Event Name",
        "Category (CD / CD Classic / Raid / Mega / Shadow Raid / Spotlight / Research / Other)",
        "Source", "Source URL"
    ])

# ----------------- Normalization -----------------

EXPECTED_COLS = [
    "Start Date", "End Date", "Event Name",
    "Category (CD / CD Classic / Raid / Mega / Shadow Raid / Spotlight / Research / Other)",
    "Source", "Source URL"
]

def ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
    for c in EXPECTED_COLS:
        if c not in df.columns:
            df[c] = ""
    # to strings where appropriate
    for c in ["Event Name",
              "Category (CD / CD Classic / Raid / Mega / Shadow Raid / Spotlight / Research / Other)",
              "Source", "Source URL"]:
        df[c] = df[c].fillna("").astype(str).str.strip()
    return df

def norm_date_like(val):
    """Coerce the incoming value to YYYY-MM-DD or None.
       Avoids slicing on non-strings (fix for 'float' not subscriptable)."""
    if val is None:
        return None
    try:
        if pd.isna(val):
            return None
    except Exception:
        pass

    if isinstance(val, str):
        s = val.strip()
        if not s:
            return None
        dt = pd.to_datetime(s, errors="coerce", utc=False, dayfirst=False)
        if pd.isna(dt):
            return None
        return dt.strftime("%Y-%m-%d")

    if isinstance(val, (int, float)):
        # Attempt unix timestamp (seconds then millis); otherwise treat as invalid
        for divisor in (1, 1000):
            try:
                if val > 10_000:  # rough guard
                    dt = datetime.utcfromtimestamp(val / divisor)
                    return dt.strftime("%Y-%m-%d")
            except Exception:
                pass
        return None

    try:
        dt = pd.to_datetime(val, errors="coerce", utc=False)
        if pd.isna(dt):
            return None
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return None

def add_date_flags(df: pd.DataFrame) -> pd.DataFrame:
    """Add 'Has Valid Dates' and 'Date Parse Status', and normalize dates."""
    start_raw = df["Start Date"]
    end_raw   = df["End Date"]

    start_norm = start_raw.apply(norm_date_like)
    end_norm   = end_raw.apply(norm_date_like)

    status = []
    has_valid = []
    s_vals = []
    e_vals = []

    for s, e in zip(start_norm, end_norm):
        if s and e:
            has_valid.append(True)
            status.append("ok")
            s_vals.append(s)
            e_vals.append(e)
        elif s and not e:
            has_valid.append(True)
            status.append("single")
            s_vals.append(s)
            e_vals.append(s)  # single-day
        elif not s and e:
            has_valid.append(True)
            status.append("end_only")
            # use end as both; still calendar-valid
            s_vals.append(e)
            e_vals.append(e)
        else:
            has_valid.append(False)
            status.append("missing")
            s_vals.append(None)
            e_vals.append(None)

    df = df.copy()
    df["Start Date"] = s_vals
    df["End Date"]   = e_vals
    df["Has Valid Dates"] = has_valid
    df["Date Parse Status"] = status
    return df

# ----------------- ICS writing -----------------

def ics_escape(text: str) -> str:
    return (text or "").replace("\\", "\\\\").replace(";", "\\;").replace(",", "\\,").replace("\n", "\\n")

def to_ics(df: pd.DataFrame) -> str:
    """Build VCALENDAR text from dated rows (all-day events)."""
    lines: List[str] = [
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-//pogo-ultimate//EN",
        "CALSCALE:GREGORIAN",
        "METHOD:PUBLISH",
    ]
    now = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

    dated = df[df["Has Valid Dates"] == True]  # noqa: E712

    for _, row in dated.iterrows():
        start = row["Start Date"]
        end   = row["End Date"]
        if not isinstance(start, str) or not start:
            continue
        if not isinstance(end, str) or not end:
            end = start

        # ICS all-day DTEND exclusive +1 day
        try:
            dt_end_excl = (pd.to_datetime(end) + timedelta(days=1)).strftime("%Y%m%d")
        except Exception:
            # last fallback; skip if parse fails
            try:
                dt_end_excl = (pd.to_datetime(start) + timedelta(days=1)).strftime("%Y%m%d")
            except Exception:
                continue

        summary = row.get("Event Name", "")
        category = row.get("Category (CD / CD Classic / Raid / Mega / Shadow Raid / Spotlight / Research / Other)", "")
        src = row.get("Source", "")
        src_url = row.get("Source URL", "")

        desc_parts = []
        if category:
            desc_parts.append(f"Category: {category}")
        if src:
            desc_parts.append(f"Source: {src}")
        if src_url:
            desc_parts.append(f"Link: {src_url}")
        description = "\\n".join(ics_escape(p) for p in desc_parts if p)

        event_lines = [
            "BEGIN:VEVENT",
            f"DTSTAMP:{now}",
            f"UID:{hash((start, end, summary))}@pogo-ultimate",
            f"SUMMARY:{ics_escape(summary)}",
            f"DTSTART;VALUE=DATE:{start.replace('-','')}",
            f"DTEND;VALUE=DATE:{dt_end_excl}",
        ]
        if description:
            event_lines.append(f"DESCRIPTION:{description}")
        event_lines.append("END:VEVENT")
        lines.extend(event_lines)

    lines.append("END:VCALENDAR")
    return "\r\n".join(lines) + "\r\n"

# ----------------- Excel/JSON writing -----------------

def write_excel_and_json(df: pd.DataFrame):
    """Write All/Events/Undated sheets and JSON exports."""
    # CSV (all rows)
    df.to_csv(DIGEST_CSV, index=False)

    # JSON (all rows)
    with open(DIGEST_XLSX.replace(".xlsx", ".json"), "w", encoding="utf-8") as f:
        json.dump(df.to_dict(orient="records"), f, ensure_ascii=False, indent=2)

    # JSON (undated only)
    undated = df[df["Has Valid Dates"] == False]  # noqa: E712
    with open("POGO_Digest_undated.json", "w", encoding="utf-8") as f:
        json.dump(undated.to_dict(orient="records"), f, ensure_ascii=False, indent=2)

    # Excel with three sheets
    with pd.ExcelWriter(DIGEST_XLSX, engine="openpyxl") as xw:
        df.to_excel(xw, sheet_name="All", index=False)
        df[df["Has Valid Dates"] == True].to_excel(xw, sheet_name="Events", index=False)   # noqa: E712
        undated.to_excel(xw, sheet_name="Undated", index=False)

# ----------------- main -----------------

def main():
    df = read_events_df()
    df = ensure_columns(df)
    df = add_date_flags(df)

    # Always write JSON/Excel first (preserve everything)
    os.makedirs("pogo_library/events", exist_ok=True)
    write_excel_and_json(df)

    # Write ICS from dated rows only
    ics_text = to_ics(df)
    with open(ICS_PATH, "w", encoding="utf-8", newline="\n") as f:
        f.write(ics_text)

    dated_n = int((df["Has Valid Dates"] == True).sum())   # noqa: E712
    print(f"Digest built: {len(df)} rows total → {dated_n} dated, {len(df) - dated_n} undated")
    print(f"ICS: {ICS_PATH} | Excel: {DIGEST_XLSX} | JSON: POGO_Digest.json, POGO_Digest_undated.json")

if __name__ == "__main__":
    main()