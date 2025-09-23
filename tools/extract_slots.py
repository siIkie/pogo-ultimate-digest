import re, json, os
from common.utils import http_get, soup_html, norm_whitespace, to_date, save_json

DATE_PAT = re.compile(r"(?:(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|"
                      r"Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)[ ,]+(\d{1,2})(?:[ ,]+(\d{4}))?)", re.I)
RANGE_PAT = re.compile(r"(\d{4}-\d{2}-\d{2})(?:\s*(?:to|-|–)\s*(\d{4}-\d{2}-\d{2}))")

def extract_date_range(text):
    text = norm_whitespace(text or "")
    m = RANGE_PAT.search(text)
    if m:
        return m.group(1), (m.group(2) or m.group(1))
    mm = DATE_PAT.findall(text)
    if mm:
        month, day, year = mm[0]
        s = f"{month} {day} {year or ''}".strip()
        d = to_date(s)
        return d, d
    return None, None

def extract_featured(text):
    t = (text or "").lower()
    for key in ["community day", "raid day", "spotlight hour", "raid hour"]:
        if key in t:
            return key.title()
    return None

def extract_region(text):
    if "city safari" in (text or "").lower():
        return "City Safari"
    return None

def enrich_events(in_path="pogo_library/events/index.json", out_path="pogo_library/events/index.json"):
    if not os.path.exists(in_path):
        return
    with open(in_path, "r", encoding="utf-8") as f:
        rows = json.load(f)

    out = []
    for r in rows:
        title = r.get("Event Name","")
        start, end = r.get("Start Date"), r.get("End Date")
        if not start or not end:
            s1, e1 = extract_date_range(title)
            if not s1 and r.get("Source URL"):
                try:
                    html = http_get(r["Source URL"])
                    text = soup_html(html).get_text(" ", strip=True)
                    s1, e1 = extract_date_range(text)
                except Exception:
                    pass
            if s1: r["Start Date"] = r["Start Date"] or s1
            if e1: r["End Date"] = r["End Date"] or e1

        r["Featured"] = r.get("Featured") or extract_featured(title)
        r["Bonuses"] = r.get("Bonuses") or None
        r["Region"] = r.get("Region") or extract_region(title)

        out.append(r)

    save_json(out_path, out)

if __name__ == "__main__":
    enrich_events()
