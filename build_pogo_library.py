import os, json, yaml, pandas as pd
from common.utils import http_get, rss_items, to_date, norm_title, norm_whitespace, soup_html, dedupe_by_key, save_json, save_ndjson

def categorize_event(title: str) -> str:
    t = (title or "").lower()
    if "community day" in t: return "Community Day"
    if "spotlight hour" in t: return "Spotlight"
    if "raid hour" in t or "raid day" in t or "5-star raid" in t or "mega" in t or "shadow raid" in t: return "Raid/Mega"
    if "research" in t or "field research" in t: return "Research"
    return "Event/News"

def parse_event_sources(cfg):
    rows = []
    for entry in cfg.get("events", []):
        if not entry.get("enabled", True): 
            continue
        kind, url, name = entry.get("kind"), entry.get("url"), entry.get("name")
        if kind == "rss":
            xml = http_get(url)
            for it in rss_items(xml):
                title = it["title"]
                rows.append({
                    "Start Date": to_date(it["pubDate"]),
                    "End Date": to_date(it["pubDate"]),
                    "Event Name": title,
                    "Category (CD / CD Classic / Raid / Mega / Shadow Raid / Spotlight / Research / Other)": categorize_event(title),
                    "Source": name,
                    "Source URL": it["link"]
                })
        elif kind == "html":
            html = http_get(url)
            soup = soup_html(html)
            seen = set()
            for node in soup.select("h1, h2, h3, a, .event, .card, .post, .item"):
                t = norm_whitespace(node.get_text(" ", strip=True))
                if not t or len(t) < 5: continue
                key = t.lower()
                if key in seen: continue
                seen.add(key)
                rows.append({
                    "Start Date": "",
                    "End Date": "",
                    "Event Name": t,
                    "Category (CD / CD Classic / Raid / Mega / Shadow Raid / Spotlight / Research / Other)": categorize_event(t),
                    "Source": name,
                    "Source URL": url
                })
    return rows

def write_event_outputs(rows):
    rows = dedupe_by_key(rows, lambda r: norm_title(r.get("Event Name","")))
    rows.sort(key=lambda r: (r.get("Start Date") or "", r.get("Event Name") or ""))

    os.makedirs("pogo_library/events", exist_ok=True)
    save_json("pogo_library/events/index.json", rows)
    save_ndjson("pogo_library/events/index.ndjson", rows)

    df = pd.DataFrame(rows)
    df.to_csv("POGO_Digest.csv", index=False)
    with open("POGO_Digest.json","w",encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)

    with pd.ExcelWriter("POGO_Digest.xlsx", engine="openpyxl") as xw:
        df.to_excel(xw, sheet_name="Events", index=False)

def main():
    with open("sources/sources.yaml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    rows = parse_event_sources(cfg)
    write_event_outputs(rows)

if __name__ == "__main__":
    main()
