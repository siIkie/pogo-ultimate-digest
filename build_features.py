import os, json, yaml, pandas as pd
from common.utils import http_get, rss_items, to_date, norm_title, norm_whitespace, dedupe_by_key, save_json, save_ndjson, first_paragraph_text

DEFAULT_FEATURE_KEYS = [
    "introducing","new feature","now available","coming soon",
    "trainers can now","launching","new way","feature update","feature rollout","companion update"
]

def is_featureish(text: str, extras=None) -> bool:
    txt = (text or "").lower()
    keys = (extras or []) + DEFAULT_FEATURE_KEYS
    return any(k in txt for k in keys)

def build_features(cfg):
    rows = []
    for entry in cfg.get("features", []):
        if not entry.get("enabled", True): 
            continue
        if entry.get("kind") != "rss":
            continue
        xml = http_get(entry["url"])
        for it in rss_items(xml):
            combined = f"{it['title']} {it['description']}"
            if is_featureish(combined, entry.get("feature_keywords")):
                summary = ""
                if it["link"]:
                    try:
                        article_html = http_get(it["link"])
                        summary = first_paragraph_text(article_html)
                    except Exception:
                        summary = norm_whitespace(it["description"]) or ""
                rows.append({
                    "Date Announced": to_date(it["pubDate"]) or "",
                    "Feature Name": it["title"],
                    "Category": "Feature",
                    "Summary": summary or norm_whitespace(it["description"]) or "",
                    "Source URL": it["link"],
                    "Source": entry["name"]
                })
    rows = dedupe_by_key(rows, lambda r: (r["Date Announced"], norm_title(r["Feature Name"])))
    rows.sort(key=lambda r: (r["Date Announced"], r["Feature Name"]), reverse=True)
    return rows

def write_features_outputs(rows):
    os.makedirs("pogo_library/features", exist_ok=True)
    save_json("pogo_library/features/index.json", rows)
    save_ndjson("pogo_library/features/index.ndjson", rows)

    df = pd.DataFrame(rows)
    df.to_csv("POGO_Features.csv", index=False)
    with open("POGO_Features.json","w",encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)

def main():
    with open("sources/sources.yaml","r",encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    rows = build_features(cfg)
    write_features_outputs(rows)

if __name__ == "__main__":
    main()
