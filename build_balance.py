import os, json, yaml, pandas as pd
from common.utils import http_get, rss_items, to_date, norm_title, norm_whitespace, dedupe_by_key, save_json, save_ndjson, first_paragraph_text

DEFAULT_BALANCE = [
    "balance update","adjustments","nerf","buff","cp update",
    "move update","move rebalance","stat change","combat update","pvp update","gbl changes"
]

def mentions_balance(text: str, extra=None) -> bool:
    txt = (text or "").lower()
    keys = (extra or []) + DEFAULT_BALANCE
    return any(k in txt for k in keys)

def build_balance(cfg):
    rows = []
    for entry in cfg.get("balance", []):
        if not entry.get("enabled", True): 
            continue
        if entry.get("kind") != "rss":
            continue
        xml = http_get(entry["url"])
        for it in rss_items(xml):
            combined = f"{it['title']} {it['description']}"
            if mentions_balance(combined, entry.get("balance_keywords")):
                summary = ""
                if it["link"]:
                    try:
                        article_html = http_get(it["link"])
                        summary = first_paragraph_text(article_html)
                    except Exception:
                        summary = norm_whitespace(it["description"]) or ""
                rows.append({
                    "Date Announced": to_date(it["pubDate"]) or "",
                    "Change Title": it["title"],
                    "Type": "Balance",
                    "Summary": summary or norm_whitespace(it["description"]) or "",
                    "Source URL": it["link"],
                    "Source": entry["name"]
                })
    rows = dedupe_by_key(rows, lambda r: (r["Date Announced"], norm_title(r["Change Title"])))
    rows.sort(key=lambda r: (r["Date Announced"], r["Change Title"]), reverse=True)
    return rows

def write_balance_outputs(rows):
    os.makedirs("pogo_library/balance", exist_ok=True)
    save_json("pogo_library/balance/index.json", rows)
    save_ndjson("pogo_library/balance/index.ndjson", rows)

    df = pd.DataFrame(rows)
    df.to_csv("POGO_Balance.csv", index=False)
    with open("POGO_Balance.json","w",encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)

def main():
    with open("sources/sources.yaml","r",encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    rows = build_balance(cfg)
    write_balance_outputs(rows)

if __name__ == "__main__":
    main()
