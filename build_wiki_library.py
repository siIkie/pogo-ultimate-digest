import os, json, yaml, pandas as pd
from common.utils import http_get, soup_html, norm_whitespace, norm_title, dedupe_by_key, save_json, save_ndjson, safe_join, first_paragraph_text

def extract_items_from_list_page(base_url: str, html: str, allow_terms=None, max_items=250):
    soup = soup_html(html)
    items = []
    for a in soup.select("a"):
        t = norm_whitespace(a.get_text(" ", strip=True))
        if not t:
            continue
        if allow_terms:
            T = t.lower()
            if not any(term.lower() in T for term in allow_terms):
                continue
        href = a.get("href") or ""
        if not href:
            continue
        url = safe_join(base_url, href)
        items.append({"title": t, "url": url})
        if len(items) >= max_items:
            break
    return items

def build_wiki(cfg):
    rows = []
    for src in cfg.get("wiki", []):
        if not src.get("enabled", True):
            continue
        url = src.get("url")
        allow = src.get("allow")
        try:
            html = http_get(url)
        except Exception:
            continue
        items = extract_items_from_list_page(url, html, allow_terms=allow, max_items=250)
        for it in items:
            summary = ""
            try:
                art_html = http_get(it["url"])
                summary = first_paragraph_text(art_html)
            except Exception:
                summary = ""
            rows.append({
                "Title": it["title"],
                "Category": "Guide/Tip",
                "Source": src["name"],
                "Source URL": it["url"],
                "Summary": summary
            })
    rows = dedupe_by_key(rows, lambda r: (r["Source"], norm_title(r["Title"])))
    rows.sort(key=lambda r: (r["Source"], r["Title"]))
    return rows

def write_wiki_outputs(rows):
    os.makedirs("pogo_library/wiki", exist_ok=True)
    save_json("pogo_library/wiki/index.json", rows)
    save_ndjson("pogo_library/wiki/index.ndjson", rows)

    with open("POGO_Wiki_Library.json","w",encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)

def main():
    with open("sources/sources.yaml","r",encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    rows = build_wiki(cfg)
    write_wiki_outputs(rows)

if __name__ == "__main__":
    main()
