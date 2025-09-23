import os, json, joblib
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.pipeline import Pipeline
from common.utils import norm_whitespace

DOMAINS = [
    ("events", "pogo_library/events/index.ndjson"),
    ("features","pogo_library/features/index.ndjson"),
    ("balance","pogo_library/balance/index.ndjson"),
    ("wiki","pogo_library/wiki/index.ndjson"),
]

def load_ndjson(path):
    rows = []
    if not os.path.exists(path): 
        return rows
    with open(path,"r",encoding="utf-8") as f:
        for line in f:
            line=line.strip()
            if not line: continue
            rows.append(json.loads(line))
    return rows

def row_text(domain, r):
    if domain=="events":
        return norm_whitespace(" ".join([
            r.get("Event Name",""), r.get("Category (CD / CD Classic / Raid / Mega / Shadow Raid / Spotlight / Research / Other)","") or "",
            r.get("Featured","") or "", r.get("Bonuses","") or "", r.get("Region","") or "",
            r.get("Source","") or ""
        ]))
    if domain=="features":
        return norm_whitespace(" ".join([r.get("Feature Name",""), r.get("Summary","") or "", r.get("Source","") or ""]))
    if domain=="balance":
        return norm_whitespace(" ".join([r.get("Change Title",""), r.get("Summary","") or "", r.get("Source","") or ""]))
    if domain=="wiki":
        return norm_whitespace(" ".join([r.get("Title",""), r.get("Summary","") or "", r.get("Source","") or ""]))
    return ""

def build_domain_index(domain, path):
    rows = load_ndjson(path)
    texts = [row_text(domain, r) for r in rows]
    pipe = Pipeline([("tfidf", TfidfVectorizer(max_features=50000, ngram_range=(1,2), lowercase=True))])
    X = pipe.fit_transform(texts)
    out_dir = f"indices/{domain}"
    os.makedirs(out_dir, exist_ok=True)
    joblib.dump(pipe, os.path.join(out_dir, "tfidf.joblib"))
    joblib.dump(X, os.path.join(out_dir, "matrix.joblib"))
    with open(os.path.join(out_dir, "rows.json"), "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)

def main():
    for domain, path in DOMAINS:
        build_domain_index(domain, path)

if __name__ == "__main__":
    main()
