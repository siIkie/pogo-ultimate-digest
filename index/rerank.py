import os, json, joblib
from datetime import date

def recency_weight(row):
    d = row.get("Date Announced") or row.get("Start Date") or ""
    try:
        y,m,dd = map(int, d.split("-"))
        days = (date.today() - date(y,m,dd)).days
        return max(0.5, min(1.5, 1.5 - days/365.0))
    except Exception:
        return 1.0

def search(domain, query, topk=20):
    base = f"indices/{domain}"
    pipe = joblib.load(os.path.join(base,"tfidf.joblib"))
    X = joblib.load(os.path.join(base,"matrix.joblib"))
    with open(os.path.join(base,"rows.json"),"r",encoding="utf-8") as f:
        rows = json.load(f)
    qv = pipe.transform([query])
    sims = (X @ qv.T).toarray().ravel()
    scored = []
    for i, s in enumerate(sims):
        scored.append((i, float(s) * recency_weight(rows[i])))
    scored.sort(key=lambda x: x[1], reverse=True)
    out = []
    for i, sc in scored[:topk]:
        r = rows[i]
        r["_score"] = sc
        out.append(r)
    return out

if __name__ == "__main__":
    import sys, json
    domain = sys.argv[1] if len(sys.argv)>1 else "events"
    query = " ".join(sys.argv[2:]) if len(sys.argv)>2 else "dialga raid hour"
    print(json.dumps(search(domain, query, 10), ensure_ascii=False, indent=2))
