import re, json
from dateutil import parser as dateparser

def parse_datespan(q: str):
    m = re.findall(r"\d{4}-\d{2}-\d{2}", q)
    if m:
        start = m[0]
        end = m[1] if len(m) > 1 else None
        return start, end
    try:
        d = dateparser.parse(q, fuzzy=True).date().isoformat()
        return d, None
    except Exception:
        return None, None

def route(q: str):
    ql = q.lower()
    if any(k in ql for k in ["balance","nerf","buff","move update","rebalance","gbl"]):
        return "balance"
    if any(k in ql for k in ["feature","now available","introducing","coming soon"]):
        return "features"
    if any(k in ql for k in ["guide","tips","how to","best","wiki"]):
        return "wiki"
    return "events"

if __name__ == "__main__":
    import sys
    q = " ".join(sys.argv[1:]) if len(sys.argv)>1 else "events this week"
    print(json.dumps({"route": route(q), "dates": parse_datespan(q)}))
