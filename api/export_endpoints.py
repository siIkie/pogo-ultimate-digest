import os, json, datetime as dt

def load_json(path):
    if not os.path.exists(path): return []
    with open(path,"r",encoding="utf-8") as f:
        return json.load(f)

def ensure_dir(p):
    os.makedirs(os.path.dirname(p), exist_ok=True)

def filter_events(events, start=None, end=None):
    out = []
    for e in events:
        s = e.get("Start Date") or ""
        if start and s and s < start: 
            continue
        if end and s and s > end:
            continue
        out.append(e)
    return out

def main():
    today = dt.date.today().isoformat()

    events = load_json("pogo_library/events/index.json")
    features = load_json("pogo_library/features/index.json")
    balance = load_json("pogo_library/balance/index.json")
    wiki = load_json("pogo_library/wiki/index.json")

    ensure_dir("api/out/_")

    with open("api/events.json","w",encoding="utf-8") as f: json.dump(events, f, ensure_ascii=False, indent=2)
    with open("api/features.json","w",encoding="utf-8") as f: json.dump(features, f, ensure_ascii=False, indent=2)
    with open("api/balance.json","w",encoding="utf-8") as f: json.dump(balance, f, ensure_ascii=False, indent=2)
    with open("api/wiki.json","w",encoding="utf-8") as f: json.dump(wiki, f, ensure_ascii=False, indent=2)

    next_30 = (dt.date.today() + dt.timedelta(days=30)).isoformat()
    upcoming = filter_events(events, start=today, end=next_30)
    with open("api/events_upcoming_30d.json","w",encoding="utf-8") as f:
        json.dump(upcoming, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
