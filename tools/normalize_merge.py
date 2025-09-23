import json, os
from jsonschema import validate, Draft202012Validator
from common.utils import norm_title, save_json

def merge_pref(a, b):
    if a and not b: return a
    if b and not a: return b
    if a and b: return a if len(str(a)) >= len(str(b)) else b
    return a or b or None

def merge_rows(rows):
    by_key = {}
    for r in rows:
        key = (norm_title(r.get("Event Name","")), r.get("Start Date") or "", r.get("End Date") or "")
        if key not in by_key:
            by_key[key] = r
            by_key[key]["Sources"] = [r.get("Source")] if r.get("Source") else []
        else:
            cur = by_key[key]
            for k,v in r.items():
                if k in ("Source","Sources"): continue
                cur[k] = merge_pref(cur.get(k), v)
            if r.get("Source"):
                cur["Sources"] = list({*cur.get("Sources", []), r["Source"]})
    return list(by_key.values())

def validate_json(rows, schema_path):
    with open(schema_path, "r", encoding="utf-8") as f:
        schema = json.load(f)
    Draft202012Validator.check_schema(schema)
    validate(instance=rows, schema=schema)

def main():
    # Events
    if os.path.exists("pogo_library/events/index.json"):
        with open("pogo_library/events/index.json","r",encoding="utf-8") as f:
            events = json.load(f)
        events = merge_rows(events)
        validate_json(events, "schemas/events.schema.json")
        save_json("pogo_library/events/index.json", events)

    # Features
    if os.path.exists("pogo_library/features/index.json"):
        with open("pogo_library/features/index.json","r",encoding="utf-8") as f:
            feats = json.load(f)
        validate_json(feats, "schemas/features.schema.json")
        save_json("pogo_library/features/index.json", feats)

    # Balance
    if os.path.exists("pogo_library/balance/index.json"):
        with open("pogo_library/balance/index.json","r",encoding="utf-8") as f:
            bal = json.load(f)
        validate_json(bal, "schemas/balance.schema.json")
        save_json("pogo_library/balance/index.json", bal)

    # Wiki
    if os.path.exists("pogo_library/wiki/index.json"):
        with open("pogo_library/wiki/index.json","r",encoding="utf-8") as f:
            wiki = json.load(f)
        validate_json(wiki, "schemas/wiki.schema.json")
        save_json("pogo_library/wiki/index.json", wiki)

if __name__ == "__main__":
    main()
