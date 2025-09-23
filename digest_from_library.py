import os, json, pandas as pd, datetime as dt
from icalendar import Calendar, Event

def build_ics(events_csv="POGO_Digest.csv", out_ics="POGO_Events.ics"):
    if not os.path.exists(events_csv):
        return
    df = pd.read_csv(events_csv)
    cal = Calendar()
    cal.add("prodid", "-//POGO Digest//EN")
    cal.add("version", "2.0")

    for _, row in df.iterrows():
        name = row.get("Event Name") or "Pokémon GO Event"
        start = (row.get("Start Date") or "")[:10]
        end = (row.get("End Date") or "")[:10]
        cat = row.get("Category (CD / CD Classic / Raid / Mega / Shadow Raid / Spotlight / Research / Other)") or "Event/News"

        e = Event()
        e.add("summary", name)
        try:
            if start:
                e.add("dtstart", dt.datetime.fromisoformat(start))
            if end:
                e.add("dtend", dt.datetime.fromisoformat(end))
        except Exception:
            pass
        e.add("description", cat)
        cal.add_component(e)

    with open(out_ics, "wb") as f:
        f.write(cal.to_ical())

def bundle_excel(out_xlsx="POGO_Digest.xlsx"):
    sheets = []
    if os.path.exists("POGO_Digest.csv"):
        sheets.append(("Events", pd.read_csv("POGO_Digest.csv")))
    if os.path.exists("POGO_Features.csv"):
        sheets.append(("Features", pd.read_csv("POGO_Features.csv")))
    if os.path.exists("POGO_Balance.csv"):
        sheets.append(("Balance", pd.read_csv("POGO_Balance.csv")))
    if os.path.exists("POGO_Wiki_Library.json"):
        with open("POGO_Wiki_Library.json","r",encoding="utf-8") as f:
            wiki = json.load(f)
        sheets.append(("Wiki", pd.DataFrame(wiki)))

    if not sheets:
        return

    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as xw:
        for name, df in sheets:
            df.to_excel(xw, sheet_name=name[:31], index=False)

def main():
    build_ics()
    bundle_excel()

if __name__ == "__main__":
    main()
