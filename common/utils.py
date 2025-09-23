import os, re, json, hashlib
import requests
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from dateutil import parser as dateparser

UA = "POGO-Digest-Bot/1.0 (+github actions; repo issues contact)"
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": UA,
    "Accept": "text/html,application/rss+xml;q=0.9,*/*;q=0.8",
})
TIMEOUT = 30
CACHE_DIR = ".cache/http"
os.makedirs(CACHE_DIR, exist_ok=True)

def _cache_path(url: str) -> str:
    h = hashlib.sha1(url.encode("utf-8")).hexdigest()
    return os.path.join(CACHE_DIR, h + ".cache")

def http_get(url: str, use_cache=True) -> str:
    cp = _cache_path(url)
    if use_cache and os.path.exists(cp):
        try:
            with open(cp, "r", encoding="utf-8") as f:
                return f.read()
        except Exception:
            pass
    resp = SESSION.get(url, timeout=TIMEOUT)
    resp.raise_for_status()
    text = resp.text
    if use_cache:
        with open(cp, "w", encoding="utf-8") as f:
            f.write(text)
    return text

def norm_whitespace(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def norm_title(s: str) -> str:
    return norm_whitespace(s).lower()

def to_date(s: str):
    s = (s or "").strip()
    if not s:
        return None
    try:
        return dateparser.parse(s, fuzzy=True).date().isoformat()
    except Exception:
        return None

def soup_html(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "lxml")

def rss_items(xml_text: str):
    soup = BeautifulSoup(xml_text, "xml")
    for item in soup.find_all("item"):
        yield {
            "title": norm_whitespace(item.title.get_text(strip=True)) if item.title else "",
            "link": norm_whitespace(item.link.get_text(strip=True)) if item.link else "",
            "pubDate": norm_whitespace(item.pubDate.get_text(strip=True)) if item.pubDate else "",
            "description": item.description.get_text(" ", strip=True) if item.description else "",
        }

def dedupe_by_key(rows, keyfunc):
    seen, out = set(), []
    for r in rows:
        try:
            k = keyfunc(r)
        except Exception:
            k = None
        if not k:
            out.append(r); continue
        if k not in seen:
            seen.add(k); out.append(r)
    return out

def save_json(path, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def save_ndjson(path, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

def safe_join(base, href):
    try:
        return urljoin(base, href)
    except Exception:
        return href

def first_paragraph_text(html: str) -> str:
    soup = soup_html(html)
    candidates = soup.select("article p") or soup.select("main p") or soup.find_all("p")
    for p in candidates:
        text = norm_whitespace(p.get_text(" ", strip=True))
        if len(text) >= 40:
            return text
    mt = soup.find("meta", attrs={"name": "description"}) or soup.find("meta", attrs={"property":"og:description"})
    if mt and mt.get("content"):
        return norm_whitespace(mt["content"])
    return ""
