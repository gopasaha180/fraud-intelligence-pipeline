# FBI Pipeline
# Run: python FBI.py
# Output: outputs/fbi_tagged_chunks.jsonl

import os

# ── Path config ────────────────────────────────────────────────
# Driven by orchestrator env var when run via nbconvert.
# Running standalone in VS Code? Set FRAUD_BASE_DIR as an env var,
# or update the fallback path below — it is the ONLY place you need
# to change the path in this notebook.
BASE_DIR      = os.environ.get(
    'FRAUD_BASE_DIR',
    r'C:\\Users\\josephsingleton\\fraud-dashboard'   # <-- update fallback for local use
)
METADATA_DIR  = os.path.join(BASE_DIR, 'data', 'metadata')
FULLTEXT_DIR  = os.path.join(BASE_DIR, 'data', 'fulltext')
PDF_DIR       = os.path.join(BASE_DIR, 'data', 'raw_pdfs')
DICT_DIR      = os.path.join(BASE_DIR, 'data', 'dictionaries')
OUTPUT_FOLDER = os.path.join(BASE_DIR, 'outputs')
for folder in [METADATA_DIR, FULLTEXT_DIR, PDF_DIR, DICT_DIR, OUTPUT_FOLDER]:
    os.makedirs(folder, exist_ok=True)
print('BASE_DIR:', BASE_DIR)
print('Ready.')

# ============================================================
# FBI Fraud Intelligence Platform — Pipeline
# ============================================================
# Cell structure:
#   Cell 1  Collect article URLs via RSS + Wayback CDX   → fbi_listings.csv
#   Cell 2  Fetch article content (RSS desc + Wayback HTML) → fbi_articles_raw.jsonl
#   Cell 3  Clean + enrich                               → fbi_master.jsonl / .csv
#   Cell 4  Chunk + fraud tagging                        → fbi_tagged_chunks.jsonl
# ============================================================


# ── CELL 1 ── Collect ALL article URLs: RSS (current) + Wayback CDX (historical)
#
# WHY TWO SOURCES:
#   RSS feeds only expose the publisher's most recent ~20-50 items. Once an
#   article ages out, it is gone from RSS. To maximise historical depth we
#   query the Wayback Machine CDX API, which has indexed fbi.gov press releases
#   back to ~2015. We then union-deduplicate both sets so every URL is unique.
#
# HOW WAYBACK CDX WORKS:
#   The CDX API returns a JSON list of (timestamp, original_url) pairs — one
#   per archived snapshot of a URL. We use collapse=urlkey so each unique page
#   appears only once, then store the best-available timestamp for use in Cell 2
#   when fetching archived HTML.
#
# WHY RSS STILL MATTERS:
#   RSS gives us the most-recent articles (last 30-60 days) that may not yet
#   be indexed by Wayback, plus it returns clean pubDate values we can use
#   directly as the article date without any Wayback timestamp parsing.

# pip install beautifulsoup4 lxml pandas requests tqdm  (if not already installed)

import os
import re
import time
import json
import hashlib

import requests
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from tqdm import tqdm

os.makedirs(METADATA_DIR, exist_ok=True)
os.makedirs(FULLTEXT_DIR, exist_ok=True)

# ── Config ──────────────────────────────────────────────────────────────────

# How far back to pull from Wayback CDX (YYYYMMDD)
CDX_FROM_DATE   = "20200101"   # change to go further back, e.g. "20180101"
CDX_TO_DATE     = ""           # empty string = up to today
CDX_LIMIT       = 5000         # max CDX rows per query; raise if you want more
SLEEP_RSS        = 1.0          # seconds between RSS feed requests
SLEEP_CDX        = 1.5          # seconds between CDX API calls (be polite)

# ── RSS feed URLs ─────────────────────────────────────────────────────────
RSS_FEEDS = [
    # National topic feeds
    "https://www.fbi.gov/feeds/fbi-in-the-news/rss.xml",
    "https://www.fbi.gov/feeds/press-releases/rss.xml",
    # Financial / cyber crime focused field offices (highest volume)
    "https://www.fbi.gov/contact-us/field-offices/newyork/news/press-releases/rss.xml",
    "https://www.fbi.gov/contact-us/field-offices/losangeles/news/press-releases/rss.xml",
    "https://www.fbi.gov/contact-us/field-offices/washingtondc/news/press-releases/rss.xml",
    "https://www.fbi.gov/contact-us/field-offices/miami/news/press-releases/rss.xml",
    "https://www.fbi.gov/contact-us/field-offices/chicago/news/press-releases/rss.xml",
    "https://www.fbi.gov/contact-us/field-offices/houston/news/press-releases/rss.xml",
    "https://www.fbi.gov/contact-us/field-offices/atlanta/news/press-releases/rss.xml",
    "https://www.fbi.gov/contact-us/field-offices/boston/news/press-releases/rss.xml",
    "https://www.fbi.gov/contact-us/field-offices/dallas/news/press-releases/rss.xml",
    "https://www.fbi.gov/contact-us/field-offices/sanfrancisco/news/press-releases/rss.xml",
    "https://www.fbi.gov/contact-us/field-offices/tampa/news/press-releases/rss.xml",
    "https://www.fbi.gov/contact-us/field-offices/phoenix/news/press-releases/rss.xml",
    "https://www.fbi.gov/contact-us/field-offices/charlotte/news/press-releases/rss.xml",
]

# ── Wayback CDX query templates ───────────────────────────────────────────
# We query two URL patterns that cover all FBI press release paths:
#   /news/press-releases/*           (national)
#   /contact-us/field-offices/*/news/press-releases/*   (field offices)
CDX_PATTERNS = [
    "fbi.gov/news/press-releases/*",
    "fbi.gov/contact-us/field-offices/*/news/press-releases/*",
]
CDX_BASE = "http://web.archive.org/cdx/search/cdx"

# ── Headers ───────────────────────────────────────────────────────────────
RSS_HEADERS = {
    "User-Agent": "python-requests/2.31 (academic research; contact: your_email@uncc.edu)",
    "Accept":     "application/rss+xml, application/xml, text/xml, */*",
}
CDX_HEADERS = {
    "User-Agent": "python-requests/2.31 (academic research; contact: your_email@uncc.edu)",
}

# ── Helpers ───────────────────────────────────────────────────────────────

def normalize_space(text: str) -> str:
    return re.sub(r"\\s+", " ", (text or "")).strip()

def stable_doc_id(url: str) -> str:
    """Stable SHA1-based doc ID, identical to all other pipelines."""
    return hashlib.sha1(url.encode("utf-8")).hexdigest()[:14]

def fetch_rss(url: str) -> str:
    resp = requests.get(url, headers=RSS_HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.text

def parse_rss_feed(feed_url: str) -> list:
    """
    Parse one RSS feed → list of listing dicts with keys:
        title_hint, url, published_raw, source_feed, data_source='rss'
    """
    try:
        xml = fetch_rss(feed_url)
    except Exception as e:
        print(f"  SKIP feed (fetch failed): {feed_url} → {e}")
        return []

    soup  = BeautifulSoup(xml, "xml")
    items = []
    for item in soup.find_all("item"):
        link_tag = item.find("link")
        guid_tag = item.find("guid")
        url = ""
        if link_tag:
            url = normalize_space(link_tag.get_text(" ", strip=True))
        if not url and guid_tag:
            url = normalize_space(guid_tag.get_text(" ", strip=True))
        if not url or not url.startswith("http"):
            continue

        title_tag = item.find("title")
        title     = normalize_space(title_tag.get_text(" ", strip=True)) if title_tag else ""

        pub_tag   = item.find("pubDate")
        published = normalize_space(pub_tag.get_text(" ", strip=True)) if pub_tag else ""

        items.append({
            "title_hint":    title,
            "url":           url,
            "published_raw": published,
            "source_feed":   feed_url,
            "data_source":   "rss",         # tag so NB2 knows not to use Wayback
            "wayback_ts":    "",            # empty for RSS items
        })
    return items

def fetch_cdx_urls(url_pattern: str,
                   from_date:   str = CDX_FROM_DATE,
                   to_date:     str = CDX_TO_DATE,
                   limit:       int = CDX_LIMIT) -> list:
    """
    Query the Wayback CDX API for all archived snapshots of url_pattern.
    Returns a list of dicts: {url, wayback_ts, published_raw, data_source='cdx'}.

    CDX returns one row per unique URL (collapse=urlkey) with the earliest
    available timestamp. We store that timestamp so Cell 2 can reconstruct
    the archived page URL: https://web.archive.org/web/<ts>/<url>
    """
    params = {
        "url":        url_pattern,
        "output":     "json",
        "fl":         "timestamp,original,statuscode",
        "collapse":   "urlkey",
        "filter":     "statuscode:200",   # only snapshots that were HTTP 200
        "limit":      str(limit),
    }
    if from_date:
        params["from"] = from_date
    if to_date:
        params["to"]   = to_date

    try:
        resp = requests.get(CDX_BASE, params=params, headers=CDX_HEADERS, timeout=60)
        resp.raise_for_status()
        rows = resp.json()
    except Exception as e:
        print(f"  SKIP CDX pattern (fetch failed): {url_pattern} → {e}")
        return []

    if not rows or len(rows) <= 1:
        # CDX returns a header row first — if only 1 row, no results
        print(f"  CDX: no results for {url_pattern}")
        return []

    # First row is the column header; skip it
    header, *data_rows = rows
    # ts_idx, orig_idx = header.index("timestamp"), header.index("original")

    results = []
    for row in data_rows:
        ts   = row[0]   # timestamp  e.g. "20231015142233"
        orig = row[1]   # original URL
        # Normalise URL: strip trailing slashes, lowercase scheme+host
        orig = orig.rstrip("/")
        if not orig.startswith("http"):
            orig = "https://" + orig
        # Convert CDX timestamp → ISO-ish date string for published_raw
        # Format: YYYYMMDDHHMMSS → YYYY-MM-DD
        pub_raw = f"{ts[:4]}-{ts[4:6]}-{ts[6:8]}" if len(ts) >= 8 else ""
        results.append({
            "title_hint":    "",    # CDX has no title — NB2 extracts it from HTML
            "url":           orig,
            "published_raw": pub_raw,
            "source_feed":   "wayback_cdx",
            "data_source":   "cdx",   # tag so NB2 knows to use Wayback HTML fetch
            "wayback_ts":    ts,      # full timestamp for Wayback URL construction
        })
    return results

# ── STEP 1: Pull RSS feeds ────────────────────────────────────────────────

print("=" * 60)
print("STEP 1: Pulling RSS feeds (current articles)")
print("=" * 60)

rss_listings = []
seen_urls    = set()

for feed_url in tqdm(RSS_FEEDS, desc="RSS feeds"):
    items     = parse_rss_feed(feed_url)
    new_items = [it for it in items if it["url"] not in seen_urls]
    for it in new_items:
        rss_listings.append(it)
        seen_urls.add(it["url"])
    label = feed_url.split("field-offices/")[1].split("/")[0].title() if "field-offices" in feed_url else "national"
    print(f"  {label}: +{len(new_items)} new  (running total={len(rss_listings)})")
    time.sleep(SLEEP_RSS)

print(f"\nRSS total: {len(rss_listings)} unique URLs")

# ── STEP 2: Pull Wayback CDX for historical URLs ──────────────────────────

print("\n" + "=" * 60)
print(f"STEP 2: Querying Wayback CDX  (from={CDX_FROM_DATE or 'all time'})")
print("=" * 60)

cdx_listings = []

for pattern in tqdm(CDX_PATTERNS, desc="CDX patterns"):
    print(f"\n  Querying CDX: {pattern}")
    rows     = fetch_cdx_urls(pattern)
    new_rows = [r for r in rows if r["url"] not in seen_urls]
    for r in new_rows:
        cdx_listings.append(r)
        seen_urls.add(r["url"])
    print(f"  CDX: {len(rows)} snapshots found, {len(new_rows)} new (not in RSS)")
    time.sleep(SLEEP_CDX)

print(f"\nCDX total: {len(cdx_listings)} historical URLs not already in RSS")

# ── STEP 3: Combine + save ─────────────────────────────────────────────────

all_listings = rss_listings + cdx_listings
print(f"\nCombined total: {len(all_listings)} unique URLs")
print(f"  RSS:     {len(rss_listings)}")
print(f"  CDX:     {len(cdx_listings)}")

# Preview
for entry in all_listings[:3]:
    print(f"  [{entry['data_source']:3s}] {entry['published_raw'][:10]}  {entry['url'][:80]}")

listings_path = os.path.join(METADATA_DIR, "fbi_listings.csv")
pd.DataFrame(all_listings).to_csv(listings_path, index=False)
print("\nSaved:", listings_path)

# ══════════════════════════════════════════════════════════════
# CELL 2 — Build article records from RSS + Wayback archived HTML
#
# Data source routing (set in Cell 1 via the 'data_source' field):
#
#   data_source == 'rss'
#       Use the RSS <description> field as the article body.
#       Fast, no extra HTTP calls — same approach as the original NB2.
#
#   data_source == 'cdx'
#       Fetch the archived HTML page from Wayback Machine:
#           https://web.archive.org/web/<wayback_ts>/<original_url>
#       Parse the article body using the FBI press release HTML structure.
#       This gives us full article text for historical articles that are
#       blocked on live fbi.gov.
#
# RATE LIMITING:
#   Wayback Machine allows ~1 req/sec before it starts returning 429s.
#   SLEEP_WAYBACK = 1.5 seconds is conservative and safe.
#   If you see repeated 429 errors, raise to 2.5.
#
# CHECKPOINT / RESUME:
#   Already-fetched doc_ids are tracked in fbi_articles_raw.jsonl.
#   On re-runs the pipeline skips URLs it already has, so it is safe
#   to interrupt and resume without duplicating data.
# ══════════════════════════════════════════════════════════════

import os, re, time, json, hashlib
import requests
import pandas as pd
from bs4 import BeautifulSoup
from tqdm import tqdm

os.makedirs(METADATA_DIR, exist_ok=True)
os.makedirs(FULLTEXT_DIR, exist_ok=True)

# ── Config ────────────────────────────────────────────────────
SLEEP_RSS     = 1.0    # seconds between RSS feed re-pulls
SLEEP_WAYBACK = 1.5    # seconds between Wayback HTML fetches

# ── Per-run Wayback fetch cap ─────────────────────────────────────────────
# Without a cap, a large CDX result set (5000 URLs × 1.5s) can run 2+ hours
# and trigger the orchestrator CellTimeoutError.
#
# This cap limits how many CDX (Wayback) articles are fetched per run.
# RSS articles are always processed first (they are instant — no HTTP fetch).
# The checkpoint/resume means the next orchestrator run picks up where this
# one left off, so you accumulate records incrementally across runs.
#
#   1500 CDX fetches × 1.5s ≈ 37 min  →  fits inside orchestrator timeout=7200s
#   Set to None to remove the cap (for standalone manual runs only).
MAX_CDX_PER_RUN = 1500

WAYBACK_FETCH_HEADERS = {
    "User-Agent": "python-requests/2.31 (academic research; contact: your_email@uncc.edu)",
    "Accept":     "text/html,application/xhtml+xml,*/*",
}
RSS_HEADERS = {
    "User-Agent": "python-requests/2.31 (academic research; contact: your_email@uncc.edu)",
    "Accept":     "application/rss+xml, application/xml, text/xml, */*",
}

# Same RSS feed list as Cell 1 (needed for the RSS description extraction path)
RSS_FEEDS = [
    "https://www.fbi.gov/feeds/fbi-in-the-news/rss.xml",
    "https://www.fbi.gov/feeds/press-releases/rss.xml",
    "https://www.fbi.gov/contact-us/field-offices/newyork/news/press-releases/rss.xml",
    "https://www.fbi.gov/contact-us/field-offices/losangeles/news/press-releases/rss.xml",
    "https://www.fbi.gov/contact-us/field-offices/washingtondc/news/press-releases/rss.xml",
    "https://www.fbi.gov/contact-us/field-offices/miami/news/press-releases/rss.xml",
    "https://www.fbi.gov/contact-us/field-offices/chicago/news/press-releases/rss.xml",
    "https://www.fbi.gov/contact-us/field-offices/houston/news/press-releases/rss.xml",
    "https://www.fbi.gov/contact-us/field-offices/atlanta/news/press-releases/rss.xml",
    "https://www.fbi.gov/contact-us/field-offices/boston/news/press-releases/rss.xml",
    "https://www.fbi.gov/contact-us/field-offices/dallas/news/press-releases/rss.xml",
    "https://www.fbi.gov/contact-us/field-offices/sanfrancisco/news/press-releases/rss.xml",
    "https://www.fbi.gov/contact-us/field-offices/tampa/news/press-releases/rss.xml",
    "https://www.fbi.gov/contact-us/field-offices/phoenix/news/press-releases/rss.xml",
    "https://www.fbi.gov/contact-us/field-offices/charlotte/news/press-releases/rss.xml",
]

# ── Helpers ────────────────────────────────────────────────────

def normalize_space(text: str) -> str:
    return re.sub(r"\\s+", " ", (text or "")).strip()

def stable_doc_id(url: str) -> str:
    return hashlib.sha1(url.encode("utf-8")).hexdigest()[:14]

def strip_html_tags(text: str) -> str:
    return BeautifulSoup(text or "", "lxml").get_text(" ", strip=True)

def fetch_rss(url: str) -> str:
    resp = requests.get(url, headers=RSS_HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.text

def source_label_from_feed(feed_url: str) -> str:
    if "field-offices" in feed_url:
        office = feed_url.split("field-offices/")[1].split("/")[0].title()
        return f"FBI - {office} Field Office"
    return "FBI - National"

def source_label_from_article_url(article_url: str) -> str:
    """Derive a source label from the article URL for CDX-sourced entries."""
    if "field-offices" in article_url:
        try:
            office = article_url.split("field-offices/")[1].split("/")[0].title()
            return f"FBI - {office} Field Office"
        except Exception:
            pass
    return "FBI - National"

def parse_fbi_article_html(html: str, url: str) -> dict:
    """
    Extract title + body text from a Wayback-archived FBI press release HTML page.

    FBI press release pages use consistent structure:
      - <h1 class="page-title"> or <h1> for the article title
      - <div class="field-items"> or <article> or <div role="main"> for the body

    Returns a dict with keys: title, body_text
    """
    soup = BeautifulSoup(html, "lxml")

    # Remove Wayback Machine toolbar injected HTML (present in archived pages)
    for tag in soup.select("#wm-ipp-base, #wm-ipp, #donato"):
        tag.decompose()

    # ── Title extraction ──────────────────────────────────────────
    title = ""
    for selector in [
        "h1.page-title",
        "h1.title",
        "h1",
        "title",
    ]:
        tag = soup.select_one(selector)
        if tag:
            title = normalize_space(tag.get_text(" ", strip=True))
            # Filter out generic site titles
            if title and "fbi.gov" not in title.lower() and len(title) > 5:
                break

    # ── Body extraction ───────────────────────────────────────────
    body_text = ""
    for selector in [
        "div.field-items",
        "div.field--name-body",
        "article",
        "div[role=\'main\']",
        "div.region-content",
        "main",
    ]:
        tag = soup.select_one(selector)
        if tag:
            # Remove nav / header / footer noise
            for noise in tag.select("nav, header, footer, .breadcrumb, .social-share"):
                noise.decompose()
            raw = tag.get_text(" ", strip=True)
            body_text = normalize_space(raw)
            if len(body_text) > 200:   # accept if we got meaningful content
                break

    return {"title": title, "body_text": body_text}

def fetch_wayback_article(original_url: str, wayback_ts: str) -> dict | None:
    """
    Fetch an FBI press release via Wayback Machine archived HTML.

    Constructs: https://web.archive.org/web/<ts>/<url>
    Falls back to the closest-available snapshot if the exact timestamp 404s.

    Returns parsed {title, body_text} or None on failure.
    """
    archived_url = f"https://web.archive.org/web/{wayback_ts}/{original_url}"
    try:
        resp = requests.get(
            archived_url,
            headers=WAYBACK_FETCH_HEADERS,
            timeout=30,
            allow_redirects=True,
        )
        if resp.status_code == 404:
            # Try without timestamp (Wayback will redirect to nearest snapshot)
            archived_url = f"https://web.archive.org/web/{original_url}"
            resp = requests.get(archived_url, headers=WAYBACK_FETCH_HEADERS, timeout=30)
        if resp.status_code != 200:
            return None
        return parse_fbi_article_html(resp.text, original_url)
    except Exception as e:
        print(f"  WARN: Wayback fetch failed for {original_url[:70]} → {e}")
        return None

# ── Load listings from Cell 1 ─────────────────────────────────────────────

listings_path = os.path.join(METADATA_DIR, "fbi_listings.csv")
listings_df   = pd.read_csv(listings_path)
print(f"Loaded {len(listings_df)} listings from Cell 1")
print(f"  data_source breakdown:\n{listings_df['data_source'].value_counts().to_string()}")

# ── Load existing records to enable checkpoint/resume ─────────────────────

raw_jsonl_path  = os.path.join(FULLTEXT_DIR, "fbi_articles_raw.jsonl")
existing_doc_ids = set()

if os.path.exists(raw_jsonl_path):
    with open(raw_jsonl_path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                rec = json.loads(line)
                existing_doc_ids.add(rec.get("doc_id", ""))
            except Exception:
                pass
    print(f"Checkpoint: {len(existing_doc_ids)} records already in {raw_jsonl_path} — will skip these")
else:
    print("No existing output file — starting fresh")

# ── Pull RSS descriptions (for data_source == 'rss' entries) ─────────────

print("\n--- Pulling RSS feed descriptions ---")
rss_data = {}   # url → {title, description, published_raw, source_label}

for feed_url in tqdm(RSS_FEEDS, desc="RSS feeds"):
    try:
        xml  = fetch_rss(feed_url)
    except Exception as e:
        print(f"  SKIP feed: {feed_url} → {e}")
        time.sleep(SLEEP_RSS)
        continue

    soup   = BeautifulSoup(xml, "xml")
    label  = source_label_from_feed(feed_url)

    for item in soup.find_all("item"):
        link_tag = item.find("link")
        guid_tag = item.find("guid")
        url = ""
        if link_tag:
            url = normalize_space(link_tag.get_text(" ", strip=True))
        if not url and guid_tag:
            url = normalize_space(guid_tag.get_text(" ", strip=True))
        if not url or not url.startswith("http"):
            continue

        title_tag = item.find("title")
        title     = normalize_space(title_tag.get_text(" ", strip=True)) if title_tag else ""

        pub_tag   = item.find("pubDate")
        published = normalize_space(pub_tag.get_text(" ", strip=True)) if pub_tag else ""

        desc_tag    = item.find("description")
        description = ""
        if desc_tag:
            description = normalize_space(strip_html_tags(desc_tag.get_text(" ", strip=True)))

        cats = sorted({
            normalize_space(c.get_text(" ", strip=True))
            for c in item.find_all("category")
            if c.get_text(strip=True)
        })

        rss_data[url] = {
            "title":         title,
            "body_raw":      description,
            "published_raw": published,
            "source":        label,
            "fbi_topics":    cats,
        }

    time.sleep(SLEEP_RSS)

print(f"RSS descriptions collected: {len(rss_data)}")

# ── Process all listings ──────────────────────────────────────────────────

print("\n--- Building article records ---")

all_records = []
n_skipped   = 0
n_rss       = 0
n_cdx_ok    = 0
n_cdx_fail  = 0

# Open output file in append mode so checkpoint works
out_f = open(raw_jsonl_path, "a", encoding="utf-8")

for _, row in tqdm(listings_df.iterrows(), total=len(listings_df), desc="Processing"):
    url         = row["url"]
    doc_id      = stable_doc_id(url)
    data_source = row.get("data_source", "rss")
    wayback_ts  = row.get("wayback_ts", "")

    # Skip already-processed docs (checkpoint/resume)
    if doc_id in existing_doc_ids:
        n_skipped += 1
        continue

    # ── CDX cap: stop Wayback fetches after MAX_CDX_PER_RUN this run ─────
    if data_source == "cdx" and MAX_CDX_PER_RUN is not None:
        if n_cdx_ok + n_cdx_fail >= MAX_CDX_PER_RUN:
            n_skipped += 1
            continue

    # ── Route 1: RSS — use cached description ────────────────────
    if data_source == "rss":
        rss_info = rss_data.get(url, {})
        record = {
            "doc_id":        doc_id,
            "title":         rss_info.get("title",         row.get("title_hint", "")),
            "url":           url,
            "published_raw": rss_info.get("published_raw", row.get("published_raw", "")),
            "body_raw":      rss_info.get("body_raw",      ""),
            "source":        rss_info.get("source",        "FBI - National"),
            "fbi_topics":    rss_info.get("fbi_topics",    []),
            "source_feed":   row.get("source_feed", ""),
            "data_source":   "rss",
        }
        n_rss += 1

    # ── Route 2: CDX — fetch archived HTML from Wayback ──────────
    else:
        fetched = fetch_wayback_article(url, str(wayback_ts))
        if fetched is None or not fetched.get("body_text"):
            n_cdx_fail += 1
            time.sleep(SLEEP_WAYBACK)
            continue

        record = {
            "doc_id":        doc_id,
            "title":         fetched["title"] or row.get("title_hint", ""),
            "url":           url,
            "published_raw": row.get("published_raw", ""),   # CDX timestamp → date set in Cell 1
            "body_raw":      fetched["body_text"],
            "source":        source_label_from_article_url(url),
            "fbi_topics":    [],
            "source_feed":   "wayback_cdx",
            "data_source":   "cdx",
        }
        n_cdx_ok += 1
        time.sleep(SLEEP_WAYBACK)   # be polite to Wayback

    all_records.append(record)
    existing_doc_ids.add(doc_id)
    out_f.write(json.dumps(record, ensure_ascii=False) + "\n")

out_f.close()

print(f"\n{'='*50}")
print(f"Records written this run:")
print(f"  RSS entries:          {n_rss}")
print(f"  CDX (Wayback OK):     {n_cdx_ok}")
print(f"  CDX (Wayback failed): {n_cdx_fail}")
print(f"  Skipped (checkpoint): {n_skipped}")
print(f"  Total new records:    {len(all_records)}")
print(f"  Output: {raw_jsonl_path}")

# Quick shape check on full file
full_df = pd.read_json(raw_jsonl_path, lines=True)
print(f"\nFull file shape: {full_df.shape}")
print(f"data_source breakdown:\n{full_df['data_source'].value_counts().to_string()}")
full_df[["doc_id","published_raw","title","data_source"]].head(5)

# ══════════════════════════════════════════════════════════════
# ── NOTEBOOK 3 ── Clean + enrich + export master dataset
# Goal:
#   - clean dates + text
#   - dedupe
#   - extract fraud signals (URLs / emails / phones / IPs / crypto)
#   - apply fraud tag taxonomy
#   - save fbi_master.jsonl + fbi_master.csv
# ══════════════════════════════════════════════════════════════

import os, re, json
import pandas as pd
from urllib.parse import urlparse


def normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()

# ── Load raw
raw_jsonl_path = os.path.join(FULLTEXT_DIR, "fbi_articles_raw.jsonl")
raw_df = pd.read_json(raw_jsonl_path, lines=True)
print("Raw shape:", raw_df.shape)

# ── Diagnostic: show exactly what columns came through from NB2
print("\nColumns in raw file:", raw_df.columns.tolist())
print("\nFirst row sample:")
print(raw_df.iloc[0].to_dict() if len(raw_df) > 0 else "EMPTY — NB2 produced no records")

# Guard: if the file is empty, stop here with a clear message
if raw_df.empty:
    raise ValueError(
        "fbi_articles_raw.jsonl is empty. "
        "NB2 scraped 0 articles — likely all article pages returned 403. "
        "Check the FAILED lines printed during NB2 and re-run NB2."
    )

# ── Normalise column names defensively
# NB2 writes 'published_raw' but if the JSONL was hand-edited or re-saved
# it may come through as 'published'. Handle both.
if "published_raw" not in raw_df.columns:
    if "published" in raw_df.columns:
        raw_df["published_raw"] = raw_df["published"]
        print("INFO: renamed 'published' → 'published_raw'")
    else:
        raw_df["published_raw"] = ""
        print("WARNING: no date column found — published_raw set to empty string")

if "body_raw" not in raw_df.columns:
    if "body" in raw_df.columns:
        raw_df["body_raw"] = raw_df["body"]
        print("INFO: renamed 'body' → 'body_raw'")
    else:
        raw_df["body_raw"] = ""
        print("WARNING: no body column found — body_raw set to empty string")

if "doc_id" not in raw_df.columns:
    import hashlib
    raw_df["doc_id"] = raw_df["url"].apply(
        lambda u: hashlib.sha1(u.encode()).hexdigest()[:14]
    )
    print("INFO: doc_id regenerated from URL")

if "source" not in raw_df.columns:
    raw_df["source"] = "FBI - Frauds & Scams"

if "fbi_topics" not in raw_df.columns:
    raw_df["fbi_topics"] = [[] for _ in range(len(raw_df))]

# ── Text cleaning (mirrors FTC clean_body)

def clean_body(text: str) -> str:
    if not text:
        return ""
    text = normalize_space(text)
    # Remove common boilerplate tails found on FBI pages
    text = re.sub(r"More from the FBI\..*$",          "", text, flags=re.IGNORECASE)
    text = re.sub(r"Contact Your Local FBI Office.*$", "", text, flags=re.IGNORECASE)
    text = re.sub(r"Submit a Tip.*$",                 "", text, flags=re.IGNORECASE)
    text = re.sub(r"Return to top",                   "", text, flags=re.IGNORECASE)
    return normalize_space(text)

df = raw_df.copy()

# Parse datetime
df["published"] = pd.to_datetime(df["published_raw"], errors="coerce", utc=True)
df["date"]  = df["published"].dt.date.astype(str)   # YYYY-MM-DD string
df["published_year"]  = df["published"].dt.year
df["published_month"] = df["published"].dt.month

# Clean text
df["title"]      = df["title"].astype(str).map(normalize_space)
df["body_1"]     = df["body_raw"].astype(str).map(clean_body)
df["body_short"] = df["body_1"].str[:1000]

# Dedupe on URL (keep latest content)
df = df.drop_duplicates(subset=["url"]).reset_index(drop=True)
print("\nCleaned shape:", df.shape)
df[["doc_id", "date", "title"]].head(3)

# ── Fraud signal extraction (identical regex set to FTC)

URL_RE   = re.compile(r"\bhttps?://[^\s)>\]]+", re.IGNORECASE)
EMAIL_RE = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.IGNORECASE)
PHONE_RE = re.compile(
    r"(?:(?:\+?1[\s\-\.])?)"
    r"(?:\(?\d{3}\)?[\s\-\.]?)"
    r"\d{3}[\s\-\.]?\d{4}\b"
)
IP_RE  = re.compile(
    r"\b(?:(?:25[0-5]|2[0-4]\d|[01]?\d?\d)\.){3}"
    r"(?:25[0-5]|2[0-4]\d|[01]?\d?\d)\b"
)
BTC_RE = re.compile(r"\b(bc1[0-9a-z]{25,62}|[13][a-km-zA-HJ-NP-Z1-9]{25,34})\b")
ETH_RE = re.compile(r"\b0x[a-fA-F0-9]{40}\b")

def extract_domains(urls):
    domains = set()
    for u in urls:
        try:
            netloc = urlparse(u).netloc.lower()
            if netloc:
                domains.add(netloc)
        except Exception:
            pass
    return sorted(domains)

def fraud_signals_from_text(text: str):
    text  = text or ""
    urls  = URL_RE.findall(text)
    emails = EMAIL_RE.findall(text)
    phones = PHONE_RE.findall(text)
    ips   = IP_RE.findall(text)
    btc   = BTC_RE.findall(text)
    eth   = ETH_RE.findall(text)
    return {
        "urls":           sorted(set(urls)),
        "domains":        extract_domains(urls),
        "emails":         sorted(set([e.lower() for e in emails])),
        "phones":         sorted(set([normalize_space(p) for p in phones])),
        "ip_addresses":   sorted(set(ips)),
        "crypto_wallets": sorted(set(btc + eth)),
    }

df["fraud_signals"] = df["body_1"].apply(fraud_signals_from_text)

# Quick check
df[["title"]].assign(
    n_domains=df["fraud_signals"].apply(lambda x: len(x["domains"])),
    n_emails =df["fraud_signals"].apply(lambda x: len(x["emails"])),
    n_phones =df["fraud_signals"].apply(lambda x: len(x["phones"])),
).head(5)

# ── Fraud Tag Taxonomy — aligned to canonical fraud_dictionary families ──────
FRAUD_TAG_RULES = [
    ("money_laundering",    r"\b(shell company|shell companies|front company|money mule|mule account|layering|structuring)\b"),
    ("check_fraud",         r"\b(check fraud|mail theft|stolen check|stolen checks|check washing|fraudulent check|altered check|forged check)\b"),
    ("sanctions",           r"\b(sanctions evasion|sanctioned entity|export control violation)\b"),
    ("terrorist_financing", r"\b(terrorist financing|terrorist organization)\b"),
    ("human_trafficking",   r"\b(human trafficking|labor trafficking)\b"),
    ("consumer_fraud",      r"\b(romance scam|lottery scam|charity fraud|investment scam|advance fee|imposter|government impersonator|business impersonator)\b"),
    ("identity_fraud",      r"\b(identity theft|stolen identity|synthetic identity|ssn|account takeover)\b"),
    ("benefits_fraud",      r"\b(government benefits fraud|medicaid fraud|pandemic relief fraud|unemployment fraud)\b"),
    ("cybercrime",          r"\b(ransomware|phishing|smishing|malware|business email compromise|bec|spear phishing|dark web)\b"),
    ("crypto_fraud",        r"\b(virtual currency|cryptocurrency|crypto exchange|pig butchering|crypto|bitcoin|ethereum|wallet address)\b"),
]

def assign_fraud_tags(title: str, body: str):
    text = f"{title} {body}".lower()
    tags = []
    for tag, pattern in FRAUD_TAG_RULES:
        if re.search(pattern, text, re.IGNORECASE):
            tags.append(tag)
    return tags if tags else ["other"]

df["fraud_tags"] = df.apply(lambda r: assign_fraud_tags(r["title"], r["body_1"]), axis=1)
df[["title", "fraud_tags", "fbi_topics"]].head(10)


# ── Quality checks (mirror FTC checks)
print("Missing values:")
print(df.isnull().sum())

print("\nShape:", df.shape)
print("\nColumns:")
print(df.columns.tolist())

# ── Final dataset (same field order as FTC final_df)
final_df = df[[
    "doc_id", "date", "published_year", "published_month",
    "source", "fraud_tags", "fraud_signals", "title", "body_1", "body_short", "url"
]].copy().reset_index(drop=True)

final_df["body_length"] = df["body_1"].str.len()
print("Shape:", final_df.shape)
print("\nColumns:")
print(final_df.columns.tolist())

# Look at one full article
print(final_df.loc[0, "body_1"])

# Distribution checks
df["fraud_tags"].explode().value_counts()
final_df["published_year"].value_counts()

# ── Export fbi_master.jsonl
jsonl_path = os.path.join(OUTPUT_FOLDER, "fbi_master.jsonl")
final_df.to_json(jsonl_path, orient="records", lines=True, force_ascii=False)
print("Saved:", jsonl_path)

# ── Export fbi_master.csv
csv_path = os.path.join(OUTPUT_FOLDER, "fbi_master.csv")
final_df_csv = final_df.copy()
final_df_csv["fraud_tags"]    = final_df_csv["fraud_tags"].apply(json.dumps)
final_df_csv["fraud_signals"] = final_df_csv["fraud_signals"].apply(json.dumps)
final_df_csv.to_csv(csv_path, index=False, encoding="utf-8")
print("Saved:", csv_path)

# ══════════════════════════════════════════════════════════════
# ── NOTEBOOK 4 ── Chunk + Fraud Tagging (mirrors FinCEN NB 5 & 6)
# Goal:
#   - load fbi_master.jsonl
#   - chunk each article (chunk_size=1200, overlap=200)
#   - apply fraud dictionary + signals dictionary
#   - save fbi_chunks.jsonl + fbi_tagged_chunks.jsonl
# ══════════════════════════════════════════════════════════════

import os, re, json
import pandas as pd

os.makedirs(DICT_DIR, exist_ok=True)

# ── Load master
master_path = os.path.join(OUTPUT_FOLDER, "fbi_master.jsonl")
results = []
with open(master_path, "r", encoding="utf-8") as f:
    for line in f:
        results.append(json.loads(line))
print("Documents loaded:", len(results))

# ── Canonical fraud dictionary (v2) ──────────────────────────────────────────
# ── Canonical fraud dictionary (v2) ──────────────────────────────────────────
fraud_dictionary = [
    # Money laundering
    ("shell company",             "money_laundering"),
    ("shell companies",           "money_laundering"),
    ("shell corporation",         "money_laundering"),
    ("front company",             "money_laundering"),
    ("money mule",                "money_laundering"),
    ("mule account",              "money_laundering"),
    ("layering",                  "money_laundering"),
    ("structuring",               "money_laundering"),
    ("smurfing",                  "money_laundering"),
    ("placement",                 "money_laundering"),
    ("beneficial owner",          "money_laundering"),
    # Check fraud
    ("check fraud",               "check_fraud"),
    ("mail theft",                "check_fraud"),
    ("stolen check",              "check_fraud"),
    ("stolen checks",             "check_fraud"),
    ("check washing",             "check_fraud"),
    ("fraudulent check",          "check_fraud"),
    ("altered check",             "check_fraud"),
    ("forged check",              "check_fraud"),
    # Sanctions
    ("sanctions evasion",         "sanctions"),
    ("sanctioned entity",         "sanctions"),
    ("export control violation",  "sanctions"),
    # Terrorist financing
    ("terrorist financing",       "terrorist_financing"),
    ("terrorist organization",    "terrorist_financing"),
    # Human trafficking
    ("human trafficking",         "human_trafficking"),
    ("labor trafficking",         "human_trafficking"),
    # Consumer fraud
    ("romance scam",              "consumer_fraud"),
    ("lottery scam",              "consumer_fraud"),
    ("charity fraud",             "consumer_fraud"),
    ("investment scam",           "consumer_fraud"),
    ("scam",                      "consumer_fraud"),
    ("fraudster",                 "consumer_fraud"),
    ("gift card scam",            "consumer_fraud"),
    ("imposter scam",             "consumer_fraud"),
    # Identity fraud
    ("identity theft",            "identity_fraud"),
    ("stolen identity",           "identity_fraud"),
    ("synthetic identity",        "identity_fraud"),
    ("account takeover",          "identity_fraud"),
    ("identity document",         "identity_fraud"),
    ("credential theft",          "identity_fraud"),
    # Benefits fraud
    ("government benefits fraud", "benefits_fraud"),
    ("medicaid fraud",            "benefits_fraud"),
    ("pandemic relief fraud",     "benefits_fraud"),
    # Cybercrime
    ("ransomware",                "cybercrime"),
    ("phishing",                  "cybercrime"),
    ("malware",                   "cybercrime"),
    ("data breach",               "cybercrime"),
    ("credential stuffing",       "cybercrime"),
    ("social engineering",        "cybercrime"),
    # Crypto fraud
    ("virtual currency",          "crypto_fraud"),
    ("cryptocurrency",            "crypto_fraud"),
    ("crypto exchange",           "crypto_fraud"),
    ("pig butchering",            "crypto_fraud"),
    ("wallet",                    "crypto_fraud"),
    ("crypto wallet",             "crypto_fraud"),
    ("rug pull",                  "crypto_fraud"),
]

fraud_df = pd.DataFrame(fraud_dictionary, columns=["keyword", "fraud_family"])
print("Fraud dictionary keywords:", len(fraud_df))

fraud_dict_path = os.path.join(DICT_DIR, "fraud_dictionary.csv")
fraud_df.to_csv(fraud_dict_path, index=False)
print("Saved fraud dictionary:", fraud_dict_path)

# ── Fraud signals dictionary (mirrors FinCEN fraud_signals)
# ── Canonical fraud signals dictionary (v2) ──────────────────────────────────
# ── Canonical fraud signals dictionary (v2) ──────────────────────────────────
fraud_signals = [
    # Entity / organization
    ("shell company",             "entity_signal"),
    ("shell companies",           "entity_signal"),
    ("front company",             "entity_signal"),
    ("beneficial owner",          "ownership_signal"),
    ("ultimate beneficial owner", "ownership_signal"),
    ("ubo",                       "ownership_signal"),
    # Transaction patterns
    ("money mule",                "transaction_signal"),
    ("money mules",               "transaction_signal"),
    ("mule account",              "transaction_signal"),
    ("layering",                  "transaction_signal"),
    ("structuring",               "transaction_signal"),
    ("smurfing",                  "transaction_signal"),
    ("round dollar transactions", "transaction_signal"),
    ("rapid movement of funds",   "transaction_signal"),
    # Payment methods
    ("wire transfer",             "payment_signal"),
    ("international wire",        "payment_signal"),
    ("cash withdrawal",           "payment_signal"),
    ("atm withdrawal",            "payment_signal"),
    ("prepaid card",              "payment_signal"),
    ("gift card",                 "payment_signal"),
    ("zelle",                     "payment_signal"),
    ("venmo",                     "payment_signal"),
    ("cash app",                  "payment_signal"),
    ("paypal",                    "payment_signal"),
    ("peer-to-peer payment",      "payment_signal"),
    ("p2p payment",               "payment_signal"),
    # Crypto signals
    ("cryptocurrency",            "crypto_signal"),
    ("virtual currency",          "crypto_signal"),
    ("crypto exchange",           "crypto_signal"),
    ("crypto wallet",             "crypto_signal"),
    ("digital wallet",            "crypto_signal"),
    ("wallet address",            "crypto_signal"),
    ("bitcoin address",           "crypto_signal"),
    ("ethereum address",          "crypto_signal"),
    ("pig butchering",            "crypto_signal"),
    ("rug pull",                  "crypto_signal"),
    # Identity / account abuse
    ("identity theft",            "identity_signal"),
    ("stolen identity",           "identity_signal"),
    ("synthetic identity",        "identity_signal"),
    ("account takeover",          "identity_signal"),
    ("credential theft",          "identity_signal"),
    ("compromised account",       "identity_signal"),
    ("unauthorized access",       "identity_signal"),
    # Cyber / attack methods
    ("phishing",                  "cyber_signal"),
    ("smishing",                  "cyber_signal"),
    ("vishing",                   "cyber_signal"),
    ("malware",                   "cyber_signal"),
    ("ransomware",                "cyber_signal"),
    ("data breach",               "cyber_signal"),
    ("credential stuffing",       "cyber_signal"),
    ("social engineering",        "cyber_signal"),
    ("remote access",             "cyber_signal"),
    ("trojan",                    "cyber_signal"),
    # Document / claim fraud
    ("false claim",               "document_signal"),
    ("false claims",              "document_signal"),
    ("fraudulent invoice",        "document_signal"),
    ("fake documentation",        "document_signal"),
    ("forged document",           "document_signal"),
    ("fabricated records",        "document_signal"),
    # Scam behavior
    ("imposter",                  "scam_signal"),
    ("impersonation",             "scam_signal"),
    ("romance scam",              "scam_signal"),
    ("investment scam",           "scam_signal"),
    ("lottery scam",              "scam_signal"),
    ("gift card scam",            "scam_signal"),
    ("advance fee",               "scam_signal"),
    ("urgent payment request",    "scam_signal"),
    # Contact / channel signals
    ("email address",             "contact_signal"),
    ("phone number",              "contact_signal"),
    ("text message",              "contact_signal"),
    ("sms message",               "contact_signal"),
    ("telegram",                  "contact_signal"),
    ("whatsapp",                  "contact_signal"),
]

signals_df = pd.DataFrame(fraud_signals, columns=["signal_keyword", "signal_category"])
print("Fraud signal keywords:", len(signals_df))

# signals_df already built above
print("Total fraud signals:", len(signals_df))
signals_df.head()

signal_dict_path = os.path.join(DICT_DIR, "fraud_signals_dictionary.csv")
signals_df.to_csv(signal_dict_path, index=False)
print("Saved fraud signals dictionary:", signal_dict_path)

# ── Inline reference number removal (mirrors FinCEN)

def remove_inline_reference_numbers(text):
    if not text:
        return ""
    text = re.sub(r'(?<=[A-Za-z.,)])\d{1,2}\b', '', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

# ── Chunking function (identical to FinCEN)

def chunk_text(text, chunk_size=1200, overlap=200):
    if not text:
        return []
    chunks     = []
    start      = 0
    text_length = len(text)
    step       = chunk_size - overlap
    if step <= 0:
        raise ValueError("chunk_size must be greater than overlap")
    while start < text_length:
        end   = start + chunk_size
        chunk = text[start:end].strip()
        if len(chunk) > 150:   # keep only meaningful chunks
            chunks.append(chunk)
        start += step
    return chunks

# ── Build chunk rows
chunk_rows = []
for r in results:
    text = r.get("body_1", r.get("body", ""))
    text = remove_inline_reference_numbers(text)
    if not text or not text.strip():
        continue
    chunks = chunk_text(text, chunk_size=1200, overlap=200)
    for idx, chunk in enumerate(chunks):
        chunk_rows.append({
            "doc_id":       r["doc_id"],
            "title":        r["title"],
            "source":       r["source"],
            "date":         r["date"],
            "url":          r.get("url", ""),
            "chunk_id":     idx,
            "chunk_text":   chunk,
            "chunk_length": len(chunk),
        })

chunks_path = os.path.join(FULLTEXT_DIR, "fbi_chunks.jsonl")
with open(chunks_path, "w", encoding="utf-8") as f:
    for row in chunk_rows:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")
print("Saved:", chunks_path)
print("Total chunks:", len(chunk_rows))

# Chunk summary CSV (mirrors FinCEN)
chunks_df  = pd.DataFrame(chunk_rows)
summary_df = (
    chunks_df.groupby(["doc_id", "title", "source", "date"], as_index=False)
    .agg(
        total_chunks    =("chunk_id",     "count"),
        avg_chunk_length=("chunk_length", "mean"),
        min_chunk_length=("chunk_length", "min"),
        max_chunk_length=("chunk_length", "max"),
    )
)
summary_path = os.path.join(FULLTEXT_DIR, "fbi_chunks_summary.csv")
summary_df.to_csv(summary_path, index=False)
print("Saved:", summary_path)
summary_df.head()

# ── Tagging (mirrors FinCEN NB 6)

fraud_dict   = pd.read_csv(fraud_dict_path)
signal_dict  = pd.read_csv(signal_dict_path)

fraud_keywords = {
    row["keyword"].strip().lower(): row["fraud_family"].strip()
    for _, row in fraud_dict.iterrows()
}
signal_keywords = {
    row["signal_keyword"].strip().lower(): row["signal_category"].strip()
    for _, row in signal_dict.iterrows()
}

def detect_fraud_tags(text, fraud_keywords):
    text_lower = text.lower()
    return sorted({
        family
        for keyword, family in fraud_keywords.items()
        if keyword in text_lower
    })

def detect_fraud_signals(text, signal_keywords):
    text_lower = text.lower()
    return sorted({
        keyword
        for keyword in signal_keywords.keys()
        if keyword in text_lower
    })

tagged_rows = []
for c in chunk_rows:
    chunk_text_val  = c.get("chunk_text", "")
    fraud_tags_out  = detect_fraud_tags(chunk_text_val, fraud_keywords)
    fraud_sigs_out  = detect_fraud_signals(chunk_text_val, signal_keywords)
    tagged_rows.append({
        "doc_id":             c["doc_id"],
        "title":              c["title"],
        "source":             c["source"],
        "date":               c["date"],
        "url":                c.get("url", ""),
        "chunk_id":           c["chunk_id"],
        "fraud_tags":         fraud_tags_out,
        "fraud_signals":      fraud_sigs_out,
        "fraud_signal_count": len(fraud_sigs_out),
        "chunk_text":         chunk_text_val,
    })

print("Tagged rows:", len(tagged_rows))
tagged_rows[:2]

# ══════════════════════════════════════════════════════════════
# Save: pivot chunks to wide format (one row per doc)
# Each chunk's body and tags become numbered columns:
#   body_1, body_2, ... and fraud_tags_1, fraud_tags_2, ...
# Shared scalar fields (doc_id, title, source, date, url) appear once.
# ══════════════════════════════════════════════════════════════

from collections import defaultdict

doc_groups = defaultdict(list)
for row in tagged_rows:
    doc_groups[row["doc_id"]].append(row)

wide_rows = []

for doc_id, chunks in doc_groups.items():
    chunks = sorted(chunks, key=lambda x: x["chunk_id"])

    record = {
        "doc_id":     doc_id,
        "source":     chunks[0]["source"],
        "date":       chunks[0]["date"],
        "title":      chunks[0]["title"],
        "url":        chunks[0].get("url", ""),
        "num_chunks": len(chunks),
    }

    # Union of all fraud tags across chunks
    all_tags = []
    seen_tags = set()
    for c in chunks:
        for t in c["fraud_tags"]:
            if t not in seen_tags:
                all_tags.append(t)
                seen_tags.add(t)
    record["fraud_tags"] = all_tags if all_tags else ["other"]

    # Union of all signals across chunks
    all_signals = []
    seen_signals = set()
    for c in chunks:
        for s in c["fraud_signals"]:
            if s not in seen_signals:
                all_signals.append(s)
                seen_signals.add(s)
    record["fraud_signals"] = all_signals

    # Numbered columns — body_1..N and fraud_tags_1..N
    for i, chunk in enumerate(chunks, start=1):
        record[f"body_{i}"]       = chunk["chunk_text"]
        record[f"fraud_tags_{i}"] = chunk["fraud_tags"]

    wide_rows.append(record)

output_path = os.path.join(OUTPUT_FOLDER, "fbi_tagged_chunks.jsonl")
with open(output_path, "w", encoding="utf-8") as f:
    for row in wide_rows:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")

print(f"Saved: {output_path}")
print(f"Documents written: {len(wide_rows)}")

if wide_rows:
    print("Columns in first record:", list(wide_rows[0].keys()))
    max_chunks = max(r["num_chunks"] for r in wide_rows)
    print(f"Max chunks in any document: {max_chunks}")

# ── Tag distribution summary (mirrors FinCEN)
rows = []
for row in tagged_rows:
    for tag in row["fraud_tags"]:
        rows.append({
            "doc_id":             row["doc_id"],
            "date":     row["date"],
            "fraud_tag":          tag,
            "fraud_signal_count": row["fraud_signal_count"],
        })

summary_df = pd.DataFrame(rows)
if not summary_df.empty:
    tag_counts = summary_df["fraud_tag"].value_counts().reset_index()
    tag_counts.columns = ["fraud_tag", "count"]
    print(tag_counts.head(10))
else:
    print("No fraud tags found yet.")
