# BleepingComputer Pipeline
# Run: python BleepingComputer.py
# Output: outputs/bleepingcomputer_fraud_data.csv

import os
import re
import json
import time
import hashlib

import requests
import pandas as pd
from bs4 import BeautifulSoup
from tqdm import tqdm

# ── Path config ────────────────────────────────────────────────
# Driven by orchestrator env var when run via nbconvert.
# Running standalone in VS Code? Set FRAUD_BASE_DIR as an env var,
# or update the fallback path below — it is the ONLY place you need
# to change the path in this notebook.
BASE_DIR      = os.environ.get(
    'FRAUD_BASE_DIR',
    r'C:\\Users\josephsingleton\fraud-dashboard'   # <-- update fallback for local use
)
OUTPUT_FOLDER = os.path.join(BASE_DIR, 'outputs')
METADATA_DIR  = os.path.join(BASE_DIR, 'data', 'metadata')
os.makedirs(OUTPUT_FOLDER, exist_ok=True)
os.makedirs(METADATA_DIR,  exist_ok=True)
print('BASE_DIR:', BASE_DIR)
print('Ready.')

# ── Helpers shared across all cells ───────────────────────────
def stable_doc_id(url: str) -> str:
    return hashlib.sha1(url.encode("utf-8")).hexdigest()[:14]

def normalize_space(text: str) -> str:
    return re.sub(r"\\s+", " ", (text or "")).strip()

# ── Central fraud config — single source of truth ───────────────────────────
import sys, os

_base = os.environ.get('FRAUD_BASE_DIR', os.path.abspath('..'))
if _base not in sys.path:
    sys.path.insert(0, _base)

from fraud_config import (
    FRAUD_DICTIONARY,                  # list of (keyword, family) tuples
    FRAUD_TAG_RULES,                   # list of (family, regex) tuples
    FRAUD_SIGNALS,                     # list of (signal_keyword, category) tuples
    SEVERITY_RANK,                     # severity-ordered list for primary_tag tiebreaking
    assign_fraud_tags,                 # regex tagger  (title, body) -> list
    assign_fraud_tags_from_keywords,   # keyword tagger (text, kw_dict) -> list
    assign_primary_tag,                # severity tiebreaker (tags_list) -> str
    build_fraud_keywords,              # builds {kw: family} dict from FRAUD_DICTIONARY
    build_signal_keywords,             # builds {kw: category} dict from FRAUD_SIGNALS
    fraud_signals_from_text,           # extracts URLs/emails/phones/IPs/crypto
    FRAUD_FAMILIES,                    # ordered list of all family names
    FAMILY_LABELS,                     # {family: display_label} for dashboards
)

print('fraud_config loaded ✓')
print(f'  Fraud families : {len(FRAUD_FAMILIES)}')
print(f'  Tag rules      : {len(FRAUD_TAG_RULES)}')
print(f'  Dictionary kws : {len(FRAUD_DICTIONARY)}')
print(f'  Signal kws     : {len(FRAUD_SIGNALS)}')

# ══════════════════════════════════════════════════════════════
# CELL 1 — Collect article URLs: live pagination + Wayback CDX
#
# BleepingComputer has deeply paginated tag archives at:
#   /tag/<tag>/          (page 1)
#   /tag/<tag>/page/N/   (pages 2+)
# These go back years and are publicly accessible.
#
# We scrape multiple fraud-relevant tags and union-deduplicate the URLs.
# The Wayback CDX API supplements the live scrape for any articles that
# have been deleted or are no longer reachable on the live site.
#
# Output: bleepingcomputer_listings.csv
#   Columns: url, title_hint, date_hint, tag, data_source
# ══════════════════════════════════════════════════════════════

# ── Config ────────────────────────────────────────────────────────────────

# Tags to scrape — ordered by expected fraud relevance.
# Add more from https://www.bleepingcomputer.com/tag/  if needed.
TAGS_TO_SCRAPE = [
    "fraud",
    "scam",
    "ransomware",
    "phishing",
    "data-breach",
    "identity-theft",
    "cryptocurrency",
]

# How many paginated archive pages to pull per tag.
# BleepingComputer's /tag/ransomware/ has 100+ pages going back to ~2012.
# Set to None to scrape ALL available pages (slowest but maximum depth).
MAX_PAGES_PER_TAG = 50      # ~50 pages × ~15 articles = ~750 articles per tag

# Wayback CDX config
CDX_FROM_DATE  = "20200101"  # YYYYMMDD — change to go further back
CDX_LIMIT      = 3000        # max CDX rows per tag pattern query

SLEEP_PAGE   = 2.0   # seconds between paginated page requests (polite)
SLEEP_CDX    = 1.5   # seconds between CDX API calls

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept":          "text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer":         "https://www.bleepingcomputer.com/",
}

CDX_HEADERS = {
    "User-Agent": "python-requests/2.31 (academic research; contact: your_email@uncc.edu)",
}

CDX_BASE = "http://web.archive.org/cdx/search/cdx"

# ── Helpers ───────────────────────────────────────────────────────────────

def get_max_page(tag: str) -> int:
    """
    Scrape the tag index page and find the highest pagination number.
    Returns 1 if no pagination is found (only one page of results).
    """
    url  = f"https://www.bleepingcomputer.com/tag/{tag}/"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=20)
        resp.raise_for_status()
    except Exception as e:
        print(f"  WARN: could not fetch tag page {url} → {e}")
        return 1

    soup     = BeautifulSoup(resp.text, "html.parser")
    # BleepingComputer pagination uses <a> tags with numeric text inside
    # a container with class "bc_latest_news_nav" or generic <div class="pagination">
    page_nums = []
    for a in soup.select("div.pagination a, ul.bc_latest_news_nav a, a[href*='/page/']"):
        try:
            page_nums.append(int(a.get_text(strip=True)))
        except ValueError:
            pass
    return max(page_nums) if page_nums else 1

def scrape_tag_page(tag: str, page: int) -> list:
    """
    Scrape one tag archive page and return list of
    {url, title_hint, date_hint, tag, data_source='live'} dicts.
    """
    if page == 1:
        page_url = f"https://www.bleepingcomputer.com/tag/{tag}/"
    else:
        page_url = f"https://www.bleepingcomputer.com/tag/{tag}/page/{page}/"

    try:
        resp = requests.get(page_url, headers=HEADERS, timeout=20)
        resp.raise_for_status()
    except Exception as e:
        print(f"  WARN: page fetch failed  {page_url} → {e}")
        return []

    soup    = BeautifulSoup(resp.text, "html.parser")
    results = []

    # BleepingComputer article listings use <h4> tags wrapping <a> links,
    # same selector the original notebook already uses.
    for h4 in soup.find_all("h4"):
        a = h4.find("a")
        if not a or not a.get("href"):
            continue
        href = a["href"]
        if not href.startswith("http"):
            href = "https://www.bleepingcomputer.com" + href

        # Try to grab the associated date from a nearby sibling element
        # BC listing items often have a <li class="cz-news-date"> nearby
        date_hint = ""
        parent = h4.find_parent()
        if parent:
            date_tag = parent.find("li", class_="cz-news-date")
            if date_tag:
                date_hint = normalize_space(date_tag.get_text(" ", strip=True))

        results.append({
            "url":        href,
            "title_hint": normalize_space(a.get_text(" ", strip=True)),
            "date_hint":  date_hint,
            "tag":        tag,
            "data_source": "live",
            "wayback_ts": "",
        })

    return results

def fetch_cdx_bc_tag(tag: str,
                     from_date: str = CDX_FROM_DATE,
                     limit:     int = CDX_LIMIT) -> list:
    """
    Query Wayback CDX for archived BleepingComputer /tag/<tag>/ listing pages.
    We fetch the archived INDEX pages (not individual articles) to extract URLs.
    Then we also directly query for individual article URLs under /news/security/.
    Returns list of {url, title_hint, date_hint, tag, data_source='cdx', wayback_ts} dicts.
    """
    # Query for individual article URLs archived under the BleepingComputer security section
    patterns = [
        f"bleepingcomputer.com/news/security/*{tag.replace('-', '*')}*",
        f"bleepingcomputer.com/news/security/*",
    ]
    results = []
    seen    = set()

    for pattern in patterns[:1]:   # primary pattern only — secondary is too broad
        params = {
            "url":      pattern,
            "output":   "json",
            "fl":       "timestamp,original,statuscode",
            "collapse": "urlkey",
            "filter":   "statuscode:200",
            "limit":    str(limit),
        }
        if from_date:
            params["from"] = from_date

        try:
            resp = requests.get(CDX_BASE, params=params, headers=CDX_HEADERS, timeout=60)
            resp.raise_for_status()
            rows = resp.json()
        except Exception as e:
            print(f"  WARN: CDX fetch failed for {pattern} → {e}")
            continue

        if not rows or len(rows) <= 1:
            continue

        _header, *data_rows = rows
        for row in data_rows:
            ts, orig = row[0], row[1]
            orig = orig.rstrip("/")
            if not orig.startswith("http"):
                orig = "https://" + orig
            # Only keep article URLs (not tag/category/page index URLs)
            if "/news/" not in orig:
                continue
            if orig in seen:
                continue
            seen.add(orig)
            pub_raw = f"{ts[:4]}-{ts[4:6]}-{ts[6:8]}" if len(ts) >= 8 else ""
            results.append({
                "url":        orig,
                "title_hint": "",
                "date_hint":  pub_raw,
                "tag":        tag,
                "data_source": "cdx",
                "wayback_ts": ts,
            })

    return results

# ── STEP 1: Live paginated archive scraping ───────────────────────────────

print("=" * 60)
print("STEP 1: Live paginated archive scraping")
print("=" * 60)

live_listings = []
all_seen_urls = set()

for tag in TAGS_TO_SCRAPE:
    print(f"\n  Tag: {tag}")

    # Discover how many pages exist
    max_available = get_max_page(tag)
    limit         = max_available if MAX_PAGES_PER_TAG is None else min(MAX_PAGES_PER_TAG, max_available)
    print(f"    Pages available: {max_available}  →  scraping up to {limit}")

    tag_count = 0
    for page in tqdm(range(1, limit + 1), desc=f"    {tag}", leave=False):
        items    = scrape_tag_page(tag, page)
        new_items = [it for it in items if it["url"] not in all_seen_urls]
        for it in new_items:
            live_listings.append(it)
            all_seen_urls.add(it["url"])
        tag_count += len(new_items)
        if not items:
            print(f"    Empty page {page} — stopping tag early")
            break
        time.sleep(SLEEP_PAGE)

    print(f"    → {tag_count} unique articles from live scrape")

print(f"\nLive total: {len(live_listings)} unique article URLs")

# ── STEP 2: Wayback CDX supplement ───────────────────────────────────────

print("\n" + "=" * 60)
print(f"STEP 2: Wayback CDX supplement  (from={CDX_FROM_DATE})")
print("=" * 60)

cdx_listings = []

for tag in tqdm(TAGS_TO_SCRAPE, desc="CDX queries"):
    rows      = fetch_cdx_bc_tag(tag)
    new_rows  = [r for r in rows if r["url"] not in all_seen_urls]
    for r in new_rows:
        cdx_listings.append(r)
        all_seen_urls.add(r["url"])
    print(f"  {tag}: {len(rows)} CDX snapshots, {len(new_rows)} new")
    time.sleep(SLEEP_CDX)

print(f"\nCDX supplement: {len(cdx_listings)} additional URLs")

# ── Combine + save ─────────────────────────────────────────────────────────

all_listings = live_listings + cdx_listings
print(f"\nCombined total: {len(all_listings)} unique article URLs")
print(f"  Live:  {len(live_listings)}")
print(f"  CDX:   {len(cdx_listings)}")

listings_path = os.path.join(METADATA_DIR, "bleepingcomputer_listings.csv")
pd.DataFrame(all_listings).to_csv(listings_path, index=False)
print("Saved:", listings_path)

# ══════════════════════════════════════════════════════════════
# CELL 2 — Fetch and parse article content
#
# Routes each listing by data_source:
#   "live"  → fetch directly from bleepingcomputer.com
#   "cdx"   → fetch archived HTML from Wayback Machine
#
# CHECKPOINT / RESUME:
#   Reads existing doc_ids from bleepingcomputer_raw.jsonl on startup.
#   Safe to interrupt and resume — no duplicate records written.
#
# Output: bleepingcomputer_raw.jsonl
# ══════════════════════════════════════════════════════════════

SLEEP_LIVE    = 2.5   # seconds between live article fetches (polite)
SLEEP_WAYBACK = 1.5   # seconds between Wayback fetches

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept":          "text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer":         "https://www.bleepingcomputer.com/",
}
WAYBACK_HEADERS = {
    "User-Agent": "python-requests/2.31 (academic research; contact: your_email@uncc.edu)",
    "Accept":     "text/html,application/xhtml+xml,*/*",
}

# ── Helpers ───────────────────────────────────────────────────────────────

def parse_bc_date(raw: str) -> str:
    """
    Convert BleepingComputer date strings to YYYY-MM-DD.
    Handles formats like: "April 10, 2024",  "2024-04-10",
    "Apr 10, 2024 03:15 PM".
    Returns empty string on failure.
    """
    if not raw:
        return ""
    try:
        from dateutil import parser as du_parser
        dt = du_parser.parse(raw, fuzzy=True)
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return ""

def parse_bc_article_html(html: str) -> dict:
    """
    Extract title, date, and body text from a BleepingComputer article page.

    BC article structure:
      - Title:   <h1> inside <div class="article_section"> or first <h1>
      - Date:    <li class="cz-news-date"> or <time> tag
      - Body:    <div class="articleBody"> — same selector original notebook used
    """
    soup = BeautifulSoup(html, "html.parser")

    # Remove Wayback toolbar if present (archived pages only)
    for tag in soup.select("#wm-ipp-base, #wm-ipp, #donato"):
        tag.decompose()

    # Title
    title = ""
    for sel in ["h1.article_section", "h1"]:
        t = soup.select_one(sel)
        if t:
            title = normalize_space(t.get_text(" ", strip=True))
            if title:
                break

    # Date
    date_raw = ""
    date_tag = soup.find("li", class_="cz-news-date")
    if not date_tag:
        date_tag = soup.find("time")
    if date_tag:
        # <time> tags often have datetime attribute
        date_raw = (
            date_tag.get("datetime", "")
            or normalize_space(date_tag.get_text(" ", strip=True))
        )

    # Body
    body_text = ""
    body_div = soup.select_one("div.articleBody")
    if body_div:
        # Remove embedded ads / related article noise
        for noise in body_div.select("div.cz-related-article-wrapp, script, .ad"):
            noise.decompose()
        body_text = normalize_space(body_div.get_text(" ", strip=True))

    return {
        "title":    title,
        "date_raw": date_raw,
        "body":     body_text,
    }

def fetch_article_live(url: str) -> dict | None:
    """Fetch and parse a live BleepingComputer article."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=20)
        resp.raise_for_status()
        return parse_bc_article_html(resp.text)
    except Exception as e:
        print(f"  WARN: live fetch failed {url[:70]} → {e}")
        return None

def fetch_article_wayback(url: str, wayback_ts: str) -> dict | None:
    """Fetch and parse a BleepingComputer article via Wayback archive."""
    archived_url = f"https://web.archive.org/web/{wayback_ts}/{url}"
    try:
        resp = requests.get(archived_url, headers=WAYBACK_HEADERS, timeout=30, allow_redirects=True)
        if resp.status_code == 404:
            archived_url = f"https://web.archive.org/web/{url}"
            resp = requests.get(archived_url, headers=WAYBACK_HEADERS, timeout=30)
        if resp.status_code != 200:
            return None
        return parse_bc_article_html(resp.text)
    except Exception as e:
        print(f"  WARN: Wayback fetch failed {url[:70]} → {e}")
        return None

# ── Load listings ─────────────────────────────────────────────────────────

listings_path = os.path.join(METADATA_DIR, "bleepingcomputer_listings.csv")
listings_df   = pd.read_csv(listings_path)
print(f"Loaded {len(listings_df)} listings")
print(f"  data_source breakdown:\n{listings_df['data_source'].value_counts().to_string()}")

# ── Checkpoint: load already-fetched doc_ids ──────────────────────────────

raw_jsonl_path   = os.path.join(OUTPUT_FOLDER, "bleepingcomputer_raw.jsonl")
existing_doc_ids = set()

if os.path.exists(raw_jsonl_path):
    with open(raw_jsonl_path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                existing_doc_ids.add(json.loads(line).get("doc_id", ""))
            except Exception:
                pass
    print(f"Checkpoint: {len(existing_doc_ids)} records already fetched — will skip")
else:
    print("No existing output — starting fresh")

# ── Fetch articles ────────────────────────────────────────────────────────

print("\n--- Fetching article content ---")

n_live_ok   = 0
n_cdx_ok    = 0
n_failed    = 0
n_skipped   = 0
all_records = []

out_f = open(raw_jsonl_path, "a", encoding="utf-8")

for _, row in tqdm(listings_df.iterrows(), total=len(listings_df), desc="Articles"):
    url         = row["url"]
    doc_id      = stable_doc_id(url)
    data_source = row.get("data_source", "live")
    wayback_ts  = str(row.get("wayback_ts", ""))

    if doc_id in existing_doc_ids:
        n_skipped += 1
        continue

    # Route by data_source
    if data_source == "live":
        parsed = fetch_article_live(url)
        sleep_time = SLEEP_LIVE
    else:
        parsed = fetch_article_wayback(url, wayback_ts)
        sleep_time = SLEEP_WAYBACK

    if not parsed or not parsed.get("body"):
        n_failed += 1
        time.sleep(sleep_time)
        continue

    date_str = parse_bc_date(parsed["date_raw"] or row.get("date_hint", ""))

    record = {
        "doc_id":      doc_id,
        "title":       parsed["title"] or row.get("title_hint", ""),
        "url":         url,
        "date":        date_str,
        "date_raw":    parsed["date_raw"] or row.get("date_hint", ""),
        "source":      "BleepingComputer",
        "tag":         row.get("tag", ""),
        "data_source": data_source,
        "body_1":      parsed["body"],
    }

    all_records.append(record)
    existing_doc_ids.add(doc_id)
    out_f.write(json.dumps(record, ensure_ascii=False) + "\n")

    if data_source == "live":
        n_live_ok += 1
    else:
        n_cdx_ok += 1

    time.sleep(sleep_time)

out_f.close()

print(f"\n{'='*50}")
print(f"Records written this run:")
print(f"  Live fetched:         {n_live_ok}")
print(f"  Wayback fetched:      {n_cdx_ok}")
print(f"  Failed / no body:     {n_failed}")
print(f"  Skipped (checkpoint): {n_skipped}")
print(f"  New records:          {len(all_records)}")

# Full file stats
full_df = pd.read_json(raw_jsonl_path, lines=True)
print(f"\nFull file: {full_df.shape[0]} records")
if "date" in full_df.columns and full_df["date"].notna().any():
    valid_dates = full_df["date"].dropna().astype(str).str[:4]
    print(f"Year range: {valid_dates.min()} – {valid_dates.max()}")

# ══════════════════════════════════════════════════════════════
# CELL 3 — Clean, tag, and export master dataset
#
# Applies canonical fraud_dictionary v2 tags + fraud_signals dict v2.
# Outputs bleepingcomputer_fraud_data.csv (canonical schema).
# ══════════════════════════════════════════════════════════════

from urllib.parse import urlparse

# ── Load raw ─────────────────────────────────────────────────────────────

raw_jsonl_path = os.path.join(OUTPUT_FOLDER, "bleepingcomputer_raw.jsonl")
raw_df = pd.read_json(raw_jsonl_path, lines=True)
print(f"Loaded {raw_df.shape[0]} records from bleepingcomputer_raw.jsonl")

if raw_df.empty:
    raise ValueError(
        "bleepingcomputer_raw.jsonl is empty — Cell 2 produced no records. "
        "Check for network/scraping errors above and re-run Cell 2."
    )

# ── Dedupe on URL ─────────────────────────────────────────────────────────
raw_df = raw_df.drop_duplicates(subset=["url"]).reset_index(drop=True)
print(f"After dedup: {raw_df.shape[0]} records")

# ── Text cleaning ─────────────────────────────────────────────────────────

def clean_body(text: str) -> str:
    """Remove BC boilerplate tails and normalize whitespace."""
    if not text:
        return ""
    text = normalize_space(text)
    text = re.sub(r"Related Articles.*$",               "", text, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r"Subscribe to our.*$",               "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bAdvertisement\b.*$",              "", text, flags=re.IGNORECASE)
    text = re.sub(r"You can follow.*BleepingComputer.*$","", text, flags=re.IGNORECASE)
    return normalize_space(text)

raw_df["body_1"] = raw_df["body_1"].astype(str).map(clean_body)

# ── Fraud tag taxonomy — uses assign_fraud_tags from fraud_config ────────────

raw_df["title"]  = raw_df["title"].fillna("").astype(str)
raw_df["body_1"] = raw_df["body_1"].fillna("").astype(str)
raw_df["fraud_tags"] = raw_df.apply(
    lambda r: assign_fraud_tags(r["title"], r["body_1"]), axis=1
)

# ── Fraud signals extraction — uses fraud_signals_from_text from fraud_config ──

raw_df["fraud_signals"] = raw_df["body_1"].apply(fraud_signals_from_text)

# ── Date enrichment ───────────────────────────────────────────────────────

raw_df["published"] = pd.to_datetime(raw_df["date"], errors="coerce", utc=True)
raw_df["published_year"]  = raw_df["published"].dt.year
raw_df["published_month"] = raw_df["published"].dt.month

# ── Final schema — canonical across all pipelines ─────────────────────────

EXPECTED_COLUMNS = [
    "doc_id", "date", "published_year", "published_month",
    "source", "fraud_tags", "fraud_signals",
    "title", "body_1", "url"
]

final_df = raw_df.reindex(columns=EXPECTED_COLUMNS).copy()
final_df["source"] = "BleepingComputer"
final_df["body_length"] = raw_df["body_1"].astype(str).str.len()

# ── Quality check ──────────────────────────────────────────────────────────

print("\nShape:", final_df.shape)
print("\nMissing values:")
print(final_df.isnull().sum())
print("\nFraud tag distribution:")
print(final_df["fraud_tags"].explode().value_counts().head(12).to_string())

if final_df["published_year"].notna().any():
    yr_min = int(final_df["published_year"].min())
    yr_max = int(final_df["published_year"].max())
    print(f"\nYear range: {yr_min} – {yr_max}")

# ── Export ────────────────────────────────────────────────────────────────

# Serialise list columns for CSV
csv_df = final_df.copy()
csv_df["fraud_tags"]    = csv_df["fraud_tags"].apply(json.dumps)
csv_df["fraud_signals"] = csv_df["fraud_signals"].apply(json.dumps)

csv_path = os.path.join(OUTPUT_FOLDER, "bleepingcomputer_fraud_data.csv")
csv_df.to_csv(csv_path, index=False)
print("\nCSV saved:", csv_path)

# JSONL (keep list columns as native lists)
jsonl_path = os.path.join(OUTPUT_FOLDER, "bleepingcomputer_fraud_data.jsonl")
final_df.to_json(jsonl_path, orient="records", lines=True, force_ascii=False)
print("JSONL saved:", jsonl_path)

print(f"\nTotal records exported: {len(final_df)}")
