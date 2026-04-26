# FTC Pipeline
# Run: python FTC.py
# Output: outputs/ftc_master.jsonl

import os

# ── Path config ────────────────────────────────────────────────
# Driven by orchestrator env var when run via nbconvert.
# Running standalone in VS Code? Set FRAUD_BASE_DIR as an env var,
# or update the fallback path below — it is the ONLY place you need
# to change the path in this notebook.
BASE_DIR      = os.environ.get(
    'FRAUD_BASE_DIR',
    r'C:\Users\josephsingleton\fraud-dashboard'   # <-- update fallback for local use
)
OUTPUT_FOLDER = os.path.join(BASE_DIR, 'outputs')
os.makedirs(OUTPUT_FOLDER, exist_ok=True)
print('BASE_DIR:', BASE_DIR)
print('Ready.')

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

# Install

# Imports

import re, time, json, hashlib
import requests
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from tqdm import tqdm

# Session + Helpers

BASE = "https://consumer.ftc.gov"
START_URL = "https://consumer.ftc.gov/consumer-alerts"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (academic research; contact: your_email@uncc.edu)",
    "Accept-Language": "en-US,en;q=0.9",
}

session = requests.Session()
session.headers.update(HEADERS)

def normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()

def stable_doc_id(url: str) -> str:
    # Stable ID across runs (NOT Python hash())
    return hashlib.sha1(url.encode("utf-8")).hexdigest()[:14]  # short stable id

# Scrape listing first 10 pages (collect first 10 pages alert URLs)

def parse_listing_page(page: int):
    url = START_URL if page == 0 else f"{START_URL}?page={page}"
    r = session.get(url, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")
    main = soup.find("main") or soup

    items = []
    for a in main.select("a[href*='/consumer-alerts/']"):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        full_url = urljoin(BASE, href)

        # Ignore the listing page itself
        if full_url.rstrip("/") == START_URL.rstrip("/"):
            continue

        # Keep only real alert paths
        if "/consumer-alerts/" not in full_url:
            continue

        title = normalize_space(a.get_text(" ", strip=True))
        items.append({"title_hint": title, "url": full_url, "source_page": url})

    # de-dupe by URL, preserve order
    dedup = {}
    for it in items:
        dedup[it["url"]] = it
    return list(dedup.values())

all_listing = []
seen = set()

MAX_PAGES = 10   # first 10 pages only

for page in range(MAX_PAGES):
    rows = parse_listing_page(page)
    new_rows = [r for r in rows if r["url"] not in seen]

    for r in new_rows:
        all_listing.append(r)
        seen.add(r["url"])

    print(f"Page {page}: +{len(new_rows)} (total={len(all_listing)})")
    time.sleep(0.8)

print("Total collected URLs:", len(all_listing))
all_listing[:3]

print("Total collected URLs:", len(all_listing))
print("First 5 titles:")
for i, x in enumerate(all_listing[:5], 1):
    print(i, x["title_hint"])

# Scrape each alert page (title, date, body, FTC topics if available)

def extract_topics(soup: BeautifulSoup):
    # FTC pages often expose topic links; we’ll capture any taxonomy-like tags we can find.
    topics = set()
    for a in soup.select("a[href*='/topics/'], a[href*='/scams/']"):
        txt = normalize_space(a.get_text(" ", strip=True))
        if txt and 2 <= len(txt) <= 60:
            topics.add(txt)
    return sorted(topics)

def extract_article(url: str):
    r = session.get(url, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")

    # Remove noisy blocks
    for bad in soup.select("script, style, nav, footer, header, aside"):
        bad.decompose()

    h1 = soup.select_one("h1")
    title = normalize_space(h1.get_text(" ", strip=True)) if h1 else ""

    t = soup.find("time")
    published = ""
    if t:
        published = normalize_space(t.get("datetime") or t.get_text(" ", strip=True))

    main = soup.find("main")
    body = normalize_space(main.get_text(" ", strip=True)) if main else ""

    ftc_topics = extract_topics(soup)

    return title, published, body, ftc_topics

# Build raw dataframe for ALL alerts

records = []

for item in tqdm(all_listing, desc="Scraping alerts"):
    try:
        title, published, body, ftc_topics = extract_article(item["url"])
        records.append({
            "doc_id": stable_doc_id(item["url"]),
            "title": title or item["title_hint"],
            "url": item["url"],
            "published_raw": published,
            "body_raw": body,
            "source": "FTC Consumer Advice - Consumer Alerts",
            "metadata": {"source_page": item["source_page"]},
            "ftc_topics": ftc_topics,  # FTC site taxonomy (separate from fraud tags)
        })
    except Exception as e:
        print("FAILED:", item["url"], "->", str(e))
    time.sleep(1.0)  # be polite

raw_df = pd.DataFrame(records)
print("Raw shape:", raw_df.shape)
raw_df.head(3)

# Clean once (dates + text cleanup + dedupe)

def clean_body(text: str) -> str:
    if not text:
        return ""
    text = normalize_space(text)

    # Remove common boilerplate tails
    text = re.sub(r"Comments closed\..*$", "", text, flags=re.IGNORECASE)
    text = re.sub(r"Return to top$", "", text, flags=re.IGNORECASE)
    text = re.sub(r"Topics\s+.*$", "", text, flags=re.IGNORECASE)
    text = re.sub(r"Return to top", "", text, flags=re.IGNORECASE)

    return normalize_space(text)

df = raw_df.copy()

# Parse datetime
df["published"] = pd.to_datetime(df["published_raw"], errors="coerce", utc=True)
df["date"] = df["published"].dt.date.astype(str)  # YYYY-MM-DD string

# Clean text
df["title"] = df["title"].astype(str).map(normalize_space)
df["body_1"] = df["body_raw"].astype(str).map(clean_body)

# Create short body
df["body_short"] = df["body_1"].str[:1000]

# Dedupe on url (and keep latest content)
df = df.drop_duplicates(subset=["url"]).reset_index(drop=True)

print("Cleaned shape:", df.shape)
df[["doc_id","date","title"]].head(3)

# Convert published date to clean datetime

df["published"] = pd.to_datetime(df["published"], errors="coerce", utc=True)

df["date"] = df["published"].dt.date
df["published_year"] = df["published"].dt.year
df["published_month"] = df["published"].dt.month

# Fraud signals extraction — uses fraud_signals_from_text from fraud_config

df["fraud_signals"] = df["body_1"].apply(fraud_signals_from_text)

# Quick check
df[["title"]].assign(
    n_domains=df["fraud_signals"].apply(lambda x: len(x["domains"])),
    n_emails=df["fraud_signals"].apply(lambda x: len(x["emails"])),
    n_phones=df["fraud_signals"].apply(lambda x: len(x["phones"])),
).head(5)

## FTC alerts usually do not publish scammer phone numbers, emails, or malicious domains.
## These are educational alerts, not raw intelligence feeds — low signal counts are normal.

# Fraud Tag Taxonomy — uses assign_fraud_tags from fraud_config

df["fraud_tags"] = df.apply(
    lambda r: assign_fraud_tags(r["title"], r["body_1"]), axis=1
)
df[["title", "fraud_tags", "ftc_topics"]].head(10)

# Check for Missing Values

df.isnull().sum()

# Check Shape + Columns

print("Shape:", df.shape)
print("\nColumns:")
print(df.columns.tolist())

# Final dataset contains:

final_df = df[
    ["doc_id", "date","published_year", "published_month","source", "fraud_tags", "fraud_signals", "title", "url", "body_1", "body_short"]
].copy()

final_df = final_df.reset_index(drop=True)
final_df.head(3)

# Check Shape + Columns

print("Shape:", final_df.shape)
print("\nColumns:")
print(final_df.columns.tolist())

# Look at Full Cleaned Text of One Article

print(final_df.loc[0, "body_1"])

# Check Body Length

final_df["body_length"] = df["body_1"].str.len()
final_df[["title", "body_length"]].head()

#If lengths are: 2000 → good (full article) but if lengths are: < 300 → something is wrong (scraping incomplete)

df["fraud_tags"].explode().value_counts()

# Quick Distribution Check

final_df["published_year"].value_counts()

# Export ftc_master.jsonl

jsonl_path = os.path.join(OUTPUT_FOLDER, 'ftc_master.jsonl')
final_df.to_json(jsonl_path, orient='records', lines=True, force_ascii=False)
print('Saved:', jsonl_path)

# Export ftc_master.csv

csv_path = os.path.join(OUTPUT_FOLDER, 'ftc_master.csv')

final_df_csv = final_df.copy()
final_df_csv['fraud_tags']    = final_df_csv['fraud_tags'].apply(json.dumps)
final_df_csv['fraud_signals'] = final_df_csv['fraud_signals'].apply(json.dumps)

final_df_csv.to_csv(csv_path, index=False, encoding='utf-8')
print('Saved:', csv_path)

# Files are saved to OUTPUT_FOLDER (set in the path config cell above).
# Open the OUTPUT_FOLDER in VS Code Explorer to access the exported files.
# csv_path and jsonl_path are printed by the cell above.
print("Outputs saved to:", OUTPUT_FOLDER)
print(" ", csv_path)
print(" ", jsonl_path)
