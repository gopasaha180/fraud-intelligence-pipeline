# IC3 Pipeline
# Run: python IC3.py
# Output: outputs/ic3_tagged_chunks.jsonl

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
METADATA_DIR  = os.path.join(BASE_DIR, 'data', 'metadata')
FULLTEXT_DIR  = os.path.join(BASE_DIR, 'data', 'fulltext')
PDF_DIR       = os.path.join(BASE_DIR, 'data', 'pdfs')
DICT_DIR      = os.path.join(BASE_DIR, 'data', 'dictionaries')
OUTPUT_FOLDER = os.path.join(BASE_DIR, 'outputs')
for folder in [METADATA_DIR, FULLTEXT_DIR, PDF_DIR, DICT_DIR, OUTPUT_FOLDER]:
    os.makedirs(folder, exist_ok=True)
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

# ============================================================
# IC3 Fraud Intelligence Platform --- Colab Pipeline
# ============================================================
# Mirrors the FBI / FinCEN pipeline structure exactly.
# Notebooks in this pipeline:
#   01  Scrape IC3 annual report PDFs + PSA listing pages  →  ic3_listings.csv
#   02  Extract text from PDFs + PSA detail pages          →  ic3_articles_raw.jsonl
#   03  Clean + enrich                                     →  ic3_master.jsonl / ic3_master.csv
#   04  Fraud tagging                                      →  ic3_tagged_chunks.jsonl
# ============================================================
#
# SOURCE NOTES:
#   IC3 (Internet Crime Complaint Center) publishes two primary data sources:
#     1. Annual Internet Crime Reports (PDFs) — ic3.gov/Media/PDF/AnnualReport/
#        Comprehensive statistics, fraud type breakdowns, loss figures by year.
#     2. Public Service Announcements (PSAs) — ic3.gov/Media/Y20XX
#        Press-release style alerts on specific fraud schemes (no Cloudflare block).
#   Unlike fbi.gov HTML pages, ic3.gov PSA listing pages are accessible from
#   server-based scrapers. Annual report PDFs are direct downloads.

# ══════════════════════════════════════════════════════════════
# ── NOTEBOOK 1 ── Collect ALL PSA URLs + Annual Report PDF links
# Goal:
#   - scrape IC3 PSA listing pages (by year) for alert URLs + metadata
#   - collect direct PDF links for annual crime reports
#   - save ic3_listings.csv
# ══════════════════════════════════════════════════════════════

# !pip -q install beautifulsoup4 lxml pandas requests tqdm

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

# ── Paths

os.makedirs(METADATA_DIR, exist_ok=True)
os.makedirs(FULLTEXT_DIR, exist_ok=True)
os.makedirs(PDF_DIR,      exist_ok=True)

# ── IC3 PSA listing pages (one per year; ic3.gov is not behind Cloudflare)
IC3_BASE_URL = "https://www.ic3.gov"

# PSA listing pages by year — add new years here as IC3 publishes them
PSA_LISTING_URLS = [
    "https://www.ic3.gov/Media/Y2024",
    "https://www.ic3.gov/Media/Y2023",
    "https://www.ic3.gov/Media/Y2022",
    "https://www.ic3.gov/Media/Y2021",
    "https://www.ic3.gov/Media/Y2020",
    "https://www.ic3.gov/Media/Y2019",
    "https://www.ic3.gov/Media/Y2018",
]

# Annual Internet Crime Report PDFs (direct download links)
# These contain aggregate stats, top crime types, loss figures, state breakdowns.
ANNUAL_REPORT_PDFS = {
    2023: "https://www.ic3.gov/Media/PDF/AnnualReport/2023_IC3Report.pdf",
    2022: "https://www.ic3.gov/Media/PDF/AnnualReport/2022_IC3Report.pdf",
    2021: "https://www.ic3.gov/Media/PDF/AnnualReport/2021_IC3Report.pdf",
    2020: "https://www.ic3.gov/Media/PDF/AnnualReport/2020_IC3Report.pdf",
    2019: "https://www.ic3.gov/Media/PDF/AnnualReport/2019_IC3Report.pdf",
    2018: "https://www.ic3.gov/Media/PDF/AnnualReport/2018_IC3Report.pdf",
}

SCRAPE_HEADERS = {
    "User-Agent":   "python-requests/2.31 (academic research; contact: your_email@uncc.edu)",
    "Accept":       "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

SLEEP_BETWEEN_PAGES    = 1.5   # seconds between listing page requests
SLEEP_BETWEEN_ARTICLES = 1.5   # seconds between PSA detail page requests (NB2)

# ── Helpers

def normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()

def stable_doc_id(url: str) -> str:
    return hashlib.sha1(url.encode("utf-8")).hexdigest()[:14]

def fetch_html(url: str) -> str:
    """Fetch an HTML page. Raises clearly on failure."""
    resp = requests.get(url, headers=SCRAPE_HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.text

def parse_psa_listing_page(listing_url: str) -> list:
    """
    Parse one IC3 PSA year-listing page → list of
    {title_hint, url, published_raw, source_year}.
    IC3 lists PSAs as <a> tags inside article/li containers.
    """
    try:
        html = fetch_html(listing_url)
    except Exception as e:
        print(f"  SKIP listing page (fetch failed): {listing_url} → {e}")
        return []

    soup  = BeautifulSoup(html, "lxml")
    items = []

    # IC3 PSA listing structure: each PSA is an <li> or <div> with a link and date
    # Try multiple selectors to be robust across IC3 site updates
    candidates = (
        soup.select("ul.media-list li")       # primary structure
        or soup.select("div.media-body")       # alternate layout
        or soup.select("article")              # fallback
    )

    # If structured selectors fail, fall back to all relevant anchor tags
    if not candidates:
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "/PSA/" in href or "/News/" in href:
                url = href if href.startswith("http") else urljoin(IC3_BASE_URL, href)
                title = normalize_space(a.get_text(" ", strip=True))
                if title:
                    items.append({
                        "title_hint":    title,
                        "url":           url,
                        "published_raw": "",
                        "source_year":   listing_url.split("Y")[-1],
                        "source_type":   "PSA",
                    })
        return items

    for candidate in candidates:
        a_tag = candidate.find("a", href=True)
        if not a_tag:
            continue

        href  = a_tag["href"]
        url   = href if href.startswith("http") else urljoin(IC3_BASE_URL, href)
        if not url.startswith("http"):
            continue

        title = normalize_space(a_tag.get_text(" ", strip=True))
        if not title:
            continue

        # Look for a date sibling — IC3 often puts dates in <span> or <small>
        date_tag  = candidate.find("span", class_=re.compile(r"date|time", re.I))
        date_tag  = date_tag or candidate.find("small")
        published = normalize_space(date_tag.get_text(" ", strip=True)) if date_tag else ""

        items.append({
            "title_hint":    title,
            "url":           url,
            "published_raw": published,
            "source_year":   listing_url.split("Y")[-1],
            "source_type":   "PSA",
        })

    return items

# ── Pull all PSA listing pages

all_listing = []
seen        = set()

for listing_url in tqdm(PSA_LISTING_URLS, desc="Scraping IC3 PSA listing pages"):
    items     = parse_psa_listing_page(listing_url)
    new_items = [it for it in items if it["url"] not in seen]
    for it in new_items:
        all_listing.append(it)
        seen.add(it["url"])
    print(f"  {listing_url.split('Y')[-1]}: +{len(new_items)} new (total={len(all_listing)})")
    time.sleep(SLEEP_BETWEEN_PAGES)

# ── Add Annual Report PDF entries as listing rows
for year, pdf_url in ANNUAL_REPORT_PDFS.items():
    if pdf_url not in seen:
        all_listing.append({
            "title_hint":    f"IC3 Annual Internet Crime Report {year}",
            "url":           pdf_url,
            "published_raw": f"January 1, {year + 1}",   # reports publish in Q1 of following year
            "source_year":   str(year),
            "source_type":   "AnnualReport_PDF",
        })
        seen.add(pdf_url)

print(f"\nTotal collected entries: {len(all_listing)}")
print(f"  PSAs:           {sum(1 for x in all_listing if x['source_type'] == 'PSA')}")
print(f"  Annual Reports: {sum(1 for x in all_listing if x['source_type'] == 'AnnualReport_PDF')}")

print("\nFirst 5 titles:")
for i, x in enumerate(all_listing[:5], 1):
    print(i, x["title_hint"])

# ── Save listings
listings_path = os.path.join(METADATA_DIR, "ic3_listings.csv")
pd.DataFrame(all_listing).to_csv(listings_path, index=False)
print("Saved:", listings_path)

# ══════════════════════════════════════════════════════════════
# ── NOTEBOOK 2 ── Scrape PSA detail pages + extract PDF text
# Goal:
#   - reload ic3_listings.csv
#   - for PSA entries: fetch article detail page, extract title/date/body
#   - for AnnualReport_PDF entries: download PDF, extract text via pdfplumber
#   - save ic3_articles_raw.jsonl
# ══════════════════════════════════════════════════════════════

# !pip -q install beautifulsoup4 lxml pandas requests tqdm pdfplumber

import os, re, time, json, hashlib
import requests
import pandas as pd
import pdfplumber
from bs4 import BeautifulSoup
from tqdm import tqdm

os.makedirs(METADATA_DIR, exist_ok=True)
os.makedirs(FULLTEXT_DIR, exist_ok=True)
os.makedirs(PDF_DIR,      exist_ok=True)

SCRAPE_HEADERS = {
    "User-Agent":   "python-requests/2.31 (academic research; contact: your_email@uncc.edu)",
    "Accept":       "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}
SLEEP_BETWEEN_ARTICLES = 1.5

def normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()

def stable_doc_id(url: str) -> str:
    return hashlib.sha1(url.encode("utf-8")).hexdigest()[:14]

def strip_html_tags(text: str) -> str:
    return BeautifulSoup(text or "", "lxml").get_text(" ", strip=True)

def fetch_html(url: str) -> str:
    resp = requests.get(url, headers=SCRAPE_HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.text

def extract_psa_article(url: str) -> dict:
    """
    Fetch one IC3 PSA detail page and extract:
      title, published_raw, body_raw, ic3_topics (any category/tag labels).
    IC3 PSA pages use a clean HTML structure with minimal JS.
    """
    try:
        html = fetch_html(url)
    except Exception as e:
        return {"error": str(e), "body_raw": "", "published_raw": "", "ic3_topics": []}

    soup = BeautifulSoup(html, "lxml")

    # ── Title: <h1> is most reliable on IC3 pages
    h1 = soup.find("h1")
    title = normalize_space(h1.get_text(" ", strip=True)) if h1 else ""

    # ── Published date: look for <time>, <span class="date">, or meta tag
    time_tag  = soup.find("time")
    date_span = soup.find("span", class_=re.compile(r"date|published", re.I))
    meta_date = soup.find("meta", {"name": re.compile(r"date", re.I)})
    published = ""
    if time_tag:
        published = normalize_space(time_tag.get("datetime", "") or time_tag.get_text())
    elif date_span:
        published = normalize_space(date_span.get_text(" ", strip=True))
    elif meta_date:
        published = normalize_space(meta_date.get("content", ""))

    # ── Body: IC3 PSA content lives in <div class="field-body"> or <article>
    body_div = (
        soup.find("div", class_=re.compile(r"field.?body|article.?body|entry.?content", re.I))
        or soup.find("article")
        or soup.find("main")
    )
    body_raw = ""
    if body_div:
        # Remove nav / header / footer cruft
        for tag in body_div.find_all(["nav", "header", "footer", "script", "style"]):
            tag.decompose()
        body_raw = normalize_space(body_div.get_text(" ", strip=True))

    # ── IC3 topic/category tags (breadcrumbs or aside labels)
    topic_tags = soup.find_all("a", class_=re.compile(r"tag|topic|category|label", re.I))
    ic3_topics = sorted({
        normalize_space(t.get_text(" ", strip=True))
        for t in topic_tags
        if t.get_text(strip=True)
    })

    return {
        "title":         title,
        "published_raw": published,
        "body_raw":      body_raw,
        "ic3_topics":    ic3_topics,
        "error":         "",
    }

def download_and_extract_pdf(url: str, pdf_dir: str) -> dict:
    """
    Download an IC3 Annual Report PDF and extract text using pdfplumber.
    Returns a dict with body_raw (full extracted text) and metadata.
    """
    filename = url.split("/")[-1]
    local_path = os.path.join(pdf_dir, filename)

    # Download if not already cached
    if not os.path.exists(local_path):
        try:
            resp = requests.get(url, headers=SCRAPE_HEADERS, timeout=60, stream=True)
            resp.raise_for_status()
            with open(local_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(f"  Downloaded: {filename}")
        except Exception as e:
            return {"body_raw": "", "published_raw": "", "ic3_topics": [], "error": str(e)}
    else:
        print(f"  Using cached: {filename}")

    # Extract text via pdfplumber
    try:
        pages_text = []
        with pdfplumber.open(local_path) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    pages_text.append(normalize_space(text))
        full_text = " ".join(pages_text)
        return {
            "body_raw":      full_text,
            "published_raw": "",           # date parsed in NB3 from title/context
            "ic3_topics":    ["AnnualReport"],
            "error":         "",
            "local_pdf":     local_path,
        }
    except Exception as e:
        return {"body_raw": "", "published_raw": "", "ic3_topics": [], "error": str(e)}

# ── Load listings

listings_path = os.path.join(METADATA_DIR, "ic3_listings.csv")
listings_df   = pd.read_csv(listings_path)
print("Listings loaded:", len(listings_df))
print("Types:", listings_df["source_type"].value_counts().to_dict())

# ── Scrape / extract all entries

all_records = []
failed      = []

for _, row in tqdm(listings_df.iterrows(), total=len(listings_df), desc="Extracting IC3 content"):
    url          = row["url"]
    source_type  = row.get("source_type", "PSA")
    source_year  = row.get("source_year", "")
    title_hint   = row.get("title_hint", "")
    published_hint = row.get("published_raw", "")

    if source_type == "AnnualReport_PDF":
        extracted = download_and_extract_pdf(url, PDF_DIR)
        source_label = f"IC3 - Annual Report {source_year}"
    else:
        extracted = extract_psa_article(url)
        source_label = f"IC3 - PSA {source_year}"
        time.sleep(SLEEP_BETWEEN_ARTICLES)

    if extracted.get("error"):
        failed.append({"url": url, "error": extracted["error"]})
        print(f"  FAILED: {url} → {extracted['error']}")

    title = extracted.get("title") or title_hint
    published_raw = extracted.get("published_raw") or published_hint

    all_records.append({
        "doc_id":        stable_doc_id(url),
        "title":         normalize_space(title),
        "url":           url,
        "published_raw": published_raw,
        "body_raw":      extracted.get("body_raw", ""),
        "source":        source_label,
        "source_type":   source_type,
        "ic3_topics":    extracted.get("ic3_topics", []),
    })

print(f"\nTotal records extracted: {len(all_records)}")
print(f"Failed: {len(failed)}")

# ── Save
raw_df = pd.DataFrame(all_records)
print("Columns:", raw_df.columns.tolist())
print("Shape:  ", raw_df.shape)
raw_df.head(3)

raw_jsonl_path = os.path.join(FULLTEXT_DIR, "ic3_articles_raw.jsonl")
raw_df.to_json(raw_jsonl_path, orient="records", lines=True, force_ascii=False)
print("Saved:", raw_jsonl_path)

if failed:
    failed_path = os.path.join(METADATA_DIR, "ic3_failed.csv")
    pd.DataFrame(failed).to_csv(failed_path, index=False)
    print(f"Failed log saved: {failed_path}")

# ══════════════════════════════════════════════════════════════
# ── NOTEBOOK 3 ── Clean + enrich + export master dataset
# Goal:
#   - clean dates + text
#   - dedupe
#   - extract fraud signals (URLs / emails / phones / IPs / crypto)
#   - apply fraud tag taxonomy (IC3-specific categories)
#   - save ic3_master.jsonl + ic3_master.csv
# ══════════════════════════════════════════════════════════════

import os, re, json
import pandas as pd
from urllib.parse import urlparse


def normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()

# ── Load raw
raw_jsonl_path = os.path.join(FULLTEXT_DIR, "ic3_articles_raw.jsonl")
raw_df = pd.read_json(raw_jsonl_path, lines=True)
print("Raw shape:", raw_df.shape)
print("\nColumns:", raw_df.columns.tolist())
print("\nFirst row sample:")
print(raw_df.iloc[0].to_dict() if len(raw_df) > 0 else "EMPTY — NB2 produced no records")

if raw_df.empty:
    raise ValueError(
        "ic3_articles_raw.jsonl is empty. "
        "NB2 extracted 0 articles — check ic3_failed.csv and re-run NB2."
    )

# ── Normalise column names defensively (mirrors FBI NB3 pattern)
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
    raw_df["source"] = "IC3"

if "ic3_topics" not in raw_df.columns:
    raw_df["ic3_topics"] = [[] for _ in range(len(raw_df))]

if "source_type" not in raw_df.columns:
    raw_df["source_type"] = "PSA"

# ── Text cleaning

def clean_body(text: str) -> str:
    if not text:
        return ""
    text = normalize_space(text)
    # Remove IC3-specific boilerplate
    text = re.sub(r"The IC3 accepts online Internet crime complaints.*$",     "", text, flags=re.IGNORECASE)
    text = re.sub(r"If you have been victimized.*?ic3\.gov.*?$",              "", text, flags=re.IGNORECASE)
    text = re.sub(r"This PSA is provided for informational purposes.*$",      "", text, flags=re.IGNORECASE)
    text = re.sub(r"To file a complaint.*ic3\.gov.*$",                        "", text, flags=re.IGNORECASE)
    text = re.sub(r"Additional resources.*$",                                 "", text, flags=re.IGNORECASE)
    # Annual report PDF artifacts (page numbers, headers)
    text = re.sub(r"\bPage \d+ of \d+\b",                                    "", text, flags=re.IGNORECASE)
    text = re.sub(r"\b20\d{2} Internet Crime Report\b",                      "", text, flags=re.IGNORECASE)
    return normalize_space(text)

df = raw_df.copy()

# ── Enrich annual report dates from title when published_raw is empty
def infer_date_from_title(row):
    if row["published_raw"] and str(row["published_raw"]).strip():
        return row["published_raw"]
    # Annual reports: extract year from source label (e.g. "IC3 - Annual Report 2022")
    m = re.search(r"Annual Report (\d{4})", str(row.get("source", "")))
    if m:
        return f"April 1, {int(m.group(1)) + 1}"  # IC3 publishes reports in Q1/Q2 of following year
    return ""

df["published_raw"] = df.apply(infer_date_from_title, axis=1)

# ── Parse datetime
df["published"] = pd.to_datetime(df["published_raw"], errors="coerce", utc=True)
df["date"]  = df["published"].dt.date.astype(str)
df["published_year"]  = df["published"].dt.year
df["published_month"] = df["published"].dt.month

# ── Clean text
df["title"]      = df["title"].astype(str).map(normalize_space)
df["body_1"]     = df["body_raw"].astype(str).map(clean_body)
df["body_short"] = df["body_1"].str[:1000]

# ── Dedupe on URL
df = df.drop_duplicates(subset=["url"]).reset_index(drop=True)
print("\nCleaned shape:", df.shape)
df[["doc_id", "date", "source_type", "title"]].head(3)

# ── Fraud signals extraction — uses fraud_signals_from_text from fraud_config ───
df["fraud_signals"] = df["body_1"].apply(fraud_signals_from_text)

# ── Fraud Tag Taxonomy — uses assign_fraud_tags from fraud_config ──────────────

df["fraud_tags"] = df.apply(lambda r: assign_fraud_tags(r["title"], r["body_1"]), axis=1)
df[["title", "fraud_tags", "ic3_topics"]].head(10)


# ── Quality checks
print("Missing values:")
print(df.isnull().sum())
print("\nShape:", df.shape)
print("\nColumns:", df.columns.tolist())

# ── Final dataset (same field order as FBI / FTC final_df)
final_df = df[[
    "doc_id", "date", "published_year", "published_month",
    "source", "source_type", "fraud_tags", "fraud_signals",
    "title", "body_1", "body_short", "url"
]].copy().reset_index(drop=True)

final_df["body_length"] = df["body_1"].str.len()
print("Shape:", final_df.shape)
print("\nColumns:", final_df.columns.tolist())

# Sample
print(final_df.loc[0, "body_1"][:500])

# Distribution checks
df["fraud_tags"].explode().value_counts()
final_df["published_year"].value_counts()
final_df["source_type"].value_counts()

# ── Export ic3_master.jsonl
jsonl_path = os.path.join(OUTPUT_FOLDER, "ic3_master.jsonl")
final_df.to_json(jsonl_path, orient="records", lines=True, force_ascii=False)
print("Saved:", jsonl_path)

# ── Export ic3_master.csv
csv_path = os.path.join(OUTPUT_FOLDER, "ic3_master.csv")
final_df_csv = final_df.copy()
final_df_csv["fraud_tags"]    = final_df_csv["fraud_tags"].apply(json.dumps)
final_df_csv["fraud_signals"] = final_df_csv["fraud_signals"].apply(json.dumps)
final_df_csv.to_csv(csv_path, index=False, encoding="utf-8")
print("Saved:", csv_path)

# ══════════════════════════════════════════════════════════════
# ── NOTEBOOK 4 ── Chunk + Fraud Tagging (mirrors FBI NB4)
# Goal:
#   - load ic3_master.jsonl
#   - chunk each article/report (chunk_size=1200, overlap=200)
#   - apply fraud dictionary + signals dictionary
#   - save ic3_chunks.jsonl + ic3_tagged_chunks.jsonl
# ══════════════════════════════════════════════════════════════

import os, re, json
import pandas as pd
from collections import defaultdict

os.makedirs(DICT_DIR, exist_ok=True)

# ── Load master
master_path = os.path.join(OUTPUT_FOLDER, "ic3_master.jsonl")
results = []
with open(master_path, "r", encoding="utf-8") as f:
    for line in f:
        results.append(json.loads(line))
print("Documents loaded:", len(results))

# ── Build keyword dicts from central fraud_config ─────────────────────────────
fraud_keywords  = build_fraud_keywords()   # {keyword: fraud_family}
signal_keywords = build_signal_keywords()  # {signal_keyword: signal_category}
print("Fraud keywords :", len(fraud_keywords))
print("Signal keywords:", len(signal_keywords))

# ── Also export CSVs to DICT_DIR for downstream tools
fraud_df   = pd.DataFrame(list(fraud_keywords.items()),  columns=["keyword", "fraud_family"])
signals_df = pd.DataFrame(list(signal_keywords.items()), columns=["signal_keyword", "signal_category"])
fraud_dict_path  = os.path.join(DICT_DIR, "fraud_dictionary.csv")
signal_dict_path = os.path.join(DICT_DIR, "fraud_signals_dictionary.csv")
fraud_df.to_csv(fraud_dict_path, index=False)
signals_df.to_csv(signal_dict_path, index=False)
print("Saved fraud dictionary:", fraud_dict_path)
print("Saved signals dictionary:", signal_dict_path)

# ── Inline reference number removal
def remove_inline_reference_numbers(text):
    if not text:
        return ""
    text = re.sub(r'(?<=[A-Za-z.,)])\d{1,2}\b', '', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

# ── Chunking function
def chunk_text(text, chunk_size=1200, overlap=200):
    if not text:
        return []
    chunks, start = [], 0
    step = chunk_size - overlap
    if step <= 0:
        raise ValueError("chunk_size must be greater than overlap")
    while start < len(text):
        chunk = text[start:start + chunk_size].strip()
        if len(chunk) > 150:
            chunks.append(chunk)
        start += step
    return chunks

# ── Build chunk rows
chunk_rows = []
for r in results:
    text = remove_inline_reference_numbers(r.get("body_1", r.get("body", "")))
    if not text or not text.strip():
        continue
    for idx, chunk in enumerate(chunk_text(text, chunk_size=1200, overlap=200)):
        chunk_rows.append({
            "doc_id":       r["doc_id"],
            "title":        r["title"],
            "source":       r["source"],
            "source_type":  r.get("source_type", "PSA"),
            "date":         r["date"],
            "url":          r.get("url", ""),
            "chunk_id":     idx,
            "chunk_text":   chunk,
            "chunk_length": len(chunk),
        })

chunks_path = os.path.join(FULLTEXT_DIR, "ic3_chunks.jsonl")
with open(chunks_path, "w", encoding="utf-8") as f:
    for row in chunk_rows:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")
print("Saved:", chunks_path)
print("Total chunks:", len(chunk_rows))

# Chunk summary CSV
chunks_df  = pd.DataFrame(chunk_rows)
summary_df = (
    chunks_df.groupby(["doc_id", "title", "source", "source_type", "date"], as_index=False)
    .agg(
        total_chunks    =("chunk_id",     "count"),
        avg_chunk_length=("chunk_length", "mean"),
        min_chunk_length=("chunk_length", "min"),
        max_chunk_length=("chunk_length", "max"),
    )
)
summary_path = os.path.join(FULLTEXT_DIR, "ic3_chunks_summary.csv")
summary_df.to_csv(summary_path, index=False)
print("Saved:", summary_path)

# ── Tagging — uses assign_fraud_tags_from_keywords from fraud_config
tagged_rows = []
for c in chunk_rows:
    chunk_text_val = c.get("chunk_text", "")
    fraud_tags_out = assign_fraud_tags_from_keywords(chunk_text_val, fraud_keywords)
    fraud_sigs_out = sorted({kw for kw in signal_keywords if kw in chunk_text_val.lower()})
    tagged_rows.append({
        "doc_id":             c["doc_id"],
        "title":              c["title"],
        "source":             c["source"],
        "source_type":        c.get("source_type", "PSA"),
        "date":               c["date"],
        "url":                c.get("url", ""),
        "chunk_id":           c["chunk_id"],
        "fraud_tags":         fraud_tags_out,
        "fraud_signals":      fraud_sigs_out,
        "fraud_signal_count": len(fraud_sigs_out),
        "chunk_text":         chunk_text_val,
    })
print("Tagged rows:", len(tagged_rows))

# ── Pivot to wide format (one row per doc)
doc_groups = defaultdict(list)
for row in tagged_rows:
    doc_groups[row["doc_id"]].append(row)

wide_rows = []
for doc_id, chunks in doc_groups.items():
    chunks = sorted(chunks, key=lambda x: x["chunk_id"])
    record = {
        "doc_id":      doc_id,
        "source":      chunks[0]["source"],
        "source_type": chunks[0].get("source_type", "PSA"),
        "date":        chunks[0]["date"],
        "title":       chunks[0]["title"],
        "url":         chunks[0].get("url", ""),
        "num_chunks":  len(chunks),
    }
    all_tags, seen_tags = [], set()
    for c in chunks:
        for t in c["fraud_tags"]:
            if t not in seen_tags:
                all_tags.append(t); seen_tags.add(t)
    record["fraud_tags"] = all_tags if all_tags else ["other"]
    all_signals, seen_signals = [], set()
    for c in chunks:
        for s in c["fraud_signals"]:
            if s not in seen_signals:
                all_signals.append(s); seen_signals.add(s)
    record["fraud_signals"] = all_signals
    for i, chunk in enumerate(chunks, start=1):
        record[f"body_{i}"]       = chunk["chunk_text"]
        record[f"fraud_tags_{i}"] = chunk["fraud_tags"]
    wide_rows.append(record)

output_path = os.path.join(OUTPUT_FOLDER, "ic3_tagged_chunks.jsonl")
with open(output_path, "w", encoding="utf-8") as f:
    for row in wide_rows:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")
print(f"Saved: {output_path}  |  Documents: {len(wide_rows)}")

# ── Tag distribution summary
rows = []
for row in tagged_rows:
    for tag in row["fraud_tags"]:
        rows.append({"doc_id": row["doc_id"], "date": row["date"],
                     "source_type": row["source_type"],
                     "fraud_tag": tag, "fraud_signal_count": row["fraud_signal_count"]})
summary_df = pd.DataFrame(rows)
if not summary_df.empty:
    tag_counts = summary_df["fraud_tag"].value_counts().reset_index()
    tag_counts.columns = ["fraud_tag", "count"]
    print(tag_counts.head(15))
    print("\nTag counts by source_type:")
    print(
        summary_df.groupby(["source_type", "fraud_tag"])
        .size().reset_index(name="count")
        .sort_values(["source_type", "count"], ascending=[True, False])
        .head(20)
    )
else:
    print("No fraud tags found yet.")
