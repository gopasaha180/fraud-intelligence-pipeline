# FinCEN Pipeline
# Run: python FinCen.py
# Output: outputs/fincen_tagged_chunks.jsonl

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
PDF_DIR       = os.path.join(BASE_DIR, 'data', 'raw_pdfs')
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

# # FinCEN Fraud Intelligence Platform — Early-Stage Colab Pipeline
#
# This notebook is part of a 6-notebook starter pipeline:
#
# 1. `01_advisories_first.ipynb`
# 2. `02_add_alerts.ipynb`
# 3. `03_add_notices.ipynb`
# 4. `04_text_extraction.ipynb`
# 5. `05_text_chunking.ipynb`
# 6. `06_fraud_tagging.ipynb`
#
# Current scope:
# - years: 2022, 2023, 2024, 2025, 2026
# - local file storage only
# - no database yet

# # Notebook 1 — Scrape Advisories First
#
# Goal:
# - define years in order
# - scrape FinCEN advisory detail pages
# - parse metadata
# - save `advisories_only.csv`

import os
import re
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from datetime import datetime
from tqdm import tqdm

RAW_PDF_DIR = os.path.join(BASE_DIR, "data", "raw_pdfs")

os.makedirs(RAW_PDF_DIR, exist_ok=True)
os.makedirs(METADATA_DIR, exist_ok=True)
os.makedirs(FULLTEXT_DIR, exist_ok=True)

HEADERS = {"User-Agent": "Mozilla/5.0"}

#start_year = 1996
#current_year = datetime.now().year

#years = sorted(range(start_year, current_year + 1), reverse=True)

years = sorted([2026, 2025, 2024, 2023, 2022,2021,2020])
print("Years in order:", years)

def clean_text(x):
    if x is None:
        return ""
    return re.sub(r" +", " ", str(x)).strip()

def extract_year_from_date(date_str):
    if not date_str:
        return None
    m = re.search(r"([0-9]{4})", str(date_str))
    return int(m.group(1)) if m else None

def fetch_html(url):
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.text

from datetime import datetime

def normalize_date(date_str):
    if not date_str or str(date_str).lower() == "nan":
        return None

    date_str = str(date_str).strip()

    formats = [
        "%B %d, %Y",   # August 28, 2025
        "%b %d, %Y",   # Aug 28, 2025
        "%m/%d/%Y",    # 08/28/2025
        "%Y-%m-%d"     # 2025-08-28
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.strftime("%m/%d/%Y")
        except:
            pass

    return None

def scrape_advisory_links_from_page(page_url):
    html = fetch_html(page_url)
    soup = BeautifulSoup(html, "lxml")

    records = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        full_url = urljoin(page_url, href)
        text = clean_text(a.get_text(" ", strip=True))

        if "/resources/advisories/" not in full_url:
            continue

        if full_url.endswith("/advisories") or full_url.endswith("/advisories/archive"):
            continue

        records.append({
            "detail_url": full_url,
            "anchor_text": text
        })

    return pd.DataFrame(records).drop_duplicates()

advisory_pages = [
    "https://www.fincen.gov/resources/advisoriesbulletinsfact-sheets/advisories",
    "https://www.fincen.gov/resources/advisoriesbulletinsfact-sheets/advisories/archive"
]


advisory_link_dfs = []
for page in advisory_pages:
    df_part = scrape_advisory_links_from_page(page)
    advisory_link_dfs.append(df_part)

advisory_links_df = pd.concat(advisory_link_dfs, ignore_index=True).drop_duplicates()
print("Advisory detail links found:", len(advisory_links_df))
advisory_links_df.head()

def parse_advisory_detail(detail_url, anchor_text=""):
    try:
        html = fetch_html(detail_url)
        soup = BeautifulSoup(html, "lxml")
        page_text = clean_text(soup.get_text(" ", strip=True))

        # Title
        title_tag = soup.find("h1")
        title = clean_text(title_tag.get_text(" ", strip=True)) if title_tag else clean_text(anchor_text)

        # Try to get FinCEN ID from page text first
        id_match = re.search(r"(FIN-\d{4}-A\d+)", page_text, re.IGNORECASE)

        # Fallback: get FinCEN ID from anchor text
        if not id_match and anchor_text:
            id_match = re.search(r"(FIN-\d{4}-A\d+)", anchor_text, re.IGNORECASE)

        # Fallback: get FinCEN ID from URL
        if not id_match:
            id_match = re.search(r"(FIN-\d{4}-A\d+)", detail_url, re.IGNORECASE)

        fincen_id = id_match.group(1).upper() if id_match else ""

        # Extract year directly from fincen_id
        year = None
        if fincen_id:
            year_match = re.search(r"FIN-(\d{4})-A\d+", fincen_id, re.IGNORECASE)
            if year_match:
                year = int(year_match.group(1))

        # Try to get a readable date if available
        date = ""

        date_match1 = re.search(
            r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}",
            page_text
        )
        if date_match1:
            date = date_match1.group(0)

        if not date:
            date_match2 = re.search(r"\b\d{2}/\d{2}/\d{4}\b", page_text)
            if date_match2:
                date = date_match2.group(0)

        # Find PDF link
        pdf_url = None
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if ".pdf" in href.lower():
                pdf_url = urljoin(detail_url, href)
                break

        return {
            "fincen_id": fincen_id,
            "title": title,
            "doc_type": "Advisory",
            "source_page": detail_url,
            "pdf_url": pdf_url,
            "date": date,
            "year": year
        }

    except Exception as e:
        print(f"Failed advisory detail page: {detail_url} -> {e}")
        return None

advisory_records = []

for _, row in tqdm(advisory_links_df.iterrows(), total=len(advisory_links_df)):
    rec = parse_advisory_detail(row["detail_url"], row["anchor_text"])

    if rec is not None and rec.get("year") is None:
        m = re.search(r"FIN-(\d{4})-A\d+", row["anchor_text"], re.IGNORECASE)
        if m:
            rec["year"] = int(m.group(1))

    if rec is not None and rec.get("year") in years:
        advisory_records.append(rec)

    time.sleep(0.3)

print("Number of advisory records:", len(advisory_records))

# Build final DataFrame
advisories_df = pd.DataFrame(advisory_records)

def extract_year_from_date(date_str):
    if not date_str:
        return None

    m = re.search(r"(20\d{2})", str(date_str))
    if m:
        return int(m.group(1))

    return None

if advisories_df.empty:
    print("No advisory records were collected.")

else:
    # basic dedup
    advisories_df = advisories_df.drop_duplicates()


    # rename year -> advisory_year
    advisories_df = advisories_df.rename(columns={"year": "advisory_year"})

    # extract date_year
    advisories_df["date_year"] = advisories_df["date"].apply(extract_year_from_date)

    # mark spanish rows
    advisories_df["is_spanish"] = advisories_df["title"].str.contains(
        r"\bAviso\b", case=False, na=False
    )

    # keep English first when same fincen_id appears twice
    advisories_df = advisories_df.sort_values(
        ["fincen_id", "is_spanish", "date"],
        na_position="last"
    )

    advisories_df = advisories_df.drop_duplicates(
        subset=["fincen_id"], keep="first")

    # remove helper column
    advisories_df = advisories_df.drop(columns=["is_spanish"])

    # final sort

    advisories_df = advisories_df[
    ["fincen_id", "title", "doc_type", "date", "source_page", "pdf_url"]
]

    advisories_df["date"] = advisories_df["date"].apply(normalize_date)

print("Advisories kept:", len(advisories_df))
advisories_df.head(20)

advisories_path = os.path.join(METADATA_DIR, "advisories_only.csv")
advisories_df.to_csv(advisories_path, index=False)
print("Saved:", advisories_path)

# # Notebook 2 — Add Alerts
#
# Goal:
# - load advisory metadata
# - scrape FinCEN Alerts
# - combine advisories + alerts
# - save `fincen_publications_step2_advisories_alerts.csv`

import os
import re
import requests
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from datetime import datetime

os.makedirs(METADATA_DIR, exist_ok=True)

HEADERS = {"User-Agent": "Mozilla/5.0"}

years = [2022, 2023, 2024, 2025, 2026]


def clean_text(x):
    if x is None:
        return ""
    return re.sub(r"\s+", " ", str(x)).strip()


def fetch_html(url):
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.text


def normalize_date(date_str):
    if not date_str or str(date_str).lower() == "nan":
        return None

    date_str = str(date_str).strip()

    formats = [
        "%m/%d/%Y",
        "%B %d, %Y",
        "%b %d, %Y",
        "%Y-%m-%d",
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.strftime("%m/%d/%Y")
        except:
            pass

    return None

advisories_path = os.path.join(METADATA_DIR, "advisories_only.csv")
advisories_df = pd.read_csv(advisories_path)

print("Advisories shape:", advisories_df.shape)
advisories_df.head()

def scrape_alerts_table(target_years):
    url = "https://www.fincen.gov/resources/advisoriesbulletinsfact-sheets"
    html = fetch_html(url)
    soup = BeautifulSoup(html, "lxml")

    records = []

    # find every table row on the page
    for tr in soup.find_all("tr"):
        cells = tr.find_all("td")
        if len(cells) < 2:
            continue

        # first cell usually has the alert link/title
        title_cell = cells[0]
        date_cell = cells[1]

        link = title_cell.find("a", href=True)
        if not link:
            continue

        anchor_text = clean_text(link.get_text(" ", strip=True))
        full_url = urljoin(url, link["href"])

        # keep only real alert IDs
        id_match = re.search(r"(FIN-\d{4}-Alert\d+)", anchor_text, re.IGNORECASE)
        if not id_match:
            continue

        fincen_id = id_match.group(1)

        # year from ID
        year_match = re.search(r"FIN-(\d{4})-Alert\d+", fincen_id, re.IGNORECASE)
        if not year_match:
            continue

        year = int(year_match.group(1))
        if year not in target_years:
            continue

        # date from the table
        raw_date = clean_text(date_cell.get_text(" ", strip=True))
        date = normalize_date(raw_date)

        # title: keep fincen id as the title for consistency with advisories
        title = fincen_id

        # pdf url: if table link is already a pdf keep it, else leave None for now
        pdf_url = full_url if ".pdf" in full_url.lower() else None

        records.append({
            "fincen_id": fincen_id,
            "title": title,
            "doc_type": "Alert",
            "date": date,
            "source_page": full_url,
            "pdf_url": pdf_url
        })

    alerts_df = pd.DataFrame(records).drop_duplicates()

    if alerts_df.empty:
        return alerts_df

    # remove Spanish duplicates if both exist
    alerts_df["is_spanish"] = alerts_df["source_page"].str.contains(
        r"spanish|espanol|español", case=False, na=False
    )

    alerts_df = alerts_df.sort_values(
        ["fincen_id", "is_spanish", "date"],
        na_position="last"
    )

    alerts_df = alerts_df.drop_duplicates(subset=["fincen_id"], keep="first")
    alerts_df = alerts_df.drop(columns=["is_spanish"])

    # same schema/order as advisories
    alerts_df = alerts_df[
        ["fincen_id", "title", "doc_type", "date", "source_page", "pdf_url"]
    ].copy()

    alerts_df = alerts_df.sort_values(
        ["date", "fincen_id"],
        na_position="last"
    ).reset_index(drop=True)

    return alerts_df

alerts_df = scrape_alerts_table(years)

print("Alerts shape:", alerts_df.shape)
print("Alerts columns:", alerts_df.columns.tolist())
alerts_df.head(20)

alerts_path = os.path.join(METADATA_DIR, "alerts_only.csv")
alerts_df.to_csv(alerts_path, index=False)
print("Saved:", alerts_path)

# make sure advisories use the same schema/order
advisories_df = advisories_df[
    ["fincen_id", "title", "doc_type", "date", "source_page", "pdf_url"]
].copy()

advisories_df["date"] = advisories_df["date"].apply(normalize_date)
alerts_df["date"] = alerts_df["date"].apply(normalize_date)

publications_df = pd.concat([advisories_df, alerts_df], ignore_index=True).drop_duplicates()

publications_df = publications_df.sort_values(
    by=["date", "doc_type", "fincen_id"],
    na_position="last"
).reset_index(drop=True)




combined_path = os.path.join(METADATA_DIR, "fincen_publications_step2_advisories_alerts.csv")
publications_df.to_csv(combined_path, index=False)

print("Combined rows:", len(publications_df))
print("Saved:", combined_path)
publications_df.head(20)

# # Notebook 3 — Add Notices
#
# Goal:
# - load advisories + alerts
# - scrape FinCEN Notices
# - combine everything
# - save final `fincen_publications.csv

import os
import re
import requests
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from datetime import datetime

os.makedirs(METADATA_DIR, exist_ok=True)

HEADERS = {"User-Agent": "Mozilla/5.0"}

years = [2022, 2023, 2024, 2025, 2026]

def clean_text(x):
    if x is None:
        return ""
    return re.sub(r"\s+", " ", str(x)).strip()


def fetch_html(url):
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.text


def normalize_date(date_str):
    if not date_str or str(date_str).lower() == "nan":
        return None

    date_str = str(date_str).strip()

    formats = [
        "%m/%d/%Y",
        "%B %d, %Y",
        "%b %d, %Y",
        "%Y-%m-%d"
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.strftime("%m/%d/%Y")
        except:
            pass

    return None

prev_path = os.path.join(METADATA_DIR, "fincen_publications_step2_advisories_alerts.csv")
prev_df = pd.read_csv(prev_path)
prev_df.head()

def scrape_notices_table(target_years):
    url = "https://www.fincen.gov/resources/advisoriesbulletinsfact-sheets"
    html = fetch_html(url)
    soup = BeautifulSoup(html, "lxml")

    records = []

    # Find the heading for the notices section
    notices_heading = soup.find(lambda tag: tag.name in ["h2", "h3"] and "fincen notices" in tag.get_text(strip=True).lower())
    if not notices_heading:
        return pd.DataFrame()

    notices_table = notices_heading.find_next("table")
    if not notices_table:
        return pd.DataFrame()

    for tr in notices_table.find_all("tr"):
        cells = tr.find_all("td")
        if len(cells) < 3:
            continue

        title_cell = cells[0]
        date_cell = cells[1]
        desc_cell = cells[2]

        link = title_cell.find("a", href=True)
        if not link:
            continue

        anchor_text = clean_text(link.get_text(" ", strip=True))
        full_url = urljoin(url, link["href"])
        desc_text = clean_text(desc_cell.get_text(" ", strip=True))

        combined_text = clean_text(f"{anchor_text} {desc_text}")

        # Require a real notice-like ID
        id_match = re.search(
            r"(FIN-\d{4}-(?:NTC\d*|CTA\d*))",
            combined_text,
            re.IGNORECASE
        )

        raw_date = clean_text(date_cell.get_text(" ", strip=True))
        date = normalize_date(raw_date)

        year = None
        if id_match:
            fincen_id = id_match.group(1).upper()
            year_match = re.search(r"FIN-(\d{4})-", fincen_id, re.IGNORECASE)
            if year_match:
                year = int(year_match.group(1))
        else:
            fincen_id = ""

        if year is None and date:
            year = int(date[-4:])

        if year not in target_years:
            continue

        records.append({
            "fincen_id": fincen_id if fincen_id else anchor_text,
            "title": fincen_id if fincen_id else anchor_text,
            "doc_type": "Notice",
            "date": date,
            "source_page": full_url,
            "pdf_url": full_url if full_url.lower().endswith(".pdf") else None
        })


    notices_df = pd.DataFrame(records).drop_duplicates()

    if notices_df.empty:
        return notices_df

    # remove Spanish duplicates if both exist
    notices_df["is_spanish"] = notices_df["source_page"].str.contains(
        r"spanish|espanol|español", case=False, na=False
    )

    notices_df = notices_df.sort_values(
        ["fincen_id", "is_spanish", "date"],
        na_position="last"
    )

    notices_df = notices_df.drop_duplicates(subset=["fincen_id"], keep="first")
    notices_df = notices_df.drop(columns=["is_spanish"])

    # same schema/order as advisories/alerts
    notices_df = notices_df[
        ["fincen_id", "title", "doc_type", "date", "source_page", "pdf_url"]
    ].copy()

    notices_df = notices_df.sort_values(
        ["date", "fincen_id"],
        na_position="last"
    ).reset_index(drop=True)

    return notices_df

notices_df = scrape_notices_table(years)

print("Notices rows:", len(notices_df))
print("Notices columns:", notices_df.columns.tolist())
notices_df.head(20)

notices_path = os.path.join(METADATA_DIR, "notices_only.csv")
notices_df.to_csv(notices_path, index=False)
print("Saved:", notices_path)

publications_df = pd.concat([prev_df, notices_df], ignore_index=True).drop_duplicates()
publications_df = publications_df.sort_values(
    by=["date", "doc_type", "fincen_id"],
    na_position="last"
).reset_index(drop=True)

final_metadata_path = os.path.join(METADATA_DIR, "fincen_publications.csv")
publications_df.to_csv(final_metadata_path, index=False)

print("Final metadata saved to:", final_metadata_path)
print("Final row count:", len(publications_df))
publications_df.head(34)

# # Notebook 4 — Download PDFs and Extract Full Text
#
# Goal:
# - load final metadata
# - download PDFs locally
# - extract text with PyMuPDF
# - fall back to OCR when needed
# - save `fincen_fulltext.jsonl`

import os
import re
import json
import time
import requests
import pandas as pd
import fitz
import pytesseract

from PIL import Image
from tqdm import tqdm

os.makedirs(METADATA_DIR, exist_ok=True)
os.makedirs(PDF_DIR, exist_ok=True)
os.makedirs(FULLTEXT_DIR, exist_ok=True)

HEADERS = {"User-Agent": "Mozilla/5.0"}

print("Metadata folder:", METADATA_DIR)
print("PDF folder:", PDF_DIR)
print("Fulltext folder:", FULLTEXT_DIR)

metadata_path = os.path.join(METADATA_DIR, "fincen_publications.csv")
publications_df = pd.read_csv(metadata_path)

print("Total publications:", len(publications_df))
print("Columns:", publications_df.columns.tolist())

publications_df["local_pdf_path"] = publications_df["fincen_id"].apply(
    lambda x: os.path.join(PDF_DIR, f"{x}.pdf")
)

publications_df.head()

def download_pdf(pdf_url, save_path):
    try:
        r = requests.get(pdf_url, headers=HEADERS, timeout=60)
        r.raise_for_status()
        with open(save_path, "wb") as f:
            f.write(r.content)
        return True
    except Exception as e:
        print(f"Download failed: {pdf_url} -> {e}")
        return False

downloaded = 0
skipped = 0
failed = 0

for _, row in tqdm(publications_df.iterrows(), total=len(publications_df)):
    pdf_url = row.get("pdf_url", None)
    save_path = row["local_pdf_path"]

    if pd.isna(pdf_url) or not str(pdf_url).strip():
        failed += 1
        continue

    if os.path.exists(save_path):
        skipped += 1
        continue

    ok = download_pdf(pdf_url, save_path)

    if ok:
        downloaded += 1
    else:
        failed += 1

    time.sleep(0.2)

print("Downloaded:", downloaded)
print("Already existed:", skipped)
print("Failed / missing URL:", failed)

with_paths_path = os.path.join(METADATA_DIR, "fincen_publications_with_local_paths.csv")
publications_df.to_csv(with_paths_path, index=False)

print("Saved:", with_paths_path)

missing = publications_df[
    ~publications_df["local_pdf_path"].apply(os.path.exists)
]

print("Missing PDFs:", len(missing))
missing.head()

def normalize_text(text):
    if not text:
        return ""

    text = text.replace("\u00a0", " ")
    text = text.replace("\xad", "")   # soft hyphen
    text = text.replace("ﬁ", "fi")
    text = text.replace("ﬂ", "fl")

    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)

    return text.strip()

def clean_page_text(text):
    if not text:
        return ""

    text = normalize_text(text)

    # remove common FinCEN header variants
    text = re.sub(r"\bF\s*I\s*N\s*C\s*E\s*N\b", " ", text, flags=re.I)
    text = re.sub(r"\bFINCEN\s+(ALERT|ADVISORY|NOTICE)\b", " ", text, flags=re.I)

    # remove stylized all-spaced words like A L E R T
    text = re.sub(r"\b(?:[A-Z]\s+){3,}[A-Z]\b", " ", text)

    # remove standalone page numbers
    text = re.sub(r"(?m)^\s*\d+\s*$", " ", text)

    # remove standalone document IDs
    text = re.sub(r"(?m)^\s*FIN-\d{4}-(?:A\d+|Alert\d+|NTC\d+)\s*$", " ", text)

    # remove date-only lines
    text = re.sub(r"(?m)^\s*[A-Z][a-z]+\s+\d{1,2},\s+\d{4}\s*$", " ", text)

    # join broken lines
    text = re.sub(r"(?<!\n)\n(?!\n)", " ", text)

    # fix hyphenation across line breaks
    text = re.sub(r"(\w)-\s+(\w)", r"\1\2", text)

    # collapse whitespace
    text = re.sub(r"\s+", " ", text)

    return text.strip()

def clean_document_text(text):
    if not text:
        return ""

    text = normalize_text(text)

    # 1) Start from the true title, not from whatever PDF object came first
    title_markers = [
        "Alert on ",
        "Advisory on ",
        "Notice on ",
        "Alert:",
        "Advisory:",
        "Notice:"
    ]

    for marker in title_markers:
        pos = text.find(marker)
        if pos != -1 and pos < 4000:
            text = text[pos:]
            break

    # 2) Remove SAR filing request block
    text = re.sub(
        r"Suspicious Activity Report\s*\(SAR\)\s*Filing Request:.*?(?=Treasury.?s 2024 National Money Laundering Risk Assessment|Overview of|$)",
        " ",
        text,
        flags=re.I
    )

    # 3) Remove inline footnote numbers stuck to words
    text = re.sub(r"([A-Za-z])(\d{1,2})\b", r"\1", text)

    # 4) Remove obvious leftover citation fragments
    text = re.sub(r"\b\d+\.\s*See\b.*?(?=(Alert on|Advisory on|Notice on|Overview of|$))", " ", text, flags=re.I)
    text = re.sub(r"\b\d+\.\s*Id\.\b", " ", text, flags=re.I)

    # 5) Clean common broken FinCEN leftovers
    text = re.sub(r"\(\s*\)", " ", text)
    text = re.sub(r"\bone of\s+[’']s\b", " ", text, flags=re.I)
    text = re.sub(r"\bFinancial Crimes Enforcement Network\s*\(\s*\)", "Financial Crimes Enforcement Network", text)

    # 6) Collapse whitespace
    text = re.sub(r"\s+", " ", text)

    return text.strip()

def extract_text_native(page):
    return page.get_text("text")

def page_needs_ocr(text):
    if not text or len(text.strip()) < 80:
        return True

    alpha_chars = sum(ch.isalpha() for ch in text)
    total_chars = max(len(text), 1)
    alpha_ratio = alpha_chars / total_chars

    if alpha_ratio < 0.45:
        return True

    return False

def extract_text_ocr(page, dpi=250):
    matrix = fitz.Matrix(dpi / 72, dpi / 72)
    pix = page.get_pixmap(matrix=matrix, alpha=False)

    mode = "RGB" if pix.n < 4 else "RGBA"
    img = Image.frombytes(mode, [pix.width, pix.height], pix.samples)

    text = pytesseract.image_to_string(img)
    return text

def extract_fincen_text(pdf_path):
    pages_out = []
    ocr_pages = 0

    try:
        doc = fitz.open(pdf_path)

        for i, page in enumerate(doc):
            native_text = extract_text_native(page)
            native_text = normalize_text(native_text)

            if page_needs_ocr(native_text):
                page_text = extract_text_ocr(page)
                method = "ocr"
                ocr_pages += 1
            else:
                page_text = native_text
                method = "native"

            page_text = clean_page_text(page_text)

            pages_out.append({
                "page_num": i + 1,
                "method": method,
                "text": page_text
            })

        doc.close()

        full_text_raw = "\n\n".join(
            p["text"] for p in pages_out if p["text"]
        )
        full_text_raw = normalize_text(full_text_raw)
        full_text_clean = clean_document_text(full_text_raw)

        return {
            "extraction_method": "native_plus_ocr_fallback",
            "ocr_pages": ocr_pages,
            "page_count": len(pages_out),
            "pages": pages_out,
            "full_text": full_text_raw,
            "full_text_clean": full_text_clean
        }

    except Exception as e:
        return {
            "extraction_method": "failed",
            "ocr_pages": 0,
            "page_count": 0,
            "pages": [],
            "full_text": "",
            "full_text_clean": "",
            "error": str(e)
        }

test_path = publications_df.loc[
    publications_df["fincen_id"] == "FIN-2026-Alert001",
    "local_pdf_path"
].iloc[0]

test_result = extract_fincen_text(test_path)

print("Method:", test_result["extraction_method"])
print("OCR pages:", test_result["ocr_pages"])
print("Pages:", test_result["page_count"])
print()
print(test_result["full_text_clean"][:5000])

results = []

for _, row in tqdm(publications_df.iterrows(), total=len(publications_df)):
    pdf_path = row["local_pdf_path"]

    if not os.path.exists(pdf_path):
        continue

    extracted = extract_fincen_text(pdf_path)

    results.append({
        "fincen_id": row["fincen_id"],
        "title": row["title"],
        "doc_type": row["doc_type"],
        "date": row["date"],
        "source_page": row.get("source_page", ""),
        "pdf_url": row["pdf_url"],
        "local_pdf_path": pdf_path,
        "method": extracted["extraction_method"],
        "ocr_pages": extracted["ocr_pages"],
        "page_count": extracted["page_count"],
        "full_text": extracted["full_text"],
        "full_text_clean": extracted["full_text_clean"]
    })

print("Documents processed:", len(results))

jsonl_path = os.path.join(FULLTEXT_DIR, "fincen_fulltext.jsonl")

with open(jsonl_path, "w", encoding="utf-8") as f:
    for r in results:
        f.write(json.dumps(r, ensure_ascii=False) + "\n")

print("Saved:", jsonl_path)

summary = pd.DataFrame([
    {
        "fincen_id": r["fincen_id"],
        "title": r["title"],
        "doc_type": r["doc_type"],
        "date": r["date"],
        "text_length_raw": len(r["full_text"]),
        "text_length_clean": len(r["full_text_clean"]),
        "method": r["method"],
        "ocr_pages": r["ocr_pages"],
        "page_count": r["page_count"]
    }
    for r in results
])

summary_path = os.path.join(FULLTEXT_DIR, "fincen_fulltext_summary.csv")
summary.to_csv(summary_path, index=False)

print("Saved:", summary_path)
summary.head()

sample = next((r for r in results if r["fincen_id"] == "FIN-2026-Alert001"), results[0])

print("ID:", sample["fincen_id"])
print("Type:", sample["doc_type"])
print("Method:", sample["method"])
print("Pages:", sample["page_count"])
print("OCR pages:", sample["ocr_pages"])
print()
print(sample["full_text_clean"][:5000])

pages_out = []

full_text_raw = "\n\n".join(p["text"] for p in pages_out if p["text"])
full_text_raw = normalize_text(full_text_raw)
full_text_clean = clean_document_text(full_text_raw)

test_result["full_text_clean"]

# # Notebook 5
#
# Goal:
# - loads fincen_fulltext.jsonl
#
# - reads full_text_clean
#
# - chunks each document
#
# - saves fincen_chunks.jsonl
#
# - saves a chunk summary CSV

# Imports and paths

import os
import json
import pandas as pd


jsonl_path = os.path.join(FULLTEXT_DIR, "fincen_fulltext.jsonl")
print("Input:", jsonl_path)

# Load fulltext records

results = []

with open(jsonl_path, "r", encoding="utf-8") as f:
    for line in f:
        results.append(json.loads(line))

print("Documents loaded:", len(results))
results[0].keys()

def remove_inline_reference_numbers(text):
    if not text:
        return ""

    # remove digit markers attached after words or punctuation
    text = re.sub(r'(?<=[A-Za-z.,)])\d{1,2}\b', '', text)

    # collapse spaces
    text = re.sub(r'\s+', ' ', text)

    return text.strip()

# Chunking function

def chunk_text(text, chunk_size=1200, overlap=200):
    if not text:
        return []

    chunks = []
    start = 0
    text_length = len(text)

    step = chunk_size - overlap
    if step <= 0:
        raise ValueError("chunk_size must be greater than overlap")

    while start < text_length:
        end = start + chunk_size
        chunk = text[start:end].strip()

        # keep only meaningful chunks
        if len(chunk) > 150:
            chunks.append(chunk)

        start += step

    return chunks

# Build chunk rows
chunk_rows = []

for r in results:
    text = r.get("full_text_clean", "")
    text = remove_inline_reference_numbers(text)

    if not text or not text.strip():
        continue

    chunks = chunk_text(text, chunk_size=1200, overlap=200)

    for idx, chunk in enumerate(chunks):
        chunk_rows.append({
            "fincen_id": r["fincen_id"],
            "title": r["title"],
            "doc_type": r["doc_type"],
            "date": r["date"],
            "source_page": r.get("source_page", ""),
            "chunk_id": idx,
            "chunk_text": chunk,
            "chunk_length": len(chunk)
        })

# Save chunk JSONL
chunks_path = os.path.join(FULLTEXT_DIR, "fincen_chunks.jsonl")

with open(chunks_path, "w", encoding="utf-8") as f:
    for row in chunk_rows:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")

print("Saved:", chunks_path)
print("Total chunks:", len(chunk_rows))

# Save chunk summary CSV

chunks_df = pd.DataFrame(chunk_rows)

summary_df = (
    chunks_df.groupby(["fincen_id", "title", "doc_type", "date"], as_index=False)
    .agg(
        total_chunks=("chunk_id", "count"),
        avg_chunk_length=("chunk_length", "mean"),
        min_chunk_length=("chunk_length", "min"),
        max_chunk_length=("chunk_length", "max"),
    )
)

summary_path = os.path.join(FULLTEXT_DIR, "fincen_chunks_summary.csv")
summary_df.to_csv(summary_path, index=False)

print("Saved:", summary_path)
summary_df.head()

# Inspect chunks for one document

sample_id = "FIN-2026-Alert001"

sample_chunks = [r for r in chunk_rows if r["fincen_id"] == sample_id]

print("Chunks for", sample_id, ":", len(sample_chunks))
print()
print(sample_chunks[0]["chunk_text"][:1200])

# # Next step:
#
#
# - Build the Fraud Dictionary
# - Create the Tagging Notebook 6
#
# The dictionary defines the fraud taxonomy, and the tagging notebook applies it to the dataset.
#
# Think of it like this:
#
# Fraud Dictionary  →  Tagging Notebook  →  Labeled Dataset

# Create the dictionaries folder



os.makedirs(DICT_DIR, exist_ok=True)

print("Dictionary folder:", DICT_DIR)

# ── Fraud dictionary — sourced from central fraud_config ─────────────────────
# The full canonical dictionary (including new families) lives in fraud_config.py.
# Here we build the DataFrame and save the CSV for downstream notebook compatibility.

import pandas as pd

fraud_df = pd.DataFrame(FRAUD_DICTIONARY, columns=["keyword", "fraud_family"])
print("Fraud dictionary keywords:", len(fraud_df))

# fraud_df already built above — skipping duplicate
print("Fraud keywords:", len(fraud_df))

# Save the fraud dictionary

fraud_dict_path = os.path.join(DICT_DIR, "fraud_dictionary.csv")

fraud_df.to_csv(fraud_dict_path, index=False)

print("Saved fraud dictionary:", fraud_dict_path)

pd.read_csv(fraud_dict_path).head(10)

# ── Fraud signals dictionary — sourced from central fraud_config ─────────────
# The full canonical signals list lives in fraud_config.py.
# Here we build the DataFrame and save the CSV for downstream notebook compatibility.

signals_df = pd.DataFrame(FRAUD_SIGNALS, columns=["signal_keyword", "signal_category"])
print("Fraud signal keywords:", len(signals_df))

# signals_df already built above — skipping duplicate
print("Signal keywords:", len(signals_df))

signal_dict_path = os.path.join(
    BASE_DIR,
    "data/dictionaries/fraud_signals_dictionary.csv"
)

signals_df.to_csv(signal_dict_path, index=False)

print("Saved fraud signals dictionary:", signal_dict_path)

pd.read_csv(signal_dict_path).head(10)

# # Notebook 6
#
# Goal:
# - loads fincen_chunks.jsonl
#
# - reads fraud_dictionary.csv
#
# - read fraud_signals_dictionary.csv
#
# - tagging + signal extraction
#
# - saves fincen_tagged_chunks.jsonl

# Load the signal dictionary
import os
import json
import re
import pandas as pd


chunks_path = os.path.join(BASE_DIR, "data/fulltext/fincen_chunks.jsonl")
fraud_dict_path = os.path.join(BASE_DIR, "data/dictionaries/fraud_dictionary.csv")
signal_dict_path = os.path.join(BASE_DIR, "data/dictionaries/fraud_signals_dictionary.csv")

# ── Load chunks from JSONL + build keyword dicts from fraud_config ───────────
chunks = []
with open(chunks_path, "r", encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if line:
            chunks.append(json.loads(line))

fraud_keywords  = build_fraud_keywords()   # {keyword: fraud_family}
signal_keywords = build_signal_keywords()  # {signal_keyword: signal_category}

print("Chunks loaded:", len(chunks))
print("Fraud keywords:", len(fraud_keywords))
print("Signal keywords:", len(signal_keywords))

# fraud_keywords and signal_keywords already built above via build_fraud_keywords()
# and build_signal_keywords() from fraud_config — no additional step needed.

# ── Tagging functions from fraud_config ──────────────────────────────────────
# assign_fraud_tags_from_keywords(text, fraud_keywords) -> list
# (replaces the local detect_fraud_tags function)
#
# For signals: use set comprehension directly — no wrapper needed.
# Both are imported from fraud_config at the top of this notebook.

print("Tagging functions ready (from fraud_config).")

tagged_rows = []

for c in chunks:
    chunk_text = c.get("chunk_text", "")

    fraud_tags    = assign_fraud_tags_from_keywords(chunk_text, fraud_keywords)
    fraud_signals = sorted({kw for kw in signal_keywords if kw in chunk_text.lower()})

    tagged_rows.append({
        "fincen_id":          c["fincen_id"],
        "title":              c["title"],
        "doc_type":           c["doc_type"],
        "date":               c["date"],
        "source_page":        c.get("source_page", ""),
        "chunk_id":           c["chunk_id"],
        "fraud_tags":         fraud_tags,
        "fraud_signals":      fraud_signals,
        "fraud_signal_count": len(fraud_signals),
        "chunk_text":         chunk_text
    })

print("Tagged rows:", len(tagged_rows))
tagged_rows[:2]

# ══════════════════════════════════════════════════════════════
# Notebook 6 — Save: pivot chunks to wide format (one row per doc)
# Each chunk's body and tags become numbered columns:
#   body_1, body_2, ... and fraud_tags_1, fraud_tags_2, ...
# Shared scalar fields (doc_id, title, doc_type, date, url) appear once.
# ══════════════════════════════════════════════════════════════

from collections import defaultdict

# Group tagged rows by fincen_id, preserving chunk order
doc_groups = defaultdict(list)
for row in tagged_rows:
    doc_groups[row["fincen_id"]].append(row)

wide_rows = []

for fincen_id, chunks in doc_groups.items():
    # Sort by chunk_id to guarantee order
    chunks = sorted(chunks, key=lambda x: x["chunk_id"])

    # Scalar fields from the first chunk (same for all chunks of a doc)
    record = {
        "doc_id":     fincen_id,
        "source":     "FinCEN",
        "date":       chunks[0]["date"],
        "title":      chunks[0]["title"],
        "doc_type":   chunks[0]["doc_type"],
        "url":        chunks[0].get("source_page", ""),
        "num_chunks": len(chunks),
    }

    # Union of all fraud tags across chunks (for master-level analysis)
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

# Write wide-format JSONL — one line per document
output_path = os.path.join(OUTPUT_FOLDER, "fincen_tagged_chunks.jsonl")

with open(output_path, "w", encoding="utf-8") as f:
    for row in wide_rows:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")

print(f"Saved: {output_path}")
print(f"Documents written: {len(wide_rows)}")

# Sanity check
if wide_rows:
    print("Columns in first record:", list(wide_rows[0].keys()))
    max_chunks = max(r["num_chunks"] for r in wide_rows)
    print(f"Max chunks in any document: {max_chunks}")

rows = []
for row in tagged_rows:
    for tag in row["fraud_tags"]:
        rows.append({
            "fincen_id": row["fincen_id"],
            "date": row["date"],
            "fraud_tag": tag,
            "fraud_signal_count": row["fraud_signal_count"]
        })

summary_df = pd.DataFrame(rows)

if not summary_df.empty:
    tag_counts = summary_df["fraud_tag"].value_counts().reset_index()
    tag_counts.columns = ["fraud_tag", "count"]
    print(tag_counts.head(10))
else:
    print("No fraud tags found yet.")
