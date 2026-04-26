# Outseer Pipeline
# Run: python Outseer.py
# Output: outputs/outseer_scraped_data.jsonl

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

# ══════════════════════════════════════════════════════════════
# ── CELL 1 ── Collect article URLs from Outseer public pages
# v7: no hardcoded dates — Wayback CDX API handles all date
#     resolution dynamically in Cell 2
# ══════════════════════════════════════════════════════════════

import re, time, json, hashlib
import requests
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from tqdm import tqdm

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
}

BASE_URL  = 'https://www.outseer.com'
MAX_PAGES = 5

# ── Expanded known URLs (v5) — covers 2021-2025 ───────────────────────────────
KNOWN_URLS = [
    # ── Blog posts ──
    'https://www.outseer.com/blog/notable-fraud-trends-in-2022-insights-from-outseer-fraudactions-threat-intelligence-team/',
    'https://www.outseer.com/blog/insights-from-outseers-global-fraud-scams-trends-report/',
    'https://www.outseer.com/blog/fraud-monitoring/',
    'https://www.outseer.com/blog/uk-scams-social-engineering-and-app-fraud/',
    'https://www.outseer.com/blog/outseer-3-d-secure-empowering-the-fight-against-scams/',
    'https://www.outseer.com/blog/scam-mitigation-quick-guide-your-journey-to-protection/',
    'https://www.outseer.com/blog/following-the-fraud-new-research-about-money-mule-networks/',
    'https://www.outseer.com/blog/new-era-of-phishing-ai-both-sides/',
    # ── Press releases ──
    'https://www.outseer.com/press-release/real-time-payments-and-app-fraud-emerging-globally/',
    'https://www.outseer.com/press-release/outseer-fraudaction-introduces-intelligence-alerts-to-dashboard-intelligence-feeds/',
    'https://www.outseer.com/press-release/2024-global-fraud-and-scams-trends-report/',
    'https://www.outseer.com/press-release/outseer-announces-2024-trends-in-faster-payments-fraud-report/',
    # ── Fraud protection / product pages ──
    'https://www.outseer.com/fraud-protection/enhancing-fraud-protection/',
    'https://www.outseer.com/payment-security/outseer-report-fraudulent-banking/',
    # ── Quarterly fraud reports ──
    'https://www.outseer.com/fraud-report-q2-2021/',
    'https://www.outseer.com/fraud-report-q3-2021/',
    # ── Reports / resources ──
    'https://www.outseer.com/reports/2024-global-fraud-and-scams-trends-report/',
    'https://www.outseer.com/reports/datos-trends-in-faster-payments-fraud/',
    'https://www.outseer.com/datos-trends-in-faster-payments-fraud/',
]

LISTING_PAGES = [
    'https://www.outseer.com/fraud-and-payment-blog/',
    'https://www.outseer.com/category/press-release/',
    'https://www.outseer.com/category/fraud-intelligence/',
    'https://www.outseer.com/category/fraud-trends/',
]

def normalize_space(text):
    return re.sub(r'\s+', ' ', (text or '')).strip()

def stable_doc_id(url):
    return hashlib.sha1(url.encode('utf-8')).hexdigest()[:14]

def fetch_html(url):
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.text

def parse_listing_page(url):
    try:
        html = fetch_html(url)
    except Exception as e:
        print(f'  SKIP {url} → {e}')
        return []

    soup  = BeautifulSoup(html, 'lxml')
    items = []

    article_links = (
        soup.select('h2.entry-title a') or
        soup.select('h3.entry-title a') or
        soup.select('article a[rel="bookmark"]') or
        soup.select('.post-title a') or
        soup.select('h2 a[href*="outseer.com"]') or
        soup.select('h3 a[href*="outseer.com"]') or
        [a for a in soup.find_all('a', href=True)
         if 'outseer.com' in a.get('href','') and
         len(normalize_space(a.get_text())) > 20]
    )

    seen_hrefs = set()
    for a in article_links:
        href  = a.get('href', '').strip()
        title = normalize_space(a.get_text(' ', strip=True))
        if not href.startswith('http'):
            href = urljoin(BASE_URL, href)
        if 'outseer.com' not in href or href in seen_hrefs:
            continue
        if any(skip in href for skip in ['#', 'wp-content', 'wp-includes', 'feed', 'xml']):
            continue
        if title and href:
            items.append({'title_hint': title, 'url': href})
            seen_hrefs.add(href)

    return items

# ── Collect all article URLs ──────────────────────────────────────────────────
all_listing = []
seen = set()

for base_listing in LISTING_PAGES:
    print(f'Scraping listing: {base_listing}')
    for page_num in range(1, MAX_PAGES + 1):
        page_url = base_listing if page_num == 1 else f'{base_listing.rstrip("/")}/page/{page_num}/'
        items = parse_listing_page(page_url)
        if not items:
            break
        new = [it for it in items if it['url'] not in seen]
        for it in new:
            all_listing.append(it)
            seen.add(it['url'])
        print(f'  Page {page_num}: +{len(new)} new (total={len(all_listing)})')
        if not new:
            break
        time.sleep(1.0)

# ── Fallback / supplement: always add KNOWN_URLS not already found ────────────
added_from_known = 0
for url in KNOWN_URLS:
    if url not in seen:
        all_listing.append({'title_hint': '', 'url': url})
        seen.add(url)
        added_from_known += 1

if added_from_known:
    print(f'Added {added_from_known} known URLs not found via dynamic scraping.')

print(f'\nTotal article URLs collected: {len(all_listing)}')
for i, x in enumerate(all_listing[:5], 1):
    print(i, x['url'])

# ══════════════════════════════════════════════════════════════
# ── CELL 2 ── Scrape each article page
# v7: Wayback CDX API for dates (no hardcoding — works for new
#     articles automatically). Falls back to JSON-LD → meta →
#     <time> → date span if CDX returns nothing.
# ══════════════════════════════════════════════════════════════

import functools

CDX_API = 'http://web.archive.org/cdx/search/cdx'

@functools.lru_cache(maxsize=256)
def wayback_date(url):
    """
    Query the Wayback Machine CDX API for the earliest capture timestamp
    of the given URL. Returns an ISO date string (YYYY-MM-DD) or ''.
    Results are cached in-process so repeat calls cost nothing.
    """
    try:
        params = {
            'url':      url,
            'output':   'json',
            'fl':       'timestamp',
            'limit':    '1',          # earliest capture only
            'from':     '20200101',   # ignore very old captures
            'filter':   'statuscode:200',
            'collapse': 'timestamp:8' # one result per day
        }
        r = requests.get(CDX_API, params=params, timeout=10)
        rows = r.json()
        # rows[0] is the header ['timestamp'], rows[1] is first data row
        if len(rows) >= 2:
            ts = rows[1][0]  # e.g. '20230505120000'
            return f'{ts[:4]}-{ts[4:6]}-{ts[6:8]}'
    except Exception:
        pass
    return ''

def extract_article(url):
    try:
        html = fetch_html(url)
    except Exception as e:
        return {'title': '', 'published_raw': '', 'content': '', 'error': str(e)}

    soup = BeautifulSoup(html, 'lxml')

    # Remove noise (keep ld+json scripts for date fallback)
    for tag in soup.select('script[type!="application/ld+json"], style, nav, footer, header, aside, .sidebar'):
        tag.decompose()

    # ── Title ─────────────────────────────────────────────────────────────────
    h1 = soup.select_one('h1')
    title = normalize_space(h1.get_text(' ', strip=True)) if h1 else ''

    # ── Date (v7: Wayback CDX → JSON-LD → meta → time → span) ────────────────
    published = ''

    # 1. Wayback Machine CDX API — works for any URL, no hardcoding needed
    published = wayback_date(url)

    # 2. JSON-LD structured data
    if not published:
        for script in soup.find_all('script', type='application/ld+json'):
            try:
                ld = json.loads(script.string or '')
                nodes = ld if isinstance(ld, list) else ld.get('@graph', [ld])
                for node in nodes:
                    date_val = node.get('datePublished') or node.get('dateModified')
                    if date_val:
                        published = normalize_space(str(date_val))
                        break
            except Exception:
                pass
            if published:
                break

    # 3. <meta property="article:published_time">
    if not published:
        meta_date = soup.find('meta', {'property': 'article:published_time'})
        if meta_date:
            published = normalize_space(meta_date.get('content', ''))

    # 4. <time> tag
    if not published:
        time_tag = soup.find('time')
        if time_tag:
            published = normalize_space(time_tag.get('datetime', '') or time_tag.get_text())

    # 5. Date span
    if not published:
        date_span = soup.find('span', class_=re.compile(r'date|published|post-date', re.I))
        if date_span:
            published = normalize_space(date_span.get_text(' ', strip=True))

    # ── Body content — expanded selector chain + <p> fallback ─────────────────
    body_div = (
        soup.select_one('.entry-content') or
        soup.select_one('.post-content') or
        soup.select_one('[class*="body_1"]') or
        soup.select_one('[class*="article"]') or
        soup.select_one('[class*="blog"]') or
        soup.select_one('article') or
        soup.select_one('main')
    )

    if body_div and len(body_div.get_text(strip=True)) > 50:
        content = normalize_space(body_div.get_text(' ', strip=True))
    else:
        paragraphs = soup.find_all('p')
        content = normalize_space(
            ' '.join(p.get_text(' ', strip=True) for p in paragraphs
                     if len(p.get_text(strip=True)) > 40)
        )

    return {'title': title, 'published_raw': published, 'content': content, 'error': ''}

# ── Scrape all articles ───────────────────────────────────────────────────────
scraped_data = []
failed = []

for item in tqdm(all_listing, desc='Scraping Outseer articles'):
    result = extract_article(item['url'])
    if result['error']:
        failed.append({'url': item['url'], 'error': result['error']})
    if not result['content']:
        print(f'  WARNING: No content extracted from {item["url"]}')
    scraped_data.append({
        'doc_id':        stable_doc_id(item['url']),
        'url':           item['url'],
        'title':         result['title'] or item['title_hint'],
        'published_raw': result['published_raw'],
        'body_1':        result['content'],
        'source':        'Outseer',
    })
    time.sleep(1.5)

scraped_df = pd.DataFrame(scraped_data)
print(f'\nTotal scraped: {len(scraped_df)} | Failed: {len(failed)}')
print(f'Records with body_1: {(scraped_df["body_1"].str.strip().str.len() > 50).sum()}')
print(f'Records with date:    {(scraped_df["published_raw"].str.strip().str.len() > 0).sum()}')
print('Columns:', scraped_df.columns.tolist())
scraped_df.head(3)

# ══════════════════════════════════════════════════════════════
# ── CELL 3 ── Clean + enrich
# ══════════════════════════════════════════════════════════════

if scraped_df.empty or 'published_raw' not in scraped_df.columns:
    print('WARNING: scraped_df is empty or missing expected columns.')
    print('Columns present:', scraped_df.columns.tolist() if not scraped_df.empty else 'none')
    scraped_df = pd.DataFrame(columns=['doc_id','url','title','published_raw','body_1','source'])

scraped_df['published']       = pd.to_datetime(scraped_df['published_raw'], errors='coerce', utc=True)
scraped_df['date']            = scraped_df['published'].dt.date.astype(str)
scraped_df['published_year']  = scraped_df['published'].dt.year
scraped_df['published_month'] = scraped_df['published'].dt.month

# Drop empty content rows
if not scraped_df.empty:
    scraped_df = scraped_df[scraped_df['body_1'].str.strip().str.len() > 50].reset_index(drop=True)
    scraped_df = scraped_df.drop_duplicates(subset=['url']).reset_index(drop=True)

print('Cleaned shape:', scraped_df.shape)
if not scraped_df.empty:
    print('Date range:', scraped_df['published'].min(), '→', scraped_df['published'].max())
    print(scraped_df[['title', 'date', 'published_year']].to_string())
else:
    print('No records after cleaning — check that scraping returned articles.')

# ══════════════════════════════════════════════════════════════
# ── CELL 4 ── Fraud Tagging — uses fraud_config central config
# ══════════════════════════════════════════════════════════════

import json as _json

# Apply fraud tags and signals using imported functions from fraud_config
scraped_df['fraud_tags']    = scraped_df['body_1'].apply(
    lambda body: assign_fraud_tags('', body)
)
scraped_df['fraud_signals'] = scraped_df['body_1'].apply(fraud_signals_from_text)

print('Tagging complete. Tag distribution:')
print(scraped_df['fraud_tags'].explode().value_counts())

# ══════════════════════════════════════════════════════════════
# ── CELL 5 ── Export — save to Drive OUTPUT_FOLDER
# ══════════════════════════════════════════════════════════════

outseer_jsonl = os.path.join(OUTPUT_FOLDER, 'outseer_scraped_data.jsonl')
outseer_json  = os.path.join(OUTPUT_FOLDER, 'outseer_scraped_data.json')
outseer_csv   = os.path.join(OUTPUT_FOLDER, 'outseer_scraped_data.csv')

# JSONL export (line-delimited)
scraped_df.to_json(outseer_jsonl, orient='records', lines=True, force_ascii=False)
# JSON export (array format — for orchestrator compatibility)
scraped_df.to_json(outseer_json, orient='records', indent=2, force_ascii=False)

# CSV export (serialize lists to JSON strings)
export_df = scraped_df.copy()
export_df['fraud_tags']    = export_df['fraud_tags'].apply(_json.dumps)
export_df['fraud_signals'] = export_df['fraud_signals'].apply(_json.dumps)
export_df.to_csv(outseer_csv, index=False)

print('Saved to Drive:')
print(' ', outseer_jsonl)
print(' ', outseer_json)
print(' ', outseer_csv)
print(f'\nTotal records: {len(scraped_df)}')
print(f'Records with fraud tags: {(scraped_df["fraud_tags"].apply(lambda x: x != ["other"]).sum())}')
print(f'Records with dates:      {scraped_df["published"].notna().sum()}')
