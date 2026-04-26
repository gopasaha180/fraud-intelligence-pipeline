# PYMNTS Pipeline
# Run: python PYMNTS.py
# Output: outputs/pymnts_master.jsonl

import os
import re
import time
import json
import hashlib
import sys

import requests
import pandas as pd
from bs4 import BeautifulSoup
from tqdm import tqdm

# ── Path config ────────────────────────────────────────────────
BASE_DIR      = os.environ.get(
    'FRAUD_BASE_DIR',
    r'C:\Users\josephsingleton\Documents\fraud-dashboard'   # <-- update fallback for local use
)
OUTPUT_FOLDER = os.path.join(BASE_DIR, 'outputs')
os.makedirs(OUTPUT_FOLDER, exist_ok=True)
print('BASE_DIR:', BASE_DIR)
print('Ready.')

# ── fraud_config ──────────────────────────────────────────────
_base = os.environ.get('FRAUD_BASE_DIR', os.path.abspath('.'))
if _base not in sys.path:
    sys.path.insert(0, _base)

from fraud_config import (
    assign_fraud_tags,
    fraud_signals_from_text,
    FRAUD_FAMILIES,
    FAMILY_LABELS,
)

print('fraud_config loaded ✓')

# ── Helpers ───────────────────────────────────────────────────
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
}

def normalize_space(text):
    return re.sub(r'\s+', ' ', (text or '')).strip()

def stable_doc_id(url):
    return hashlib.sha1(url.encode('utf-8')).hexdigest()[:14]

# ══════════════════════════════════════════════════════════════
# ── STEP 1 ── Discover article URLs from PYMNTS
# ══════════════════════════════════════════════════════════════

TARGET_CATEGORIES = [
    'https://www.pymnts.com/category/fraud-prevention/',
    'https://www.pymnts.com/category/news/security-and-risk/',
    'https://www.pymnts.com/tag/identity-theft/',
    'https://www.pymnts.com/tag/fraud/',
    'https://www.pymnts.com/tag/cybersecurity/',
]

MAX_PAGES = 8

def discover_links(pages_per_cat=MAX_PAGES):
    all_links = []
    seen = set()
    for base_url in TARGET_CATEGORIES:
        cat_name = base_url.split('/')[-2]
        print(f'  Scanning: {cat_name}')
        for page in range(1, pages_per_cat + 1):
            url = f'{base_url}page/{page}/' if page > 1 else base_url
            try:
                res = requests.get(url, headers=HEADERS, timeout=15)
                soup = BeautifulSoup(res.content, 'html.parser')
                links = soup.find_all('a', href=re.compile(r'https://www.pymnts.com/'))
                new = 0
                for l in links:
                    href = l['href']
                    if any(x in href for x in ['/category/', '/tag/', '/author/', '/contact-us/', '/about/']):
                        continue
                    if href not in seen:
                        all_links.append(href)
                        seen.add(href)
                        new += 1
                print(f'    Page {page}: +{new} new (total={len(all_links)})')
                if new == 0:
                    break
                time.sleep(1.0)
            except Exception as e:
                print(f'    Error on {url}: {e}')
                break

    print(f'\nDiscovery complete. Found {len(all_links)} article URLs.')
    return all_links

discovered_urls = discover_links()

# ══════════════════════════════════════════════════════════════
# ── STEP 2 ── Scrape articles + apply canonical tagging
# ══════════════════════════════════════════════════════════════

def scrape_and_tag(url_list):
    results = []
    print(f'Scraping {len(url_list)} articles...')

    for url in tqdm(url_list, desc='Scraping PYMNTS'):
        try:
            res = requests.get(url, headers=HEADERS, timeout=10)
            soup = BeautifulSoup(res.content, 'html.parser')

            # ── Date extraction (3-strategy)
            date_val = None
            time_tag = soup.find('time')
            if time_tag and time_tag.has_attr('datetime'):
                date_val = time_tag['datetime']
            if not date_val:
                meta_date = soup.find('meta', property='article:published_time')
                if meta_date:
                    date_val = meta_date.get('content')
            if not date_val:
                byline = soup.find(class_=re.compile(r'post-date|byline|author', re.I))
                if byline:
                    date_val = byline.get_text(strip=True)
            published_raw = date_val or ''

            # ── Title
            h1 = soup.find('h1')
            title = normalize_space(h1.get_text(strip=True)) if h1 else ''

            # ── Body text
            content_div = soup.find('div', class_='post-content') or soup.find('article')
            if not content_div:
                continue
            body = normalize_space(content_div.get_text(separator=' '))
            body_short = body[:1000]

            # ── Canonical fraud tagging
            fraud_tags    = assign_fraud_tags(title, body)
            fraud_signals = fraud_signals_from_text(f'{title} {body}')

            results.append({
                'doc_id':        stable_doc_id(url),
                'url':           url,
                'title':         title,
                'published_raw': published_raw,
                'body_1':        body,
                'body_short':    body_short,
                'source':        'PYMNTS',
                'fraud_tags':    fraud_tags,
                'fraud_signals': fraud_signals,
            })
            time.sleep(1.2)

        except Exception:
            continue

    return pd.DataFrame(results)

raw_df = scrape_and_tag(discovered_urls)
print(f'\nScraped: {len(raw_df)} articles')
print('Columns:', raw_df.columns.tolist())

# ══════════════════════════════════════════════════════════════
# ── STEP 3 ── Clean + enrich
# ══════════════════════════════════════════════════════════════

df = raw_df.copy()

# Parse dates
df['published']       = pd.to_datetime(df['published_raw'], errors='coerce', utc=True)
df['date']            = df['published'].dt.date.astype(str)
df['published_year']  = df['published'].dt.year
df['published_month'] = df['published'].dt.month

# Deduplicate
print(f'Before dedup: {len(df)}')
df = df.drop_duplicates(subset=['url'],   keep='first')
df = df.drop_duplicates(subset=['title'], keep='first')
df = df[df['body_1'].str.strip().str.len() > 50]
df = df.sort_values('published', ascending=False).reset_index(drop=True)
print(f'After dedup:  {len(df)}')

# Final column selection
final_df = df[[
    'doc_id', 'date', 'published_year', 'published_month',
    'source', 'fraud_tags', 'fraud_signals',
    'title', 'url', 'body_1', 'body_short'
]].copy().reset_index(drop=True)

print('\nFinal shape:', final_df.shape)
print('Tag distribution:')
print(final_df['fraud_tags'].explode().value_counts().head(10))

# ══════════════════════════════════════════════════════════════
# ── STEP 4 ── Export
# ══════════════════════════════════════════════════════════════

jsonl_path = os.path.join(OUTPUT_FOLDER, 'pymnts_master.jsonl')
csv_path   = os.path.join(OUTPUT_FOLDER, 'pymnts_master.csv')

final_df.to_json(jsonl_path, orient='records', lines=True, force_ascii=False)

csv_df = final_df.copy()
csv_df['fraud_tags']    = csv_df['fraud_tags'].apply(json.dumps)
csv_df['fraud_signals'] = csv_df['fraud_signals'].apply(json.dumps)
csv_df.to_csv(csv_path, index=False, encoding='utf-8')

print('Saved:')
print(' ', jsonl_path)
print(' ', csv_path)
print(f'\nTotal records: {len(final_df)}')
print(f'Tagged records: {(final_df["fraud_tags"].apply(lambda x: x != ["other"]).sum())}')
