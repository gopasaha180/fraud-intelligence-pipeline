# Fraud Intelligence Aggregation System

**DSBA 6390 Practicum — UNC Charlotte × USAA**

A multi-source fraud intelligence pipeline that ingests publicly available data from seven sources, normalizes each into a canonical schema, applies a two-stage fraud taxonomy tagging layer, and produces aggregated outputs for dashboard consumption.

---

## Key Features

- Multi-source data ingestion (FinCEN, FTC, IC3, FBI, Reddit, industry reports)
- Data normalization into a unified schema
- NLP-based fraud detection and classification
- Fraud signal extraction (emails, URLs, IPs, crypto wallets)
- Aggregated outputs for analytics and visualization
- Streamlit-ready data pipeline for dashboard integration

---

## Pipeline Architecture
1. Data Ingestion
Collects data from multiple structured and unstructured sources
2. Data Processing & Cleaning
Normalizes formats (PDF, HTML, JSON)
3. Fraud Detection Layer
Applies taxonomy-based tagging and classification
4. Signal Extraction
Extracts key fraud indicators (emails, IPs, URLs, crypto wallets)
5. Output Generation
Produces structured datasets for analytics and dashboards

---

## Business Impact
- Enables centralized fraud intelligence across multiple sources
- Improves fraud detection consistency through standardized taxonomy
- Supports decision-making via aggregated fraud insights

---

## My Contribution
- Developed fraud detection logic and signal extraction pipeline
- Designed unified fraud taxonomy and schema across sources
- Integrated multi-source data pipelines into a cohesive system
- Contributed to Git-based collaboration and project deployment

---

## Folder Structure

```
fraud_project/                  ← FRAUD_BASE_DIR points here
│
├── fraud_config.py             ← Central config: taxonomy, tag rules, signals, helpers
│
├── FinCen.ipynb                ← FinCEN advisories, alerts, notices (PDF + HTML)
├── FTC.ipynb                   ← FTC Consumer Alerts (web scrape)
├── FBI.ipynb                   ← FBI press releases (RSS + Wayback CDX)
├── IC3.ipynb                   ← IC3 PSAs + annual crime report PDFs
├── BleepingComputer.ipynb      ← BleepingComputer security articles (paginated scrape)
├── Outseer.ipynb               ← Outseer fraud intelligence blog + reports
├── PYMNTS.ipynb                ← PYMNTS fraud/payments news
│
├── fraud_detector.ipynb        ← Stage 2: EDA, trend analysis, dashboard exports
├── orchestrator.ipynb          ← Runs all pipelines + detector in sequence
│
├── data/
│   ├── metadata/               ← Per-source listing CSVs (article URL inventories)
│   ├── fulltext/               ← Raw scraped article text (JSONL)
│   ├── raw_pdfs/               ← Downloaded PDFs (FinCEN, IC3 annual reports)
│   └── dictionaries/           ← Serialized fraud keyword/signal dictionaries
│
└── outputs/                    ← Final pipeline outputs (read by fraud_detector)
    ├── fincen_tagged_chunks.jsonl
    ├── ftc_master.jsonl
    ├── fbi_tagged_chunks.jsonl
    ├── ic3_tagged_chunks.jsonl
    ├── bleepingcomputer_fraud_data.csv
    ├── outseer_scraped_data.jsonl
    ├── pymnts_master.jsonl
    ├── master_flat_docs.csv        ← One row per document, all sources merged
    └── dashboard_exports/          ← CSVs for dashboard consumption
```

---

## Setup

### 1. Prerequisites

**Python 3.10+** and the following system packages:

```bash
# Ubuntu / Debian
sudo apt-get install poppler-utils tesseract-ocr

# macOS (Homebrew)
brew install poppler tesseract

# Windows — install manually:
#   Tesseract: https://github.com/UB-Mannheim/tesseract/wiki
#   Poppler:   https://github.com/oschwartz10612/poppler-windows/releases
```

### 2. Python dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure the base path

All notebooks read a single environment variable: **`FRAUD_BASE_DIR`**.

**Option A — Set the environment variable (recommended):**

```bash
# Windows (Command Prompt)
set FRAUD_BASE_DIR=C:\Users\YourName\fraud_project

# Windows (PowerShell)
$env:FRAUD_BASE_DIR = "C:\Users\YourName\fraud_project"

# macOS / Linux
export FRAUD_BASE_DIR=/home/yourname/fraud_project
```

**Option B — Edit the fallback path in any notebook:**

Each notebook has a path config cell at the top. The fallback is the only line you need to change:

```python
BASE_DIR = os.environ.get(
    'FRAUD_BASE_DIR',
    r'C:\Users\YourName\fraud_project'   # <-- update this
)
```

**Option C — Use a `.env` file with `python-dotenv`:**

Create a `.env` file in the project root:
```
FRAUD_BASE_DIR=C:\Users\YourName\fraud_project
```

Then add to the top of any notebook's path config cell:
```python
from dotenv import load_dotenv
load_dotenv()
```

---

## Running the System

### Run everything via the orchestrator (recommended)

Open `orchestrator.ipynb` in VS Code, configure Step 2, and run all cells:

```
Step 0 — Install dependencies     (pip + system packages)
Step 2 — Configuration            ← Edit here: BASE_DIR, FORCE_RERUN, PIPELINES_TO_RUN
Step 3 — Pre-flight check         Verifies notebooks exist, shows what will run
Step 4 — Run pipelines            Executes each enabled pipeline via nbconvert
Step 5 — Sync outputs             Confirms all output files are present
Step 6 — Run fraud detector       Produces EDA + dashboard_exports/
```

Key orchestrator settings in Step 2:

| Setting | Description |
|---|---|
| `FORCE_RERUN = True` | Re-scrape all sources from scratch |
| `FORCE_RERUN = False` | Skip pipelines whose output file already exists |
| `PIPELINES_TO_RUN` | Dict of `source_name: True/False` to enable/disable individual pipelines |
| `PIPELINE_TIMEOUTS` | Per-pipeline timeout in seconds (FBI and BleepingComputer need extended timeouts) |

### Run individual pipelines

Open any pipeline notebook directly in VS Code and run all cells top to bottom. Ensure `FRAUD_BASE_DIR` is set first (or the fallback path is updated).

---

## Fraud Taxonomy

The system classifies documents into 10 fraud families using regex tag rules and keyword matching defined in `fraud_config.py`.

| Family | Description |
|---|---|
| `money_laundering` | Shell companies, money mules, structuring, layering |
| `check_fraud` | Mail theft, check washing, forged/altered checks |
| `sanctions` | Sanctions evasion, export control violations |
| `terrorist_financing` | Terrorist financing and organization activity |
| `human_trafficking` | Labor and human trafficking |
| `consumer_fraud` | Romance scams, imposter scams, gift card fraud |
| `identity_fraud` | Identity theft, account takeover, synthetic identity |
| `benefits_fraud` | Unemployment fraud, Medicaid fraud, pandemic relief fraud |
| `cybercrime` | Ransomware, phishing, data breaches, malware |
| `crypto_fraud` | Crypto investment fraud, rug pulls, wallet theft |

Each document receives:
- `fraud_tags` — list of all matching families
- `fraud_signals` — extracted IOCs (URLs, emails, phone numbers, IPs, crypto addresses)
- `primary_tag` — highest-severity tag per document (severity order defined in `SEVERITY_RANK`)

---

## Canonical Output Schema

All pipelines produce output conforming to this schema:

| Column | Type | Description |
|---|---|---|
| `doc_id` | str | Stable SHA1-based ID (14 chars), keyed on URL |
| `date` | str | Publication date (YYYY-MM-DD) |
| `source` | str | Source name (e.g., `FinCEN`, `FTC`, `FBI`) |
| `fraud_tags` | list | Matching fraud family names |
| `fraud_signals` | dict | Extracted IOCs by category |
| `title` | str | Article/document title |
| `body_1` | str | Full or first-chunk body text |
| `url` | str | Source URL |

FinCEN and FBI produce multi-chunk output (`body_1`, `body_2`, ...) for long PDFs. The master flat output includes all chunks as additional columns.

---

## Key Design Decisions

**`fraud_config.py` is the single source of truth.** All taxonomy, tag rules, signals, and helper functions live here. To modify fraud categories or add keywords, edit only this file — all pipelines inherit changes automatically.

**FBI uses RSS + Wayback CDX, not HTML scraping.** fbi.gov blocks datacenter IPs via Cloudflare. RSS feeds return current articles; the Wayback CDX API supplements historical depth without triggering blocks.

**Checkpoint/resume on `doc_id`.** Long-running pipelines (FBI, BleepingComputer) check for existing `doc_id` entries before fetching, enabling safe interruption and continuation.

**Per-pipeline timeouts in the orchestrator.** FBI (~37 min for 1,500 Wayback fetches) and BleepingComputer (paginated archive scraping) require extended timeouts. These are configured individually in `PIPELINE_TIMEOUTS`, not as a global value.

---

## Source Reference

| Source | Ingestion method | Output file |
|---|---|---|
| FinCEN | PDF download + HTML scrape | `fincen_tagged_chunks.jsonl` |
| FTC | HTML scrape (Consumer Alerts) | `ftc_master.jsonl` |
| FBI | RSS feeds + Wayback CDX | `fbi_tagged_chunks.jsonl` |
| IC3 | HTML scrape (PSAs) + PDF download | `ic3_tagged_chunks.jsonl` |
| BleepingComputer | Paginated tag archive scrape + Wayback CDX | `bleepingcomputer_fraud_data.csv` |
| Outseer | Blog/press release scrape + Wayback CDX | `outseer_scraped_data.jsonl` |
| PYMNTS | Category/tag page scrape | `pymnts_master.jsonl` |

---

## Contact

DSBA 6390 Practicum Team — UNC Charlotte  
Project partner: USAA
