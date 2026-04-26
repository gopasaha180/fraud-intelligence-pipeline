# =============================================================================
# fraud_config.py — Central Fraud Intelligence Configuration
# =============================================================================
# Single source of truth for:
#   - Fraud tag taxonomy (fraud_dictionary)
#   - Fraud signals dictionary (fraud_signals)
#   - Severity ranking for primary_tag tiebreaking
#   - Tagging helper functions shared across all pipelines
#
# Usage in any pipeline notebook:
#   import sys, os
#   sys.path.insert(0, os.environ.get('FRAUD_BASE_DIR', '..'))
#   from fraud_config import (
#       FRAUD_TAG_RULES, FRAUD_DICTIONARY, FRAUD_SIGNALS,
#       SEVERITY_RANK, assign_fraud_tags, assign_primary_tag,
#       fraud_signals_from_text
#   )
# =============================================================================

import re
from urllib.parse import urlparse

# =============================================================================
# 1. FRAUD TAG TAXONOMY — Canonical fraud_dictionary v2 (extended)
#    Format: list of (keyword, fraud_family) tuples
#    Used by: FinCEN, FBI, IC3 (keyword-lookup style)
# =============================================================================

FRAUD_DICTIONARY = [
    # ── Money laundering ─────────────────────────────────────────────────────
    ("shell company",              "money_laundering"),
    ("shell companies",            "money_laundering"),
    ("shell corporation",          "money_laundering"),
    ("front company",              "money_laundering"),
    ("money mule",                 "money_laundering"),
    ("mule account",               "money_laundering"),
    ("layering",                   "money_laundering"),
    ("structuring",                "money_laundering"),
    ("smurfing",                   "money_laundering"),
    ("placement",                  "money_laundering"),
    ("beneficial owner",           "money_laundering"),

    # ── Check fraud ──────────────────────────────────────────────────────────
    ("check fraud",                "check_fraud"),
    ("mail theft",                 "check_fraud"),
    ("stolen check",               "check_fraud"),
    ("stolen checks",              "check_fraud"),
    ("check washing",              "check_fraud"),
    ("fraudulent check",           "check_fraud"),
    ("altered check",              "check_fraud"),
    ("forged check",               "check_fraud"),

    # ── Sanctions ────────────────────────────────────────────────────────────
    ("sanctions evasion",          "sanctions"),
    ("sanctioned entity",          "sanctions"),
    ("export control violation",   "sanctions"),

    # ── Terrorist financing ──────────────────────────────────────────────────
    ("terrorist financing",        "terrorist_financing"),
    ("terrorist organization",     "terrorist_financing"),

    # ── Human trafficking ────────────────────────────────────────────────────
    ("human trafficking",          "human_trafficking"),
    ("labor trafficking",          "human_trafficking"),

    # ── Consumer fraud ───────────────────────────────────────────────────────
    ("romance scam",               "consumer_fraud"),
    ("lottery scam",               "consumer_fraud"),
    ("charity fraud",              "consumer_fraud"),
    ("investment scam",            "consumer_fraud"),
    ("scam",                       "consumer_fraud"),
    ("fraudster",                  "consumer_fraud"),
    ("gift card scam",             "consumer_fraud"),
    ("imposter scam",              "consumer_fraud"),
    ("advance fee",                "consumer_fraud"),
    ("government impersonator",    "consumer_fraud"),
    ("business impersonator",      "consumer_fraud"),

    # ── Identity fraud ───────────────────────────────────────────────────────
    ("identity theft",             "identity_fraud"),
    ("stolen identity",            "identity_fraud"),
    ("synthetic identity",         "identity_fraud"),
    ("account takeover",           "identity_fraud"),
    ("identity document",          "identity_fraud"),
    ("credential theft",           "identity_fraud"),
    ("ssn",                        "identity_fraud"),

    # ── Benefits fraud ───────────────────────────────────────────────────────
    ("government benefits fraud",  "benefits_fraud"),
    ("medicaid fraud",             "benefits_fraud"),
    ("pandemic relief fraud",      "benefits_fraud"),
    ("unemployment fraud",         "benefits_fraud"),

    # ── Cybercrime ───────────────────────────────────────────────────────────
    ("ransomware",                 "cybercrime"),
    ("phishing",                   "cybercrime"),
    ("smishing",                   "cybercrime"),
    ("vishing",                    "cybercrime"),
    ("malware",                    "cybercrime"),
    ("data breach",                "cybercrime"),
    ("credential stuffing",        "cybercrime"),
    ("social engineering",         "cybercrime"),
    ("business email compromise",  "cybercrime"),
    ("bec",                        "cybercrime"),
    ("spear phishing",             "cybercrime"),
    ("dark web",                   "cybercrime"),

    # ── Crypto fraud ─────────────────────────────────────────────────────────
    ("virtual currency",           "crypto_fraud"),
    ("cryptocurrency",             "crypto_fraud"),
    ("crypto exchange",            "crypto_fraud"),
    ("crypto wallet",              "crypto_fraud"),
    ("pig butchering",             "crypto_fraud"),
    ("wallet address",             "crypto_fraud"),
    ("bitcoin",                    "crypto_fraud"),
    ("ethereum",                   "crypto_fraud"),
    ("rug pull",                   "crypto_fraud"),
    ("wallet",                     "crypto_fraud"),

    # ── NEW: Data privacy ────────────────────────────────────────────────────
    ("personal data",              "data_privacy"),
    ("coppa",                      "data_privacy"),
    ("privacy rule",               "data_privacy"),
    ("privacy violation",          "data_privacy"),
    ("unauthorized disclosure",    "data_privacy"),
    ("sensitive data",             "data_privacy"),
    ("pii",                        "data_privacy"),
    ("data protection",            "data_privacy"),
    ("personally identifiable",    "data_privacy"),

    # ── NEW: AI fraud ────────────────────────────────────────────────────────
    ("prompt injection",           "ai_fraud"),
    ("deepfake",                   "ai_fraud"),
    ("ai scam",                    "ai_fraud"),
    ("ai-generated fraud",         "ai_fraud"),
    ("llm exploit",                "ai_fraud"),
    ("ai recommendation poisoning","ai_fraud"),
    ("hidden prompt",              "ai_fraud"),
    ("generative ai fraud",        "ai_fraud"),

    # ── NEW: Drug trafficking ────────────────────────────────────────────────
    ("fentanyl",                   "drug_trafficking"),
    ("narcotic",                   "drug_trafficking"),
    ("cocaine",                    "drug_trafficking"),
    ("heroin",                     "drug_trafficking"),
    ("opioid",                     "drug_trafficking"),
    ("drug distribution",          "drug_trafficking"),
    ("drug trafficking",           "drug_trafficking"),
    ("controlled substance",       "drug_trafficking"),

    # ── NEW: Securities fraud ────────────────────────────────────────────────
    ("ponzi",                      "securities_fraud"),
    ("investment fraud",           "securities_fraud"),
    ("securities fraud",           "securities_fraud"),
    ("hedge fund fraud",           "securities_fraud"),
    ("stock manipulation",         "securities_fraud"),
    ("insider trading",            "securities_fraud"),
    ("pyramid scheme",             "securities_fraud"),
    ("unregistered securities",    "securities_fraud"),
]


# =============================================================================
# 2. FRAUD TAG RULES — Regex style (used by FTC, FBI doc-level, BleepingComputer)
#    Format: list of (fraud_family, regex_pattern) tuples
#    Compiled once at import time for performance.
# =============================================================================

FRAUD_TAG_RULES = [
    ("money_laundering",    r"\b(shell company|shell companies|front company|money mule|mule account|layering|structuring|smurfing|beneficial owner)\b"),
    ("check_fraud",         r"\b(check fraud|mail theft|stolen check|stolen checks|check washing|fraudulent check|altered check|forged check)\b"),
    ("sanctions",           r"\b(sanctions evasion|sanctioned entity|export control violation)\b"),
    ("terrorist_financing", r"\b(terrorist financing|terrorist organization)\b"),
    ("human_trafficking",   r"\b(human trafficking|labor trafficking)\b"),
    ("consumer_fraud",      r"\b(romance scam|lottery scam|charity fraud|investment scam|advance fee|imposter|government impersonator|gift card scam|scam|fraudster)\b"),
    ("identity_fraud",      r"\b(identity theft|stolen identity|synthetic identity|ssn|account takeover|credential theft)\b"),
    ("benefits_fraud",      r"\b(government benefits fraud|medicaid fraud|pandemic relief fraud|unemployment fraud)\b"),
    ("cybercrime",          r"\b(ransomware|phishing|smishing|vishing|malware|data breach|business email compromise|bec|spear phishing|dark web|credential stuffing|social engineering)\b"),
    ("crypto_fraud",        r"\b(virtual currency|cryptocurrency|crypto exchange|pig butchering|crypto|bitcoin|ethereum|wallet address|rug pull|crypto wallet)\b"),
    # ── NEW families ──────────────────────────────────────────────────────────
    ("data_privacy",        r"\b(personal data|coppa|privacy rule|privacy violation|unauthorized disclosure|sensitive data|pii|data protection|personally identifiable)\b"),
    ("ai_fraud",            r"\b(prompt injection|deepfake|ai scam|ai-generated fraud|llm exploit|ai recommendation poisoning|hidden prompt|generative ai fraud)\b"),
    ("drug_trafficking",    r"\b(fentanyl|narcotic|cocaine|heroin|opioid|drug distribution|drug trafficking|controlled substance)\b"),
    ("securities_fraud",    r"\b(ponzi|investment fraud|securities fraud|hedge fund fraud|stock manipulation|insider trading|pyramid scheme|unregistered securities)\b"),
]

# Pre-compile all regex patterns once
_COMPILED_TAG_RULES = [(family, re.compile(pattern, re.IGNORECASE)) for family, pattern in FRAUD_TAG_RULES]


# =============================================================================
# 3. FRAUD SIGNALS DICTIONARY — Canonical v2
#    Format: list of (signal_keyword, signal_category) tuples
# =============================================================================

FRAUD_SIGNALS = [
    # Entity / organization
    ("shell company",              "entity_signal"),
    ("shell companies",            "entity_signal"),
    ("front company",              "entity_signal"),
    ("beneficial owner",           "ownership_signal"),
    ("ultimate beneficial owner",  "ownership_signal"),
    ("ubo",                        "ownership_signal"),
    # Transaction patterns
    ("money mule",                 "transaction_signal"),
    ("money mules",                "transaction_signal"),
    ("mule account",               "transaction_signal"),
    ("layering",                   "transaction_signal"),
    ("structuring",                "transaction_signal"),
    ("smurfing",                   "transaction_signal"),
    ("round dollar transactions",  "transaction_signal"),
    ("rapid movement of funds",    "transaction_signal"),
    # Payment methods
    ("wire transfer",              "payment_signal"),
    ("international wire",         "payment_signal"),
    ("cash withdrawal",            "payment_signal"),
    ("atm withdrawal",             "payment_signal"),
    ("prepaid card",               "payment_signal"),
    ("gift card",                  "payment_signal"),
    ("zelle",                      "payment_signal"),
    ("venmo",                      "payment_signal"),
    ("cash app",                   "payment_signal"),
    ("paypal",                     "payment_signal"),
    ("peer-to-peer payment",       "payment_signal"),
    ("p2p payment",                "payment_signal"),
    # Crypto signals
    ("cryptocurrency",             "crypto_signal"),
    ("virtual currency",           "crypto_signal"),
    ("crypto exchange",            "crypto_signal"),
    ("crypto wallet",              "crypto_signal"),
    ("digital wallet",             "crypto_signal"),
    ("wallet address",             "crypto_signal"),
    ("bitcoin address",            "crypto_signal"),
    ("ethereum address",           "crypto_signal"),
    ("pig butchering",             "crypto_signal"),
    ("rug pull",                   "crypto_signal"),
    # Identity / account abuse
    ("identity theft",             "identity_signal"),
    ("stolen identity",            "identity_signal"),
    ("synthetic identity",         "identity_signal"),
    ("account takeover",           "identity_signal"),
    ("credential theft",           "identity_signal"),
    ("compromised account",        "identity_signal"),
    ("unauthorized access",        "identity_signal"),
    # Cyber / attack methods
    ("phishing",                   "cyber_signal"),
    ("smishing",                   "cyber_signal"),
    ("vishing",                    "cyber_signal"),
    ("malware",                    "cyber_signal"),
    ("ransomware",                 "cyber_signal"),
    ("data breach",                "cyber_signal"),
    ("credential stuffing",        "cyber_signal"),
    ("social engineering",         "cyber_signal"),
    ("remote access",              "cyber_signal"),
    ("trojan",                     "cyber_signal"),
    # Document / claim fraud
    ("false claim",                "document_signal"),
    ("false claims",               "document_signal"),
    ("fraudulent invoice",         "document_signal"),
    ("fake documentation",         "document_signal"),
    ("forged document",            "document_signal"),
    ("fabricated records",         "document_signal"),
    # Scam behavior
    ("imposter",                   "scam_signal"),
    ("impersonation",              "scam_signal"),
    ("romance scam",               "scam_signal"),
    ("investment scam",            "scam_signal"),
    ("lottery scam",               "scam_signal"),
    ("gift card scam",             "scam_signal"),
    ("advance fee",                "scam_signal"),
    ("urgent payment request",     "scam_signal"),
    # Contact / channel signals
    ("email address",              "contact_signal"),
    ("phone number",               "contact_signal"),
    ("text message",               "contact_signal"),
    ("sms message",                "contact_signal"),
    ("telegram",                   "contact_signal"),
    ("whatsapp",                   "contact_signal"),
]


# =============================================================================
# 4. SEVERITY RANKING — Used to break ties when a doc has multiple fraud tags
#    Lower index = higher severity = wins the primary_tag slot
# =============================================================================

SEVERITY_RANK = [
    "terrorist_financing",
    "human_trafficking",
    "drug_trafficking",      # new
    "money_laundering",
    "sanctions",
    "securities_fraud",      # new
    "crypto_fraud",
    "cybercrime",
    "ai_fraud",              # new
    "check_fraud",
    "identity_fraud",
    "consumer_fraud",
    "benefits_fraud",
    "data_privacy",          # new
    "other",
]

_SEVERITY_INDEX = {tag: i for i, tag in enumerate(SEVERITY_RANK)}


# =============================================================================
# 5. TAGGING FUNCTIONS — Drop-in replacements for per-pipeline implementations
# =============================================================================

def assign_fraud_tags(title: str, body: str) -> list:
    """
    Regex-based tagger. Returns a sorted list of matching fraud family tags.
    Falls back to ['other'] if nothing matches.
    Used by: FTC, FBI (doc-level), BleepingComputer, Outseer, PYMNTS, IC3
    """
    text = f"{title} {body}".lower()
    tags = [family for family, pattern in _COMPILED_TAG_RULES if pattern.search(text)]
    return sorted(set(tags)) if tags else ["other"]


def assign_fraud_tags_from_keywords(text: str, fraud_keywords: dict) -> list:
    """
    Keyword-lookup tagger (used by FinCEN / chunk-level tagging).
    fraud_keywords: dict of {keyword: fraud_family} built from FRAUD_DICTIONARY.
    Returns sorted list of matching families, or ['other'].
    """
    text_lower = text.lower()
    tags = sorted({
        family
        for keyword, family in fraud_keywords.items()
        if keyword in text_lower
    })
    return tags if tags else ["other"]


def assign_primary_tag(fraud_tags: list) -> str:
    """
    Picks the single most-severe tag from a list of fraud tags.
    Uses SEVERITY_RANK for tiebreaking — lower index wins.
    Falls back to 'other' if the list is empty or unrecognized.
    """
    if not fraud_tags:
        return "other"
    return min(fraud_tags, key=lambda t: _SEVERITY_INDEX.get(t, len(SEVERITY_RANK)))


def build_fraud_keywords() -> dict:
    """
    Builds the keyword-lookup dict from FRAUD_DICTIONARY.
    Convenience function for pipelines that use the keyword-in-text style.
    Returns: {keyword_str: fraud_family_str}
    """
    return {kw.strip().lower(): family.strip() for kw, family in FRAUD_DICTIONARY}


def build_signal_keywords() -> dict:
    """
    Builds the signal keyword-lookup dict from FRAUD_SIGNALS.
    Returns: {signal_keyword_str: signal_category_str}
    """
    return {kw.strip().lower(): cat.strip() for kw, cat in FRAUD_SIGNALS}


# =============================================================================
# 6. FRAUD SIGNALS EXTRACTION — Regex extractors (URLs, emails, phones, etc.)
#    Identical implementation used across all pipelines.
# =============================================================================

_URL_RE   = re.compile(r"\bhttps?://[^\s)>\]]+", re.IGNORECASE)
_EMAIL_RE = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.IGNORECASE)
_PHONE_RE = re.compile(
    r"(?:(?:\+?1[\s\-\.])?)(?:\(?\d{3}\)?[\s\-\.]?)\d{3}[\s\-\.]?\d{4}\b"
)
_IP_RE    = re.compile(
    r"\b(?:(?:25[0-5]|2[0-4]\d|[01]?\d?\d)\.){3}(?:25[0-5]|2[0-4]\d|[01]?\d?\d)\b"
)
_BTC_RE   = re.compile(r"\b(bc1[0-9a-z]{25,62}|[13][a-km-zA-HJ-NP-Z1-9]{25,34})\b")
_ETH_RE   = re.compile(r"\b0x[a-fA-F0-9]{40}\b")


def _extract_domains(urls: list) -> list:
    domains = set()
    for u in urls:
        try:
            netloc = urlparse(u).netloc.lower()
            if netloc:
                domains.add(netloc)
        except Exception:
            pass
    return sorted(domains)


def fraud_signals_from_text(text: str) -> dict:
    """
    Extracts structured fraud signals (URLs, emails, phones, IPs, crypto wallets)
    from raw text. Returns a dict with six keys.
    """
    text  = text or ""
    urls  = _URL_RE.findall(text)
    btc   = _BTC_RE.findall(text)
    eth   = _ETH_RE.findall(text)
    return {
        "urls":           sorted(set(urls)),
        "domains":        _extract_domains(urls),
        "emails":         sorted({e.lower() for e in _EMAIL_RE.findall(text)}),
        "phones":         sorted({re.sub(r"\s+", " ", p).strip() for p in _PHONE_RE.findall(text)}),
        "ip_addresses":   sorted(set(_IP_RE.findall(text))),
        "crypto_wallets": sorted(set(btc + eth)),
    }


# =============================================================================
# 7. TAXONOMY METADATA — For documentation and dashboard labels
# =============================================================================

FRAUD_FAMILIES = [
    "money_laundering",
    "check_fraud",
    "sanctions",
    "terrorist_financing",
    "human_trafficking",
    "consumer_fraud",
    "identity_fraud",
    "benefits_fraud",
    "cybercrime",
    "crypto_fraud",
    "data_privacy",       # new
    "ai_fraud",           # new
    "drug_trafficking",   # new
    "securities_fraud",   # new
    "other",
]

FAMILY_LABELS = {
    "money_laundering":   "Money Laundering",
    "check_fraud":        "Check Fraud",
    "sanctions":          "Sanctions Evasion",
    "terrorist_financing":"Terrorist Financing",
    "human_trafficking":  "Human Trafficking",
    "consumer_fraud":     "Consumer Fraud",
    "identity_fraud":     "Identity Fraud",
    "benefits_fraud":     "Benefits Fraud",
    "cybercrime":         "Cybercrime",
    "crypto_fraud":       "Crypto Fraud",
    "data_privacy":       "Data Privacy",
    "ai_fraud":           "AI Fraud",
    "drug_trafficking":   "Drug Trafficking",
    "securities_fraud":   "Securities Fraud",
    "other":              "Other / Unclassified",
}
