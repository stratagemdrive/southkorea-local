"""
fetch_news.py
Fetches South Korea news headlines from public RSS feeds, translates Korean
content to English, categorises stories, and writes docs/southkorea_news.json.
No external APIs are used.
"""

import json
import os
import hashlib
import logging
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime

import feedparser
from deep_translator import GoogleTranslator
from langdetect import detect, LangDetectException
from dateutil import parser as dateutil_parser

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

OUTPUT_DIR = "docs"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "southkorea_news.json")
MAX_PER_CATEGORY = 20
MAX_AGE_DAYS = 7
COUNTRY = "southkorea"

CATEGORIES = ["Diplomacy", "Military", "Energy", "Economy", "Local Events"]

# Keywords used to assign categories (checked against title + description)
CATEGORY_KEYWORDS = {
    "Diplomacy": [
        "diplomat", "foreign minister", "summit", "treaty", "sanctions",
        "bilateral", "embassy", "ambassador", "united nations", "UN",
        "relations", "foreign policy", "alliance", "talks", "negotiat",
        "visit", "agreement", "pact", "cooperation", "international",
        "G20", "G7", "ASEAN", "Korea-US", "ROK-US", "Korea-Japan",
        "Korea-China", "North Korea", "DPRK", "denucleariz",
    ],
    "Military": [
        "military", "army", "navy", "air force", "defense", "defence",
        "weapon", "missile", "nuclear", "drill", "exercise", "troops",
        "soldier", "combat", "warship", "fighter jet", "THAAD",
        "DMZ", "demilitarized", "armistice", "Korean War",
        "joint exercise", "deterrence", "marine", "conscript", "enlist",
    ],
    "Energy": [
        "energy", "nuclear power", "reactor", "electricity", "oil",
        "gas", "LNG", "renewable", "solar", "wind power", "coal",
        "fuel", "power plant", "grid", "emissions", "carbon",
        "hydrogen", "EV", "electric vehicle", "battery", "semiconductor",
        "chip", "KEPCO", "petrol", "refinery",
    ],
    "Economy": [
        "economy", "economic", "GDP", "growth", "trade", "export",
        "import", "tariff", "market", "stock", "won", "currency",
        "inflation", "interest rate", "bank", "finance", "investment",
        "budget", "fiscal", "chaebol", "Samsung", "Hyundai", "LG",
        "SK", "Lotte", "Kakao", "Naver", "startup", "industry",
        "manufacturing", "jobs", "unemployment", "IMF", "WTO",
    ],
    "Local Events": [
        "Seoul", "Busan", "Incheon", "Daegu", "Gwangju", "Daejeon",
        "Ulsan", "Jeju", "provincial", "municipal", "city", "district",
        "festival", "culture", "weather", "earthquake", "flood",
        "fire", "accident", "police", "crime", "court", "health",
        "hospital", "school", "education", "sport", "K-pop",
        "entertainment", "tourism", "local", "community", "election",
        "rally", "protest", "residents",
    ],
}

# RSS feed list: (source_name, feed_url)
# Sources originally requested, with two substitutions noted:
#   Korea Pro   -> NK News         (paywalled; NK News has full public RSS)
#   Korea Exposé-> KBS World Radio (relaunched as paid newsletter; no public RSS)
RSS_SOURCES = [
    ("Korea Herald",         "https://www.koreaherald.com/rss/000000000001"),
    ("Korea JoongAng Daily", "https://koreajoongangdaily.joins.com/rss/news"),
    ("The Korea Times",      "https://www.koreatimes.co.kr/www2/common/rss.asp"),
    ("Yonhap News Agency",   "https://en.yna.co.kr/RSS/news.xml"),
    ("Arirang News",         "https://www.arirang.com/rss/rss_news.aspx"),
    ("Hankyoreh",            "https://english.hani.co.kr/rss/english/"),
    ("NK News",              "https://www.nknews.org/feed/"),           # replaces Korea Pro
    ("Daily NK",             "https://www.dailynk.com/english/feed/"),
    ("KBS World Radio",      "https://world.kbs.co.kr/rss/rss_english.htm"),  # replaces Korea Exposé
    ("Business Korea",       "https://www.businesskorea.co.kr/rss/allEn.xml"),
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

translator = GoogleTranslator(source="auto", target="en")

CUTOFF = datetime.now(timezone.utc) - timedelta(days=MAX_AGE_DAYS)


def safe_translate(text: str) -> str:
    """Translate text to English if it appears to be Korean; return as-is otherwise."""
    if not text or not text.strip():
        return text
    try:
        lang = detect(text)
    except LangDetectException:
        lang = "unknown"
    if lang == "ko":
        try:
            return translator.translate(text[:4900])  # GoogleTranslator limit ~5000 chars
        except Exception as exc:
            logger.warning("Translation failed: %s", exc)
    return text


def parse_date(entry) -> datetime | None:
    """Extract a timezone-aware datetime from a feedparser entry."""
    for attr in ("published_parsed", "updated_parsed"):
        val = getattr(entry, attr, None)
        if val:
            try:
                return datetime(*val[:6], tzinfo=timezone.utc)
            except Exception:
                pass
    for attr in ("published", "updated"):
        val = getattr(entry, attr, None)
        if val:
            try:
                return parsedate_to_datetime(val).astimezone(timezone.utc)
            except Exception:
                pass
            try:
                return dateutil_parser.parse(val).astimezone(timezone.utc)
            except Exception:
                pass
    return None


def make_id(url: str, title: str) -> str:
    return hashlib.md5(f"{url}|{title}".encode()).hexdigest()


def categorise(title: str, description: str) -> str:
    """Return the best-matching category or 'Local Events' as default."""
    combined = f"{title} {description}".lower()
    scores = {cat: 0 for cat in CATEGORIES}
    for cat, keywords in CATEGORY_KEYWORDS.items():
        for kw in keywords:
            if kw.lower() in combined:
                scores[cat] += 1
    best = max(scores, key=lambda c: scores[c])
    if scores[best] == 0:
        return "Local Events"
    return best


def fetch_feed(source_name: str, url: str) -> list[dict]:
    """Fetch and parse one RSS feed, returning a list of story dicts."""
    logger.info("Fetching %s ...", source_name)
    try:
        feed = feedparser.parse(url)
    except Exception as exc:
        logger.error("Could not fetch %s: %s", source_name, exc)
        return []

    stories = []
    for entry in feed.entries:
        pub_dt = parse_date(entry)
        if pub_dt is None or pub_dt < CUTOFF:
            continue

        raw_title = getattr(entry, "title", "") or ""
        raw_desc = getattr(entry, "summary", "") or ""
        link = getattr(entry, "link", "") or ""

        title = safe_translate(raw_title.strip())
        # Description used only for categorisation; not stored in output
        desc_for_cat = safe_translate(raw_desc.strip()[:500])

        category = categorise(title, desc_for_cat)

        stories.append({
            "id": make_id(link, raw_title),
            "title": title,
            "source": source_name,
            "url": link,
            "published_date": pub_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "category": category,
        })

    logger.info("  -> %d recent stories from %s", len(stories), source_name)
    return stories


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def load_existing() -> dict[str, list[dict]]:
    """Load existing JSON output, keyed by category."""
    if not os.path.exists(OUTPUT_FILE):
        return {cat: [] for cat in CATEGORIES}
    with open(OUTPUT_FILE, encoding="utf-8") as f:
        data = json.load(f)
    # Normalise: expect {"category": [...], ...} at top level
    result = {}
    for cat in CATEGORIES:
        result[cat] = data.get(cat, [])
    return result


def merge(existing: dict[str, list[dict]], fresh: list[dict]) -> dict[str, list[dict]]:
    """
    Merge fresh stories into existing buckets per category.
    - Deduplicate by id.
    - Keep stories <= MAX_AGE_DAYS old.
    - Keep newest MAX_PER_CATEGORY per category, replacing oldest first.
    """
    # Build lookup of existing ids to avoid duplicates
    existing_ids: set[str] = set()
    for stories in existing.values():
        for s in stories:
            existing_ids.add(s["id"])

    # Add fresh stories not already present
    for story in fresh:
        if story["id"] not in existing_ids:
            cat = story["category"]
            existing[cat].append(story)
            existing_ids.add(story["id"])

    # Per category: drop stale, sort by date descending, trim to MAX_PER_CATEGORY
    cutoff_str = CUTOFF.strftime("%Y-%m-%dT%H:%M:%SZ")
    for cat in CATEGORIES:
        stories = [s for s in existing[cat] if s.get("published_date", "") >= cutoff_str]
        stories.sort(key=lambda s: s.get("published_date", ""), reverse=True)
        existing[cat] = stories[:MAX_PER_CATEGORY]

    return existing


def save(data: dict[str, list[dict]]) -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output = {
        "country": COUNTRY,
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "categories": CATEGORIES,
    }
    output.update(data)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    logger.info("Written %s", OUTPUT_FILE)


def main():
    existing = load_existing()

    all_fresh: list[dict] = []
    for source_name, url in RSS_SOURCES:
        all_fresh.extend(fetch_feed(source_name, url))

    logger.info("Total fresh stories collected: %d", len(all_fresh))

    merged = merge(existing, all_fresh)
    save(merged)

    for cat in CATEGORIES:
        logger.info("  %-16s : %d stories", cat, len(merged[cat]))


if __name__ == "__main__":
    main()
