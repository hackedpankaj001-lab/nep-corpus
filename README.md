# Nepali Corpus Scrapers

Scripts to scrape Nepali text from government websites and news sources. Designed for building training corpora for LLM fine-tuning.

## What's Included

| Script | Sources | Language | Articles/Run |
|--------|---------|----------|-------------|
| `govt_scraper.py` | 17 Nepal government ministries | Mixed (EN/NE) | ~200-500 |
| `news_rss_scraper.py` | 18 RSS news feeds | EN + NE | ~300-600 |
| `ekantipur_scraper.py` | Ekantipur (7 provinces + national) | Nepali | ~100-300 |

## Setup

```bash
pip install -r requirements.txt
```

## Government Ministry Scraper

Scrapes press releases, notices, circulars, and news from 17 Nepal government ministry websites (mof.gov.np, moest.gov.np, mohp.gov.np, etc.).

```bash
# List all configured ministries
python govt_scraper.py --list

# Scrape a single ministry
python govt_scraper.py --ministry mof

# Scrape all ministries, save JSON per ministry
python govt_scraper.py --all --output data/govt/

# Scrape with more pages (default: 3 pages per endpoint)
python govt_scraper.py --all --pages 10 --output data/govt/
```

**How it works:** Most Nepal govt websites use a common CMS with `/category/press-release/`, `/category/notice/` URL patterns and `/content/{id}/` article links. The scraper auto-detects both category-listing and table-based layouts. Handles Nepali (Bikram Sambat) dates and SSL certificate issues common on .gov.np sites.

**Adding a new ministry:** Edit the `MINISTRIES` dict in `govt_scraper.py`:
```python
MINISTRIES["new_ministry"] = MinistryConfig(
    source_id="new_ministry",
    name="Ministry of XYZ",
    name_ne="XYZ मन्त्रालय",
    base_url="https://moxyz.gov.np",
    endpoints={
        "press_release": "/category/press-release/",
        "notice": "/category/notice/",
    },
)
```

## News RSS Scraper

Fetches articles from 18 Nepal news RSS feeds — both English and Nepali.

```bash
# List all feeds
python news_rss_scraper.py --list

# Fetch all feeds
python news_rss_scraper.py --output data/news/

# Nepali feeds only (for Devanagari corpus)
python news_rss_scraper.py --language ne --output data/nepali_news/

# English feeds only
python news_rss_scraper.py --language en --output data/english_news/

# Single feed
python news_rss_scraper.py --feed setopati --output data/

# JSONL format (one article per line — good for training)
python news_rss_scraper.py --output corpus.jsonl --format jsonl
```

**Sources include:** Kathmandu Post, OnlineKhabar (EN+NE), Setopati, Nagarik News, BBC Nepali, Annapurna Post, Gorkhapatra, Pahilo Post, Khabarhub, and more.

## Ekantipur Scraper

Async HTML scraper for ekantipur.com (Nepal's largest Nepali-language news site). Their RSS returns 404, so this scrapes HTML directly.

```bash
# Scrape national + all 7 provinces
python ekantipur_scraper.py --output data/ekantipur/

# Single province
python ekantipur_scraper.py --province gandaki --output data/ekantipur/

# National only
python ekantipur_scraper.py --national --output data/ekantipur/

# JSONL for training
python ekantipur_scraper.py --output ekantipur.jsonl --format jsonl

# List provinces
python ekantipur_scraper.py --list
```

## Output Format

All scrapers output JSON with these fields:

```json
{
  "id": "unique_id",
  "title": "Article title (may be in Devanagari)",
  "url": "https://...",
  "source_id": "source_identifier",
  "source_name": "Human-readable source name",
  "language": "en|ne",
  "published_at": "2025-01-15T00:00:00",
  "summary": "Article excerpt if available",
  "scraped_at": "2025-01-15T12:00:00"
}
```

## Building a Corpus

Example: scrape everything into JSONL for fine-tuning:

```bash
mkdir -p corpus

# Government documents
python govt_scraper.py --all --pages 10 --output corpus/govt/

# News RSS (run daily via cron for fresh articles)
python news_rss_scraper.py --output corpus/news.jsonl --format jsonl

# Ekantipur
python ekantipur_scraper.py --output corpus/ekantipur.jsonl --format jsonl
```

Set up a daily cron job to accumulate articles over time:
```bash
# Add to crontab -e
0 6 * * * cd /path/to/nepali-corpus && python news_rss_scraper.py --output /data/corpus/news_$(date +\%Y\%m\%d).jsonl --format jsonl
```

## Notes

- **Rate limiting:** All scrapers include configurable delays between requests (default 0.5-1s). Be respectful to the servers.
- **SSL issues:** Nepal govt sites often have expired/invalid SSL certs. The govt scraper disables SSL verification (`verify=False`) — this is expected.
- **Nepali dates:** Government documents use Bikram Sambat (BS) calendar dates (e.g., 2081-09-15). These are preserved as-is in the `date_bs` field.
- **Deduplication:** All scrapers deduplicate by URL within a single run. For cross-run dedup, use the `id` or `url` field.

## License

MIT
