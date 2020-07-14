# Scrape & Structure Earnings Call Transcripts (and metadata) from fool.com

## Process Overview

1. **Crawl Fool.com for Transcript URLs** `update_downloader.py` scrapes urls from the [earnings call landing page](https://www.fool.com/earnings-call-transcripts/?page=1) - a paginated list of earnings call links (most recent calls appear first)
2. **Store Raw HTML Contents from those URLs** `downloader.py` downloads, compresses (gzip), and archives contents of each url (stored locally or in s3)
3. **Scrape & Structure Transcripts**: `scraper.py` transforms individual raw html file into a consistently structured JSON, with detailed metadata about speakers and their individual statements (stored locally or in s3)

#### Features & Options
- Decoupled into functional units:
  - events: `downloader.py` and `scraper.py` are self-contained modules that handle individual events
  - event-queues: `update_downloader.py` and `update_scraper.py` are wrappers that invoke/queue a series of events to stay in sync with new calls on fool.com
- Supports local or S3 file-store (see `outputpath` input parameter)
- Polite scraping (i.e. various throttles available in `config.py`)

### Process Details
