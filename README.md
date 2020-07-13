# Source, Scrape, and Structure Quarterly Earnings Call Transcripts (and metadata) from fool.com

## Process Overview

1. **Build Download Queue of Conference Call URLs** `update_downloader.py` scrapes links from [fool.com/earnings-cal-transcripts](https://www.fool.com/earnings-call-transcripts/?page=1), a paginated list of earnings call urls (most recent calls appear first)
2. **Download Raw HTML Contents from those URLs** `downloader.py` downloads, compresses (gzip), and archives html contents for each url
3. **Scrape, Structure, and Store Transcripts**: `scraper.py` transforms individual raw html file into consistently structured JSON, with detailed metadata about speakers and their individual statements

#### Features & Options
- Decoupled into functional units:
  - events: `downloader.py` and `scraper.py` are self-contained modules that handle individual events
  - event-queues: `update_downloader.py` and `update_scraper.py` are wrappers that invoke/queue a series of events 
- Supports local or S3 file-store (see `outputpath` input parameter)
- Polite scraping (i.e. various throttles available in `config.py`)
