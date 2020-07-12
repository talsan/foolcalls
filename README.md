# Source, Scrape, and Structure Quarterly Earnings Call Transcripts (and metadata) from fool.com

## Process Overview

1. **Build a Call URL Download Queue** `downloader.py` scrapes links from [fool.com/earnings-cal-transcripts](https://www.fool.com/earnings-call-transcripts/?page=1), a paginated list list of earnings call urls (most recent calls appear first)
2. **Download Raw Contents from Call URLs** `downloader.py` then downloads, compresses (gzip), and archives html contents of each url (stored locally or in S3)
3. **Scrape, Structure, and Store Transcripts**: `scraper.py` transforms individual raw html file into consistently structured JSON (stored locally or in S3)

#### Features & Options
- Event-driven, stream-ready design
  - `downloader.py` and `scraper.py` modules each decouple (via seperate functions) the event-processing step from the event-queueing/invoking step
  - while 1000s of events could all be processed in seconds (parallel/asynchronous computing, etc.), it is advised to take it easy on fool.com and run sequentially with conservative sleep time between requests. 
- Supports local or S3 file-store (see `outputpath` input parameter)
- Gentle Scraping
  - Defaults to stopping crawling process when it detects that no new calls are available (this can be turned off via `config.py`)
  - Randomized sleep between requests (lower/threshold set in `config.py`)
  - Randomized user agents in request header (configurable in `config.py`)

## Process Architecture
![Process Architecture](Empty)

## Usage Patterns
