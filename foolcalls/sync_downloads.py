import os
from datetime import datetime
import argparse
import logging
from config import Aws, FoolCalls
from foolcalls import downloaders, scrapers
from foolcalls.utils import helpers
import re
import glob


log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# BUILD A DOWNLOAD QUEUE
# ---------------------------------------------------------------------------
def build_download_queue(outputpath: str, overwrite: str) -> list:

    if overwrite:
        previously_processed_call_urls = []
    else:
        previously_processed_call_urls = get_previously_processed_call_urls(outputpath)

    call_urls = get_call_urls(previously_processed_call_urls)

    download_queue = [helpers.to_cid(call_url) for call_url in call_urls]

    log.info(f'***** {len(call_urls)} transcripts have been queued for downloading ***** ')
    return download_queue


def get_previously_processed_call_urls(outputpath: str) -> list:
    path_prefix = 'state=downloaded/'

    if outputpath == 's3':
        previously_processed_paths = helpers.list_keys(Bucket=Aws.S3_FOOLCALLS_BUCKET,
                                                       Prefix=path_prefix,
                                                       full_path=False,
                                                       remove_ext=True)
    else:
        previously_processed_paths = [re.sub(path_prefix, '', re.sub('\.[^.]+$', '', os.path.basename(key)))
                                      for key in
                                      glob.glob(f'{outputpath.rstrip("/")}/{path_prefix.rstrip("/")}/**/*.gz',
                                                recursive=True)]

    previously_processed_call_urls = []
    for pp_path in previously_processed_paths:
        pp_cid = re.sub('rundate=\\d{8}/cid=', '', pp_path)
        pp_url = f'{FoolCalls.EARNINGS_TRANSCRIPTS_ROOT}/{helpers.to_url(pp_cid)}'
        previously_processed_call_urls.append(pp_url)

    return previously_processed_call_urls


# crawl paginated list of call links/urls (i.e. start at page 1, collect links and move to the next page, and so on)
# as of 2020-07-10, there are 20 links per page.
# so as to avoid hitting fool.com unnecessarily, the process stops if it reaches a page whose urls are already downloaded.
# to turn this setting off, set FoolCalls.TRAVERSE_ALL_PAGES_FOR_NEW_URLS (in config.py) to True
def get_call_urls(previously_processed_call_urls: list = None) -> list:

    if previously_processed_call_urls is None:
        previously_processed_call_urls = []

    page_num = FoolCalls.START_PAGE
    new_call_urls = []
    while page_num <= (FoolCalls.MAX_PAGES or float('inf')):

        this_page_urls = scrapers.scrape_transcript_urls_by_page(page_num=page_num)
        helpers.sleep_between_requests()

        if this_page_urls is not None:

            # get new urls on the page that weren't already processed
            new_urls_on_this_page = list(set(this_page_urls) - set(previously_processed_call_urls))
            log.info(f'{len(new_urls_on_this_page)} unprocessed urls on page {page_num}')
            new_call_urls.extend(new_urls_on_this_page)
            page_num += 1

            if not FoolCalls.TRAVERSE_ALL_PAGES_FOR_NEW_URLS:
                if len(new_urls_on_this_page) > 0:
                    new_call_urls.extend(new_urls_on_this_page)
                    page_num += 1
                else:
                    break
        else:
            log.info(f'No links found on page {page_num}, indicating no more calls are available')
            break

    return new_call_urls


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main(outputpath, overwrite, scraper_callback):
    cid_download_queue = build_download_queue(outputpath, overwrite)
    for i, cid in enumerate(cid_download_queue):

        log.info(f'now downloading/scraping {i + 1} of {len(cid_download_queue)}')

        try:
            downloaders.main(cid, outputpath, scraper_callback)
            helpers.sleep_between_requests()

        except Exception as e:
            log.error(f'error: {e}')


if __name__ == "__main__":
    # command line arguments
    parser = argparse.ArgumentParser(description='Download raw html files of earnings call transcripts from fool.com')
    parser.add_argument('outputpath', help=f'where to send output on local machine; if outputpath==\'s3\', output is '
                                           f'uploaded to the Aws.OUPUT_BUCKET variable defined in config.py')
    parser.add_argument('--scraper_callback',
                        help='whether to invoke the transcript scraper immediately after a file is downloaded; '
                             'otherwise scraping can be run as a separate batch process', action='store_true')
    parser.add_argument('--overwrite', help=f'overwrite transcripts that have already been downloaded to specified '
                                            f'<outputpath>; otherwise it\'s an update (i.e. only download new transcripts)',
                        action='store_true')
    args = parser.parse_args()

    # logging (will inherit log calls from utils.pricing and utils.s3_helpers)
    this_file = os.path.basename(__file__).replace('.py', '')
    log_id = f'{this_file}_{datetime.now().strftime("%Y%m%dT%H%M%S")}'
    logging.basicConfig(filename=f'./logs/{log_id}.log', level=logging.INFO,
                        format=f'%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    log.info(f'configuration parameters: {FoolCalls.__dict__}')
    log.info(f'input parameters: {args}')

    # run main
    main(args.outputpath, args.overwrite, args.scraper_callback)
    log.info(f'successfully completed script')
