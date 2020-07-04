import os
from datetime import datetime
import argparse
import logging
import config
import boto3
from lxml import html
from utils.decorators import handle_one_element, handle_many_elements
import requestors
from utils import helpers
from io import BytesIO
import gzip
import shutil
import re

if config.FoolCalls.SCRAPE_DURING_DOWNLOAD:
    import scraper

log = logging.getLogger(__name__)

aws_session = boto3.Session(aws_access_key_id=config.Access.AWS_KEY,
                            aws_secret_access_key=config.Access.AWS_SECRET)


# ---------------------------------------------------------------------------
# BUILD A DOWNLOAD QUEUE
# ---------------------------------------------------------------------------
def build_download_queue(overwrite):
    previously_processed_call_urls = [] if overwrite else get_previously_processed_call_urls()
    call_urls = get_new_call_urls(previously_processed_call_urls)

    download_queue = [{'cid': helpers.to_cid(call_url), 'call_url': call_url}
                      for call_url in call_urls]

    log.info(f'***** {len(call_urls)} transcripts have been queued for downloading ***** ')
    return download_queue


def get_previously_processed_call_urls():
    previously_processed_paths = helpers.list_keys(Bucket=config.FoolCalls.S3_FOOLCALLS_BUCKET,
                                                   Prefix='state=downloaded/',
                                                   full_path=False,
                                                   remove_ext=True)

    previously_processed_call_urls = []
    for pp_path in previously_processed_paths:
        pp_cid = re.sub('rundate=\\d{8}/cid=', '', pp_path)
        pp_url = f'{config.FoolCalls.EARNINGS_TRANSCRIPTS_ROOT}/{helpers.to_url(pp_cid)}'
        previously_processed_call_urls.append(pp_url)

    return previously_processed_call_urls


def get_new_call_urls(previously_processed_call_urls):
    page_num = config.FoolCalls.START_PAGE
    new_call_urls = []
    while len(new_call_urls) < (config.FoolCalls.MAX_N_TRANSCRIPT_DOWNLOADS or float('inf')):
        this_page_urls_ext = scrape_transcript_urls_from_single_page(page_num=page_num)
        if this_page_urls_ext is not None:
            this_page_urls = [f'{config.FoolCalls.ROOT}{call_url_ext}' for call_url_ext in this_page_urls_ext]
            # get new urls on the page that weren't already processed
            new_urls_on_this_page = list(set(this_page_urls) - set(previously_processed_call_urls))
            log.info(f'{len(new_urls_on_this_page)} unprocessed urls on page {page_num}')
            new_call_urls.extend(new_urls_on_this_page)
            page_num += 1

            if not config.FoolCalls.TRAVERSE_ALL_PAGES_FOR_NEW_URLS:
                if len(new_urls_on_this_page) > 0:
                    new_call_urls.extend(new_urls_on_this_page)
                    page_num += 1
                else:
                    break
        else:
            log.info(f'No links found on page {page_num}, indicating no more calls are available')
            break

    return new_call_urls


def scrape_transcript_urls_from_single_page(page_num):
    response = requestors.links(page_num)
    html_selector = html.fromstring(response.text)
    links_selector = find_link_container(html_selector)
    call_urls = extract_call_urls(links_selector)
    return call_urls


# ---------------------------------------------------------------------------
# SELECTORS/EXTRACTORS
# ---------------------------------------------------------------------------
# one selector: find the container that holds the links
@handle_one_element(error_on_empty=True)
def find_link_container(html_selector):
    links_container = html_selector.xpath('.//div[@class = "content-block listed-articles recent-articles m-np"]')
    return links_container


# one extractor: extract the links sitting in links-container
@handle_many_elements(error_on_empty=False)
def extract_call_urls(links_container):
    call_urls = links_container.xpath('.//div[@class="list-content"]/a/@href')
    return call_urls


# ---------------------------------------------------------------------------
# DOWNLOADER ENGINE
# ---------------------------------------------------------------------------
class Downloader:
    def __init__(self, cid, call_url):
        # downloader
        self.cid = cid
        self.call_url = call_url

        self.html_content = bytes()
        self.fool_download_ts = str()

        # s3 upload
        self.s3_client = aws_session.client('s3', region_name=config.FoolCalls.S3_REGION_NAME)
        self.s3_upload_response = ''  # returned from aws after s3-upload

    def download_raw_transcript(self):
        r = requestors.transcript(self.call_url)

        self.html_content = r.content
        self.fool_download_ts = str(datetime.now())
        return self

    def put_raw_transcript_in_s3(self):
        download_date = datetime.now().strftime('%Y%m%d')

        s3_key = f'state=downloaded/rundate={download_date}/cid={self.cid}.gz'

        metadata = {'cid': self.cid,
                    'call_url': self.call_url,
                    'fool_download_ts': self.fool_download_ts}

        # gzip file
        # code adapted from https://gist.github.com/tobywf/079b36898d39eeb1824977c6c2f6d51e
        # with explanation here https://tobywf.com/2017/06/gzip-compression-for-boto3/
        input_file_buffer = BytesIO(self.html_content)
        compressed_file_buffer = BytesIO()
        with gzip.GzipFile(fileobj=compressed_file_buffer, mode='wb') as gz:
            shutil.copyfileobj(input_file_buffer, gz)
        compressed_file_buffer.seek(0)

        self.s3_upload_response = self.s3_client.upload_fileobj(Bucket=config.FoolCalls.S3_FOOLCALLS_BUCKET,
                                                                Key=s3_key,
                                                                Fileobj=compressed_file_buffer,
                                                                ExtraArgs={'Metadata': metadata,
                                                                           'ContentType': 'text/html',
                                                                           'ContentEncoding': 'gzip'})

        s3_output_url = f'{config.FoolCalls.S3_OBJECT_ROOT}/{config.FoolCalls.S3_FOOLCALLS_BUCKET}/{s3_key}'
        log.info(f's3 upload success: {s3_output_url}')


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main(overwrite):
    log.info(f'configuration parameters: {config.FoolCalls.__dict__}')
    log.info(f'input parameters: overwrite={overwrite}')
    download_queue = build_download_queue(overwrite)
    for i, queue_item in enumerate(download_queue):
        log.info(f'now downloading/scraping {i + 1} of {len(download_queue)}')
        try:
            dl = Downloader(cid=queue_item['cid'],
                            call_url=queue_item['call_url'])

            dl.download_raw_transcript().put_raw_transcript_in_s3()

            if config.FoolCalls.SCRAPE_DURING_DOWNLOAD:
                scraper.process_transcript(cid=dl.cid, html_content=dl.html_content)

        except Exception as e:
            log.error(f'error: {e}')


if __name__ == "__main__":
    # command line arguments
    parser = argparse.ArgumentParser(description='Extract Raw ETF Holding Files')
    parser.add_argument('--overwrite', help=f'Overwrite holdings that have already been downloaded to S3',
                        action='store_true')
    args = parser.parse_args()

    # logging (will inherit log calls from utils.pricing and utils.s3_helpers)
    this_file = os.path.basename(__file__).replace('.py', '')
    log_id = f'{this_file}_{datetime.now().strftime("%Y%m%dT%H%M%S")}'
    logging.basicConfig(filename=f'./logs/{log_id}.log', level=logging.INFO,
                        format=f'%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # run main
    main(args.overwrite)
    log.info(f'successfully completed script')
