import os
from datetime import datetime
import argparse
import logging
import config
import json
import boto3
import re
from foolcalls.utils import helpers, extractors
from io import BytesIO
import gzip
import shutil

log = logging.getLogger(__name__)

aws_session = boto3.Session(aws_access_key_id=config.Access.AWS_KEY,
                            aws_secret_access_key=config.Access.AWS_SECRET)


# ---------------------------------------------------------------------------
# SCRAPER
# ---------------------------------------------------------------------------
def process_transcript(cid, html_content):
    call_url = f'{config.FoolCalls.EARNINGS_TRANSCRIPTS_ROOT}/{helpers.to_url(cid)}'
    log.info(f'scraping cid: {cid}; with url: {call_url}')

    # scrape
    call_transcript_data = scrape_transcript(html_content)

    # add source_metadata
    output = {'cid': cid, 'call_url': call_url}
    output.update(call_transcript_data)

    # upload to s3
    put_transcript_in_s3(cid, output)


def scrape_transcript(html_text):
    # init output
    output = {}

    # top level html elements (aka containers)
    # returns a dictionary wherein each key is a different container object
    # no information is being extracted yet; different parts of the page are being isolated here
    containers = extractors.find_containers(html_text)

    # extract structured data from elements
    publisher_metadata = extractors.get_publication_metadata(containers['publication_info'])
    call_title_metadata = extractors.get_title_metadata(containers['article_header'])
    call_header_metadata = extractors.get_header_metadata(containers['transcript_header'])
    call_duration_metadata = extractors.get_duration_metadata(containers['duration'])
    call_statement_data = extractors.get_statement_data(pres_elements=containers['pres'],
                                                        qa_elements=containers['qa'])

    # metadata for speakers on call (not scraped, derived from statement metadata)
    call_participant_metadata = extractors.get_participant_metadata(call_statement_data)

    # format output from extracted data
    output.update(publisher_metadata)
    output.update(call_title_metadata)
    output.update(call_header_metadata)
    output.update(call_duration_metadata)
    output.update({'participants': call_participant_metadata})
    output.update({'call_transcript': call_statement_data})
    return output

def build_scraper_queue(overwrite):
    downloaded_paths = helpers.list_keys(Bucket=config.FoolCalls.S3_FOOLCALLS_BUCKET,
                                         Prefix='state=downloaded/')

    scraper_queue = [{'cid': re.findall('cid=(.*).gz', dl_path)[0],
                      's3_key': dl_path}
                     for dl_path in downloaded_paths]

    if not overwrite:
        previously_scraped_cids = helpers.list_keys(Bucket=config.FoolCalls.S3_FOOLCALLS_BUCKET,
                                                    Prefix=f'state=structured/version={config.FoolCalls.SCRAPER_VERSION}/cid=',
                                                    full_path=False,
                                                    remove_ext=True)

        scraper_queue = [queue_item for queue_item in scraper_queue
                         if queue_item['cid'] not in previously_scraped_cids]

    return scraper_queue


def get_raw_transcript_from_s3(s3_key):
    input_file_buffer = BytesIO()
    output_file_buffer = BytesIO()

    s3_client = aws_session.client('s3', region_name=config.FoolCalls.S3_REGION_NAME)
    s3_client.download_fileobj(Bucket=config.FoolCalls.S3_FOOLCALLS_BUCKET,
                               Key=s3_key,
                               Fileobj=input_file_buffer)

    input_file_buffer.seek(0)
    with gzip.GzipFile(fileobj=input_file_buffer, mode='rb') as gz:
        shutil.copyfileobj(gz, output_file_buffer)

    return output_file_buffer.getvalue()


def put_transcript_in_s3(cid, output):
    s3_client = aws_session.client('s3', region_name=config.FoolCalls.S3_REGION_NAME)
    s3_key = f'state=structured/version={config.FoolCalls.SCRAPER_VERSION}/cid={cid}.json'
    s3_client.put_object(Bucket=config.FoolCalls.S3_FOOLCALLS_BUCKET,
                         Key=s3_key,
                         Body=json.dumps(output))

    s3_output_url = f'{config.FoolCalls.S3_OBJECT_ROOT}/{config.FoolCalls.S3_FOOLCALLS_BUCKET}/{s3_key}'
    log.info(f's3 upload success: {s3_output_url}')


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main(overwrite):
    scraper_queue = build_scraper_queue(overwrite)
    for queue_item in scraper_queue:
        html_content = get_raw_transcript_from_s3(s3_key=queue_item['s3_key'])
        process_transcript(cid=queue_item['cid'], html_content=html_content)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
# everything below will only be executed if this script is called from the command line
# if this file is imported, nothing below will be executed
# ---------------------------------------------------------------------------
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
