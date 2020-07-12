import os
from datetime import datetime
import argparse
import logging
from config import Aws, FoolCalls
import json
import boto3
import re
from foolcalls.utils import helpers, extractors
from io import BytesIO
import gzip
import shutil
import glob

log = logging.getLogger(__name__)

aws_session = boto3.Session(aws_access_key_id=Aws.AWS_KEY,
                            aws_secret_access_key=Aws.AWS_SECRET)


# ---------------------------------------------------------------------------
# SCRAPER
# ---------------------------------------------------------------------------
def process_transcript(cid: str, html_content: bytes, outputpath: str) -> None:
    call_url = f'{FoolCalls.EARNINGS_TRANSCRIPTS_ROOT}/{helpers.to_url(cid)}'
    log.info(f'scraping cid: {cid}; with url: {call_url}')

    # scrape
    call_transcript_data = scrape_transcript(html_content)

    # add source_metadata
    output = {'cid': cid, 'call_url': call_url}
    output.update(call_transcript_data)

    # upload to s3
    key = f'state=structured/version={FoolCalls.SCRAPER_VERSION}/cid={cid}.json'
    save_transcript(outputpath, key, output)


def scrape_transcript(html_text: bytes) -> dict:
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


def build_scraper_queue(outputpath: str, overwrite: str) -> list:
    if outputpath == 's3':
        downloaded_paths = helpers.list_keys(Bucket=Aws.S3_FOOLCALLS_BUCKET,
                                             Prefix='state=downloaded/')
    else:
        downloaded_paths = [dp.replace(f'{outputpath.rstrip("/")}/', '')
                            for dp in glob.glob(f'{outputpath.rstrip("/")}/state=downloaded/**/*.gz')]

    scraper_queue = [{'cid': re.findall('cid=(.*).gz', dl_path)[0],
                      'input_key': dl_path}
                     for dl_path in downloaded_paths]

    # if file has been downloaded multiple times, take latest rundate
    for sc in scraper_queue:
        sc.update({'rundate':re.findall('rundate=(\\d{8})', sc['input_key'])[0]})


    if not overwrite:
        previously_scraped_cids = helpers.list_keys(Bucket=Aws.S3_FOOLCALLS_BUCKET,
                                                    Prefix=f'state=structured/version={FoolCalls.SCRAPER_VERSION}/cid=',
                                                    full_path=False,
                                                    remove_ext=True)

        scraper_queue = [queue_item for queue_item in scraper_queue
                         if queue_item['cid'] not in previously_scraped_cids]

    return scraper_queue

def get_raw_transcript(outputpath,input_key):
    if outputpath == 's3':
        html_content = get_raw_transcript_from_s3(input_key)
    else:
        html_content = get_raw_transcript_from_local(outputpath, input_key)

    return html_content

def get_raw_transcript_from_s3(input_key):
    input_file_buffer = BytesIO()
    output_file_buffer = BytesIO()

    s3_client = aws_session.client('s3', region_name=Aws.S3_REGION_NAME)
    s3_client.download_fileobj(Bucket=Aws.S3_FOOLCALLS_BUCKET,
                               Key=input_key,
                               Fileobj=input_file_buffer)

    input_file_buffer.seek(0)
    with gzip.GzipFile(fileobj=input_file_buffer, mode='rb') as gz:
        shutil.copyfileobj(gz, output_file_buffer)

    return output_file_buffer.getvalue()


def get_raw_transcript_from_local(outputpath, input_key):
    output_file_buffer = BytesIO()
    with gzip.open(f'{outputpath.rstrip("/")}/{input_key}', 'rb') as f_in:
        shutil.copyfileobj(f_in, output_file_buffer)

    return output_file_buffer.getvalue()


def save_transcript(outputpath: str, key: str, output: dict) -> None:
    if outputpath == 's3':
        put_transcript_in_s3(key, output)
    else:
        save_transcript_local(outputpath, key, output)


def save_transcript_local(outputpath: str, key: str, output: dict) -> None:
    output_path = f'{outputpath.rstrip("/")}/{key}'
    if not os.path.exists(os.path.dirname(output_path)):
        os.makedirs(os.path.dirname(output_path))
    with open(output_path, 'w') as f:
        json.dump(output, f)
    print(f'wrote: {key} locally to {outputpath}')


def put_transcript_in_s3(key: str, output: dict) -> None:
    s3_client = aws_session.client('s3', region_name=Aws.S3_REGION_NAME)
    s3_client.put_object(Bucket=Aws.S3_FOOLCALLS_BUCKET,
                         Key=key,
                         Body=json.dumps(output))

    s3_output_url = f'{Aws.S3_OBJECT_ROOT}/{Aws.S3_FOOLCALLS_BUCKET}/{key}'
    log.info(f's3 upload success: {s3_output_url}')


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main(outputpath, overwrite):
    scraper_queue = build_scraper_queue(outputpath, overwrite)
    for queue_item in scraper_queue:
        html_content = get_raw_transcript(outputpath, input_key=queue_item['input_key'])
        process_transcript(queue_item['cid'], html_content, outputpath)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
# everything below will only be executed if this script is called from the command line
# if this file is imported, nothing below will be executed
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    # command line arguments
    parser = argparse.ArgumentParser(description='scrape the contents of a call from a ')
    parser.add_argument('outputpath', help=f'where to send output on local machine; if outputpath==\'s3\', output is '
                                           f'uploaded to the Aws.OUPUT_BUCKET variable defined in config.py')
    parser.add_argument('--overwrite', help=f'Overwrite holdings that have already been downloaded to S3',
                        action='store_true')
    args = parser.parse_args()

    # logging (will inherit log calls from utils.pricing and utils.s3_helpers)
    this_file = os.path.basename(__file__).replace('.py', '')
    log_id = f'{this_file}_{datetime.now().strftime("%Y%m%dT%H%M%S")}'
    logging.basicConfig(filename=f'./logs/{log_id}.log', level=logging.INFO,
                        format=f'%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # run main
    main(args.outputpath, args.overwrite)
    log.info(f'successfully completed script')
