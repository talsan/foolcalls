import os
from datetime import datetime
import argparse
import logging
from foolcalls.config import Aws, FoolCalls
import boto3
from . import scrapers, helpers
from io import BytesIO
import gzip
import shutil
import random
import requests


log = logging.getLogger(__name__)

class Downloader:
    def __init__(self, cid: str, outputpath: str):
        # downloader
        self.cid = cid
        self.call_url = f'{FoolCalls.EARNINGS_TRANSCRIPTS_ROOT}/{helpers.to_url(cid)}'

        self.key = f'state=downloaded/rundate={datetime.now().strftime("%Y%m%d")}/cid={self.cid}.gz'
        self.outputpath = outputpath
        self.html_content = bytes()
        self.fool_download_ts = str()

    def request_transcript_url(self):
        request = dict(url=self.call_url,
                       headers={'User-Agent': random.choice(FoolCalls.USER_AGENT_LIST)})

        log.info(f'request: {request}')
        response = requests.get(**request)
        assert response.status_code == 200
        self.html_content = response.content
        self.fool_download_ts = str(datetime.now())

        return self

    def save_raw_transcript(self):
        if self.outputpath.lower() == 's3':
            self.put_raw_transcript_in_s3()
        else:
            self.save_raw_transcript_locally()
        return self

    def save_raw_transcript_locally(self):
        input_file_buffer = BytesIO(self.html_content)
        output_path = f'{self.outputpath.rstrip("/")}/{self.key}'
        if not os.path.exists(os.path.dirname(output_path)):
            os.makedirs(os.path.dirname(output_path))
        with gzip.GzipFile(output_path, mode='wb') as gz_out:
            shutil.copyfileobj(input_file_buffer, gz_out)
        print(f'wrote: {self.key} locally to {self.outputpath}')

    def put_raw_transcript_in_s3(self):
        input_file_buffer = BytesIO(self.html_content)
        compressed_file_buffer = BytesIO()

        metadata = {'cid': self.cid,
                    'call_url': self.call_url,
                    'fool_download_ts': self.fool_download_ts}

        with gzip.GzipFile(fileobj=compressed_file_buffer, mode='wb') as gz:
            shutil.copyfileobj(input_file_buffer, gz)
        compressed_file_buffer.seek(0)

        aws_session = boto3.Session(aws_access_key_id=Aws.AWS_KEY,
                                    aws_secret_access_key=Aws.AWS_SECRET)
        s3_client = aws_session.client('s3')

        s3_client.upload_fileobj(Bucket=Aws.S3_FOOLCALLS_BUCKET,
                                 Key=self.key,
                                 Fileobj=compressed_file_buffer,
                                 ExtraArgs={'Metadata': metadata,
                                            'ContentType': 'text/html',
                                            'ContentEncoding': 'gzip'})

        s3_output_url = f'{Aws.S3_OBJECT_ROOT}/{Aws.S3_FOOLCALLS_BUCKET}/{self.key}'
        log.info(f's3 upload success: {s3_output_url}')


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main(cid, outputpath, scraper_callback):
    dl = Downloader(cid=cid, outputpath=outputpath)

    dl.request_transcript_url(). \
        save_raw_transcript()

    if scraper_callback:
        scrapers.process_transcript(cid=dl.cid, html_content=dl.html_content, outputpath=outputpath)


if __name__ == "__main__":
    # command line arguments
    parser = argparse.ArgumentParser(description='Download raw html files of earnings call transcripts from fool.com')
    parser.add_argument('cid', help='call id to parse')
    parser.add_argument('outputpath', help=f'where to send output on local machine; if outputpath==\'s3\', output is '
                                           f'uploaded to the Aws.OUPUT_BUCKET variable defined in config.py')
    parser.add_argument('--scraper_callback',
                        help='whether to invoke the transcript scraper immediately after a file is downloaded; '
                             'otherwise scraping can be run as a separate batch process', action='store_true')
    args = parser.parse_args()

    # logging (will inherit log calls from utils.pricing and utils.s3_helpers)
    this_file = os.path.basename(__file__).replace('.py', '')
    log_id = f'{this_file}_{datetime.now().strftime("%Y%m%dT%H%M%S")}'
    logging.basicConfig(filename=f'../logs/{log_id}.log', level=logging.INFO,
                        format=f'%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    log.info(f'configuration parameters: {FoolCalls.__dict__}')
    log.info(f'input parameters: {args}')

    # run main
    main(args.cid, args.outputpath, args.scraper_callback)
    log.info(f'successfully completed script')
