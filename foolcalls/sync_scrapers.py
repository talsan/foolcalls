import os
from datetime import datetime
import argparse
import logging
from foolcalls.config import Aws, FoolCalls, Local
import boto3
import re
import glob
from foolcalls import scrapers, helpers
import multiprocessing as mp

log = logging.getLogger(__name__)

aws_session = boto3.Session(aws_access_key_id=Aws.AWS_KEY,
                            aws_secret_access_key=Aws.AWS_SECRET)


def build_scraper_queue(outputpath: str, overwrite: str) -> list:
    log.info('building scraper queue...')
    if outputpath == 's3':
        downloaded_paths = helpers.list_keys(Bucket=Aws.S3_FOOLCALLS_BUCKET,
                                             Prefix='state=downloaded/')
    else:
        downloaded_paths = [dp.replace(f'{outputpath.rstrip("/")}/', '')
                            for dp in glob.glob(f'{outputpath.rstrip("/")}/state=downloaded/**/*.gz')]

    scraper_queue_raw = [{'cid': re.findall('cid=(.*)\\.gz', dl_path)[0],
                          'key': dl_path}
                         for dl_path in downloaded_paths]

    # if two cid's exist, use the most recent rundate
    def drop_duplicates(scraper_queue):
        result = {}
        for sc in scraper_queue:
            sc.update({'rundate': re.findall('rundate=(\\d{8})', sc['key'])[0]})
            if (sc['cid'] not in result) or sc['rundate'] >= result[sc['cid']]['rundate']:
                result[sc['cid']] = sc

        return [v for k, v in result.items()]

    scraper_queue = drop_duplicates(scraper_queue_raw)

    if not overwrite:
        if outputpath == 's3':
            previously_scraped_cids = helpers.list_keys(Bucket=Aws.S3_FOOLCALLS_BUCKET,
                                                        Prefix=f'state=structured/'
                                                               f'version={FoolCalls.SCRAPER_VERSION}/cid=',
                                                        full_path=False,
                                                        remove_ext=True)
        else:
            previously_scraped_cids = [re.findall('cid=(.*)\\.json', key)[0]
                                       for key in glob.glob(f'{outputpath.rstrip("/")}/state=structured/'
                                                            f'version={FoolCalls.SCRAPER_VERSION}/cid=*.json')]

        scraper_queue = [queue_item for queue_item in scraper_queue
                         if queue_item['cid'] not in previously_scraped_cids]

    log.info(f'beginning to scrape {len(scraper_queue)} transcripts from {outputpath}')
    return scraper_queue


def main(outputpath, overwrite):
    scraper_queue = build_scraper_queue(outputpath, overwrite)

    if Local.MULTIPROCESS_ON:
        mp_inputs = [(sc['cid'], outputpath, sc['key']) for sc in scraper_queue]
        cpu_count = mp.cpu_count() if Local.MULTIPROCESS_CPUS is None else Local.MULTIPROCESS_CPUS
        pool = mp.Pool(processes=cpu_count)
        pool.starmap(scrapers.main, mp_inputs)
    else:
        for queue_item in scraper_queue:
            scrapers.main(queue_item['cid'], outputpath, queue_item['key'])


if __name__ == "__main__":
    # command line arguments
    parser = argparse.ArgumentParser(description='scrape the contents of a call from a ')
    parser.add_argument('outputpath', help=f'where to send output on local machine; if outputpath==\'s3\', output is '
                                           f'uploaded to the Aws.OUPUT_BUCKET variable defined in config.py')
    parser.add_argument('--overwrite', help=f'Overwrite parsed transcripts that have already been downloaded to S3',
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
