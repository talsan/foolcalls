import boto3
from foolcalls.config import Aws, FoolCalls
import logging
import re
import random
import time

log = logging.getLogger(__name__)

# GLOBAL DATA (AWS CLIENT)
aws_session = boto3.Session(aws_access_key_id=Aws.AWS_KEY,
                            aws_secret_access_key=Aws.AWS_SECRET)

s3_client = aws_session.client('s3', region_name=Aws.S3_REGION_NAME)


def list_keys(Bucket, Prefix='', Suffix='', full_path=True, remove_ext=False):
    # get pages for bucket and prefix
    paginator = s3_client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=Bucket, Prefix=Prefix)

    # iterate through pages and store the keys in a list
    keys = []
    for page in page_iterator:
        if 'Contents' in page.keys():
            for content in page['Contents']:
                key = content['Key']
                if not key.endswith('/'):  # ignore directories
                    if key.endswith(Suffix):
                        if not full_path:
                            key = re.sub(Prefix, '', key)
                        if remove_ext:
                            key = re.sub('\.[^.]+$','',key)
                        keys.append(key)
    return keys


def to_cid(call_url):
    url_split = call_url.split('/')
    return f'{url_split[-4]}-{url_split[-3]}-{url_split[-2]}-{url_split[-1].replace(".aspx", "")}'


def to_url(cid):
    cid_split = cid.split('-')
    return f'{"/".join(cid_split[0:3])}/{"-".join(cid_split[3:])}.aspx'

def sleep_between_requests():
    sleep_seconds = random.randint(FoolCalls.MIN_SLEEP_BETWEEN_REQUESTS,
                                   FoolCalls.MAX_SLEEP_BETWEEN_REQUESTS)
    log.info(f'post request sleep for {sleep_seconds} seconds ...')
    time.sleep(sleep_seconds)