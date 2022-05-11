import os
from dotenv import load_dotenv

load_dotenv()

class Local:
    MULTIPROCESS_ON = True
    MULTIPROCESS_CPUS = None # None defaults to mp.cpu_count()


class Aws:
    # aws config
    AWS_KEY = os.environ.get('AWS_KEY')
    AWS_SECRET = os.environ.get('AWS_SECRET')

    S3_REGION_NAME = 'us-west-2'
    S3_FOOLCALLS_BUCKET = 'fool-calls'
    S3_OBJECT_ROOT = 'https://s3.console.aws.amazon.com/s3/object'

    ATHENA_REGION_NAME = 'us-west-2'
    ATHENA_OUTPUT_BUCKET = 'fool-calls-athena-output'

    ATHENA_SLEEP_BETWEEN_REQUESTS = 3
    ATHENA_QUERY_TIMEOUT = 200

class AlphaVantage():
    AV_KEY = os.environ.get('AV_KEY')

class FoolCalls:

    SCRAPER_VERSION = '202007.1'

    # fixed ishares.com values, the rest is derived/scraped
    ROOT = 'https://www.fool.com'
    EARNINGS_LINKS_ROOT = f'{ROOT}/earnings-call-transcripts'
    EARNINGS_TRANSCRIPTS_ROOT = f'{ROOT}/earnings/call-transcripts'

    # LINK COLLECTION PARAMETERS
    START_PAGE = 1 # useful for debugging specific page
    TRAVERSE_ALL_PAGES_FOR_NEW_URLS = False
    # if True: process looks for new links inside *all* (1000+) pages
    # if False: process looks for unprocessed links until it arrives at a page with no unprocessed links
    # note this only relevant if overwrite=False (because if overwrite=True, you're processing everything over again)

    # throttle requests to fool.com
    MIN_SLEEP_BETWEEN_REQUESTS = 2
    MAX_SLEEP_BETWEEN_REQUESTS = 8
    MAX_PAGES = None # should be None unless running quick tests (e.g. get the first 30-50 transcripts)

    # user agents that get randomly cycled through when making ishares.com download requests
    USER_AGENT_LIST = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
        'Mozilla/5.0 (Windows NT 5.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36',
        'Mozilla/4.0 (compatible; MSIE 9.0; Windows NT 6.1)',
        'Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko',
        'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)',
        'Mozilla/5.0 (Windows NT 6.1; Trident/7.0; rv:11.0) like Gecko',
        'Mozilla/5.0 (Windows NT 6.2; WOW64; Trident/7.0; rv:11.0) like Gecko',
        'Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; rv:11.0) like Gecko',
        'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.0; Trident/5.0)',
        'Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; rv:11.0) like Gecko',
        'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)',
        'Mozilla/5.0 (Windows NT 6.1; Win64; x64; Trident/7.0; rv:11.0) like Gecko',
        'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0)',
        'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; Trident/6.0)',
        'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; .NET CLR 2.0.50727; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729)'
    ]
