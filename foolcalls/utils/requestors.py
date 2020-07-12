import requests
import logging
import random
import config
from foolcalls.utils import decorators

log = logging.getLogger(__name__)


@decorators.sleep_between_requests
def transcript(call_url):
    request = dict(url=call_url,
                   headers={'User-Agent': random.choice(config.USER_AGENT_LIST)})

    response = requests.get(**request)

    log.info(f'request: {request}')

    assert response.status_code == 200
    return response


@decorators.sleep_between_requests
def links(page_num):
    request = dict(url=config.FoolCalls.EARNINGS_LINKS_ROOT,
                   params={'page': page_num},
                   headers={'User-Agent': random.choice(config.USER_AGENT_LIST)})

    log.info(f'request: {request}')

    response = requests.get(**request)
    assert response.status_code == 200
    return response