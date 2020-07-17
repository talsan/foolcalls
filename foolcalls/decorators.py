import logging
from functools import wraps

log = logging.getLogger(__name__)


# DECORATORS
# def sleep_between_requests(func):
#     @wraps(func)
#     def wrapper(*args, **kwargs):
#         response = func(*args, **kwargs)
#         sleep_seconds = random.randint(config.FoolCalls.MIN_SLEEP_BETWEEN_REQUESTS,
#                                        config.FoolCalls.MAX_SLEEP_BETWEEN_REQUESTS)
#         log.info(f'post request sleep for {sleep_seconds} seconds ...')
#         time.sleep(sleep_seconds)
#         return response
#     return wrapper


def handle_one_element(error_on_empty=True):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            selector = func(*args, **kwargs)
            # begin to validate function output
            if len(selector) == 1:
                return selector[0]
            elif len(selector) > 1:
                raise Exception(f'multiple elements returned when only one was expected within {func}')
            elif error_on_empty:
                raise Exception(f'no elements returned when one was expected within {func}')
            else:
                return None
            # end validation
        return wrapper
    return decorator


def handle_many_elements(error_on_empty=True):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            selector = func(*args, **kwargs)
            # begin to validate function output
            if len(selector) >= 1:
                return selector
            elif error_on_empty:
                raise Exception(f'no elements returned when one or more was expected within {func}')
            else:
                return None
        return wrapper
    return decorator
