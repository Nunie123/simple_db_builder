import datetime
from functools import wraps


def time_and_log(logger):
    def time_and_log_decarator(function):
        @wraps(function)
        def wrapper(*args, **kwargs):
            a = '' if not args else args
            ka = '' if not kwargs else kwargs
            logger.info(f'Attempting to execute `{function.__name__}` with {a}{ka}.')

            start_time = datetime.datetime.now()
            function(*args, **kwargs)
            end_time = datetime.datetime.now()
            duration = (end_time - start_time).total_seconds()

            logger.info(f'Successfully executed `{function.__name__}` with {a}{ka} in {duration} seconds.')
            return None
        return wrapper
    return time_and_log_decarator