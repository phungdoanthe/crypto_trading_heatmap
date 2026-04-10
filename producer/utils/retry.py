import time
from functools import wraps
from kafka import errors


def retry(exceptions, retries=10, delay=5):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(1, retries + 1):
                try:
                    return func(*args, **kwargs)

                except exceptions as e:
                    print(f"[Attempt {attempt}] Error: {e}")
                    if attempt == retries:
                        raise
                    time.sleep(delay)

        return wrapper
    return decorator