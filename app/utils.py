# utils.py (new file for decorator)
import os
import requests
from tenacity import retry, stop_after_attempt, wait_exponential
from functools import wraps
from typing import Tuple, Callable

ALERT_WEBHOOK = os.environ.get('ALERT_WEBHOOK', '')

def send_alert(message: str):
    if ALERT_WEBHOOK:
        data = {'text': message} if 'slack' in ALERT_WEBHOOK else {'content': message}
        try:
            requests.post(ALERT_WEBHOOK, json=data)
        except Exception as e:
            print(f"Failed to send alert: {e}")

def error_handler(retries: int = 3, backoff_factor: float = 1.0, exceptions: Tuple[type(Exception), ...] = (Exception,)) -> Callable:
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        @retry(stop=stop_after_attempt(retries), wait=wait_exponential(multiplier=backoff_factor), reraise=True)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except exceptions as e:
                send_alert(f"Error in {func.__name__}: {str(e)}")
                raise
        return wrapper
    return decorator