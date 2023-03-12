from flask import jsonify
from functools import update_wrapper
from datetime import datetime, timedelta


class RateLimiter:
    def __init__(self, limit, interval):
        self.limit = limit
        self.interval = interval
        self.requests = []

    def __call__(self, func):
        def wrapper(*args, **kwargs):
            now = datetime.now()
            # remove old requests from list
            self.requests = [r for r in self.requests if r > now - timedelta(seconds=self.interval)]
            if len(self.requests) >= self.limit:
                return jsonify({"message": "Rate limit exceeded. Try again later."}), 429
            self.requests.append(now)
            return func(*args, **kwargs)

        return update_wrapper(wrapper, func)
