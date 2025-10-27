import time

class RateLimiterHandler:
    def __init__(self, max_requests=50, window_seconds=60):
        self.max_requests = max_requests
        self.window = window_seconds
        self.requests = []

    def wait_if_needed(self):
        now = time.time()
        self.requests = [req_time for req_time in self.requests
                         if now - req_time < self.window]

        if len(self.requests) >= self.max_requests:
            sleep_time = self.window - (now - self.requests[0])
            time.sleep(sleep_time)
            self.requests = []

        self.requests.append(time.time())
