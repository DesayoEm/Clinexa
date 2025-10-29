import random as rand
from airflow.utils.log.logging_mixin import LoggingMixin


log = LoggingMixin().log

class FailureGenerator:
    """Controlled failures for testing resilience"""

    def __init__(self, enabled: bool,  failure_rate: float):
        self.enabled = enabled
        self.failure_rate = failure_rate


    def maybe_fail_extraction(self, page_num):
        if not self.enabled:
            return

        should_cause_chaos = rand.random() < self.failure_rate

        if should_cause_chaos:
            error = Exception("Delibrate error error")
            log.warning(f"Failure generator strikes at page {page_num}!")
            raise error
        else:
            log.debug(f"Failure generator spares page {page_num}")