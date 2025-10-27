class ClinexaException(Exception):
    """ Base class for all CTPExceptions exceptions"""


class RequestTimeoutError(ClinexaException):
    def __init__(self):
        super().__init__()
        self.log = f"Request failed on page after max attempts"

