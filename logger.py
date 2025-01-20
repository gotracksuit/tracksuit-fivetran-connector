import logging
import requests


class SumoLogger(logging.Handler):
    def __init__(self, level=logging.NOTSET):
        super().__init__(level)
        self.endpoint_url = ""  # TODO: implement

    def error(self, message):
        print("ERROR:", message)
        self.log(logging.ERROR, message)

    def emit(self, record):
        try:
            log_entry = self.format(record)
            headers = {
                "Content-Type": "text/plain",
            }
            requests.post(self.endpoint_url, data=log_entry, headers=headers)
        except Exception:
            self.handleError(record)


sumoLogger = SumoLogger()
