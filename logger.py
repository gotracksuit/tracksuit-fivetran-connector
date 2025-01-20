import logging
import requests


SUMO_LOGIC_URL = "https://collectors.au.sumologic.com/receiver/v1/http/ZaVnC4dhaV2TQKUB12dUiOO7DEirKLsOU4U6jUJRY9XIz4Bskyo4Dp6jqIy9ck6Dbk_AaS3tFqcPagLjvZQ4W5KXkn5YxRJakgF7W-D7pCznCtIkkejjvw=="


class SumoLogger(logging.Handler):
    def __init__(self, endpoint_url):
        super().__init__()
        self.endpoint_url = endpoint_url

    def emit(self, record):
        log_entry = self.format(record)
        try:
            print(log_entry.encode('utf-8'))
            response = requests.post(
                self.endpoint_url, data=log_entry.encode('utf-8'))
            response.raise_for_status()

        except requests.exceptions.RequestException as e:
            print(f"Failed to send log to Sumo Logic: {e}")


logger = logging.getLogger('sumo_logger')
logger.setLevel(logging.DEBUG)
sumo_handler = SumoLogger(SUMO_LOGIC_URL)
sumo_handler.setLevel(logging.DEBUG)

logger.addHandler(sumo_handler)
