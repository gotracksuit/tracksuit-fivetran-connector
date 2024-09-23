import concurrent

import requests
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
import jwt

from data_classes import json_to_funnel_metrics


def decode_jwt(token):
    try:
        decoded = jwt.decode(token, options={"verify_signature": False})
        return decoded
    except jwt.DecodeError as e:
        print(f"Error decoding JWT: {e}")
        return {}


class Api:
    def __init__(self, jwt):
        self.session = requests.Session()
        self.jwt = jwt
        self.session.headers.update({
            'Authorization': f'Bearer {jwt}',
            'Content-Type': 'application/json',
        })
        # TODO: PROD URL move to config
        self.base_url = "https://dev.api.gotracksuit.com/v1"

    def fetch_funnel_data(self, account_brand_id, start_date, end_date):
        print(f"Fetching data for {account_brand_id} between {start_date}--{end_date}")
        filter_response = self.session.get(f"{self.base_url}/funnel/filters?accountBrandId={account_brand_id}")
        filter_response.raise_for_status()
        wave_dates = filter_response.json().get('waveDates', [])

        # TODO: the start date doesn't matter as long as the end date is within the wave dates
        is_within_timespan = start_date in wave_dates and end_date in wave_dates
        if not is_within_timespan:
            print(f"Skipping {account_brand_id} as it does not have data for {start_date}-{end_date}")
            return None

        url = f"{self.base_url}/bulk/funnel/{account_brand_id}?waveStartDate={start_date}&waveEndDate={end_date}"
        response = self.session.get(url)
        response.raise_for_status()
        print(f"Data fetched for {account_brand_id} between {start_date}--{end_date}")
        return json_to_funnel_metrics(response.json())

    def get_funnel_metrics_for_initial_sync(self, start_date, end_date):
        try:
            print("Fetching account brands")
            account_brand_ids = decode_jwt(self.jwt).get('accountBrands', [])

            print(f"Fetching {len(account_brand_ids)} brand(s) between {start_date}--{end_date}")

            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = {
                    executor.submit(self.fetch_funnel_data, account_brand_id, start_date, end_date): account_brand_id
                    for account_brand_id in account_brand_ids}
                results = []
                for future in concurrent.futures.as_completed(futures):
                    account_brand_id = futures[future]
                    try:
                        result = future.result()
                        results.append(result)
                    except Exception as e:
                        print(f"Error processing account brand {account_brand_id}: {e}")

            print(f"All data fetched successfully. {len(results)} records found.")
            return results
        except Exception as e:
            print(e, file=sys.stderr)
            sys.exit(1)
