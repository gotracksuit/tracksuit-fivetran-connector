import concurrent
import requests
from datetime import datetime
from dateutil.relativedelta import relativedelta
from concurrent.futures import ThreadPoolExecutor
import jwt

from data_classes import json_to_funnel_metrics


def decode_jwt(token):
    try:
        decoded = jwt.decode(token, options={
            "verify_signature": False})
        return decoded
    except jwt.DecodeError as e:
        print(f"Error decoding JWT: {e}")
        return {}


class MetricSyncerRepo:
    def __init__(self, jwt_token):
        self.session = requests.Session()
        self.token = decode_jwt(jwt_token)  # TODO: naming
        # TODO: PROD URL move to config
        self.base_url = "https://dev.api.gotracksuit.com/v1"

        self.session.headers.update({
            'Authorization': f'Bearer {jwt_token}',
            'Content-Type': 'application/json',
        })

    def fetch_account_brand_ids_for_client(self):
        return self.token.get('accountBrands', [])

    def fetch_available_dates(self, account_brand_id):
        print(
            f"Fetching available wave dates for {account_brand_id}"
        )

        filter_response = self.session.get(
            f"{self.base_url}/funnel/filters?accountBrandId={account_brand_id}"
        )
        filter_response.raise_for_status()
        wave_dates = filter_response.json().get('waveDates', [])

        return wave_dates

    def fetch_funnel_data(self, account_brand_id, start_date, end_date):
        print(
            f"Fetching data for {account_brand_id} between {start_date}--{end_date}"
        )

        response = self.session.get(
            f"{self.base_url}/bulk/funnel/{account_brand_id}?waveStartDate={start_date}&waveEndDate={end_date}"
        )

        # TODO: fail is we timeout - all else should pass
        try:
            response.raise_for_status()
        except Exception:
            print(
                "failed to retrieve data for {account_brand_id} between {start_date}--{end_date}, sync has been skipped")
            return []

        print(
            f"Data fetched for {account_brand_id} between {start_date}--{end_date}"
        )

        return json_to_funnel_metrics(response.json())


class MetricSyncer:
    def __init__(self, repo: MetricSyncerRepo):
        self.repo = repo

    def get_funnel_data(self, account_brand_id, last_synced_date):
        available_wave_dates = self.repo.fetch_available_dates(
            account_brand_id)
        if not any(date > last_synced_date for date in available_wave_dates):
            print(
                f"Skipping {account_brand_id} as it does not have data from {last_synced_date}"
            )
            return None

        last_available_wave_date = available_wave_dates[-1]
        return self.repo.fetch_funnel_data(
            account_brand_id,
            self.__add_one_month(last_synced_date),
            last_available_wave_date
        )

    @staticmethod
    def __add_one_month(date):
        month_added = datetime.strptime(
            date, "%Y-%m-%d") + relativedelta(months=1)
        return month_added.strftime("%Y-%m-%d")

    @staticmethod
    def __account_brand_ids_to_sync(account_brand_ids, account_brand_ids_for_client):
        if account_brand_ids is None:
            return account_brand_ids_for_client

        return [account_brand_id for account_brand_id in account_brand_ids if
                account_brand_id in account_brand_ids_for_client]

    def sync_for(self, account_brand_ids, from_date):
        print("Fetching account brands")

        account_brand_ids_for_client = self.repo.fetch_account_brand_ids_for_client()
        if len(account_brand_ids_for_client) == 0:
            raise ValueError("no accounts found")

        account_brand_ids_for_sync = self.__account_brand_ids_to_sync(
            account_brand_ids, account_brand_ids_for_client)

        results = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {
                executor.submit(
                    self.get_funnel_data,
                    account_brand_id,
                    from_date,
                ): account_brand_id
                for account_brand_id in account_brand_ids_for_sync}

            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                results.append(result)

        metrics = [
            metric for result in results
            if result is not None for metric in result
        ]

        print(
            f"All data fetched successfully. {len(metrics)} records found."
        )

        return metrics
