import concurrent
import requests
from datetime import datetime
from dateutil.relativedelta import relativedelta
from concurrent.futures import ThreadPoolExecutor
import jwt
import os
from data_classes import json_to_funnel_metrics


def decode_jwt(token):
    try:
        decoded = jwt.decode(token, options={
            "verify_signature": False})
        return decoded
    except jwt.DecodeError as e:
        print(f"Error decoding JWT: {e}")
        return {}


class MetricFetcherRepo:
    def __init__(self, jwt_token):
        self.session = requests.Session()
        self.token = decode_jwt(jwt_token)
        if os.getenv("ENV") == "local":
            print("Using local environment")
            self.base_url = "https://dev.api.gotracksuit.com/v1"
        else:
            print("Using prod environment")
            self.base_url = "https://prod.api.gotracksuit.com/v1"

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

    def fetch_funnel_data(self, account_brand_id, start_date, end_date, filter_type):
        print(
            f"Fetching data for {account_brand_id} between {start_date}--{end_date}"
        )

        response = self.session.get(
            f"{self.base_url}/bulk/funnel/{account_brand_id}?waveStartDate={start_date}&waveEndDate={end_date}&filterType={filter_type}"
        )

        try:
            response.raise_for_status()
        except TimeoutError:
            raise TimeoutError(
                "request timed out, the sync has been aborted. If the issue persists, please try again later")
        except Exception:
            print(
                "failed to retrieve data for {account_brand_id} between {start_date}--{end_date}, sync has been skipped")
            return []

        print(
            f"Data fetched for {account_brand_id} between {start_date}--{end_date}"
        )

        return json_to_funnel_metrics(response.json())


class MetricFetcher:
    def __init__(self, repo: MetricFetcherRepo):
        self.repo = repo

    @staticmethod
    def __add_one_month(date):
        month_added = datetime.strptime(
            date, "%Y-%m-%d") + relativedelta(months=1)
        return month_added.strftime("%Y-%m-%d")

    def account_brand_ids_to_sync(self, account_brand_ids):
        account_brand_ids_for_client = self.repo.fetch_account_brand_ids_for_client()
        if len(account_brand_ids_for_client) == 0:
            raise ValueError("no accounts found")

        if account_brand_ids is None:
            return account_brand_ids_for_client

        valid_accounts_to_sync = []
        for account_brand_id in account_brand_ids:
            if int(account_brand_id) not in account_brand_ids_for_client:
                print(
                    f"you do not have premission to sync {account_brand_id}, skipping")
                continue

            valid_accounts_to_sync.append(account_brand_id)

        print(f"syncing account brands {valid_accounts_to_sync}")
        return valid_accounts_to_sync

    def wave_range_to_sync(self, account_brand_ids, last_synced_date):
        results = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {
                executor.submit(
                    self.repo.fetch_available_dates,
                    account_brand_id,
                ): account_brand_id
                for account_brand_id in account_brand_ids}

            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                results.append(result)

        sorted_dates = sorted([
            date for result in results
            for date in result
        ])

        if len(sorted_dates) == 0:
            raise ValueError(
                f"there is no data for any of the account brands {account_brand_ids} seleceted, try again later")

        if last_synced_date is None:
            return {"from": sorted_dates[0], "to": sorted_dates[-1]}

        if not any(date > last_synced_date for date in sorted_dates):
            raise ValueError(
                f"skipping sync for account brands {account_brand_ids} as it does not have data since the last sync on {last_synced_date}"
            )

        return {"from": self.__add_one_month(last_synced_date), "to": sorted_dates[-1]}

    def __get_funnel_data(self, account_brand_id, from_date, to_date, filter_type):
        print(f"syncing metrics for {account_brand_id}")
        return self.repo.fetch_funnel_data(
            account_brand_id,
            from_date,
            to_date,
            filter_type
        )

    def fetch_for(self, account_brand_ids, from_date, to_date, filter_type):
        print(
            f"Fetching account brands {account_brand_ids} between {from_date}--{to_date}, filtered by {filter_type}")

        if filter_type == "All":
            filter_type = ""

        results = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {
                executor.submit(
                    self.__get_funnel_data,
                    account_brand_id,
                    from_date,
                    to_date,
                    filter_type
                ): account_brand_id
                for account_brand_id in account_brand_ids}

            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                results.append(result)

        print("metrics fetched")
        metrics = [
            metric for result in results
            if result is not None
            for metric in result
        ]

        print(
            f"All data fetched successfully. {len(metrics)} records found."
        )

        return sorted(metrics, key=lambda x: (x.wave_date, x.id))
