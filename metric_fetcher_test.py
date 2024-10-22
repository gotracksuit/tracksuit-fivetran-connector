import pytest
from unittest.mock import Mock
from metric_fetcher import MetricFetcherRepo, MetricFetcher
from data_classes import mock_funnel_metric


class TestWaveRangeToSync:
    def test_returns_the_earliest_and_latest_wave_dates_for_all_accounts_if_no_last_synced_date_is_passed_in(self):
        mock_repo = Mock(spec=MetricFetcherRepo)
        mock_repo.fetch_account_brand_ids_for_client.return_value = [1, 2]
        mock_repo.fetch_available_dates.side_effect = [
            ["2020-02-01"],
            ["2020-01-01", "2020-02-01", "2020-03-01"]
        ]

        result = MetricFetcher(repo=mock_repo).wave_range_to_sync([1, 2], None)

        assert result == {"from": "2020-01-01", "to": "2020-03-01"}

    def test_returns_the_earliest_and_latest_wave_dates_for_all_accounts_past_the_last_synced_date(self):
        mock_repo = Mock(spec=MetricFetcherRepo)
        mock_repo.fetch_account_brand_ids_for_client.return_value = [1, 2]
        mock_repo.fetch_available_dates.side_effect = [
            ["2020-05-01"],
            ["2020-01-01", "2020-02-01", "2020-03-01"]
        ]

        result = MetricFetcher(repo=mock_repo).wave_range_to_sync(
            [1, 2], "2020-02-01")

        assert result == {"from": "2020-03-01", "to": "2020-05-01"}

    def test_returns_the_same_from_and_to_date_if_only_one_month_has_passed_since_the_last_synced_date(self):
        mock_repo = Mock(spec=MetricFetcherRepo)
        mock_repo.fetch_account_brand_ids_for_client.return_value = [1, 2]
        mock_repo.fetch_available_dates.return_value = [
            "2020-01-01", "2020-02-01"]

        result = MetricFetcher(repo=mock_repo).wave_range_to_sync(
            [1, 2], "2020-01-01")

        assert result == {"from": "2020-02-01", "to": "2020-02-01"}

    def test_raises_if_no_wave_dates_for_any_accounts_past_the_last_synced_date(self):
        mock_repo = Mock(spec=MetricFetcherRepo)
        mock_repo.fetch_account_brand_ids_for_client.return_value = [1, 2]
        mock_repo.fetch_available_dates.return_value = []

        with pytest.raises(ValueError, match="there is no data for any of the account brands \\[1, 2\\] seleceted, try again later"):
            MetricFetcher(repo=mock_repo).wave_range_to_sync([1, 2], None)

    def test_raises_if_there_is_no_data_since_the_last_synced_date_for_any_of_the_accounts(self):
        mock_repo = Mock(spec=MetricFetcherRepo)
        mock_repo.fetch_account_brand_ids_for_client.return_value = [1, 2]
        mock_repo.fetch_available_dates.return_value = ["2020-02-01"]

        with pytest.raises(ValueError, match="skipping sync for account brands \\[1, 2\\] as it does not have data since the last sync on 2020-02-01"):
            MetricFetcher(repo=mock_repo).wave_range_to_sync(
                [1, 2], "2020-02-01")

    def test_filters_out_empty_wave_dates(self):
        mock_repo = Mock(spec=MetricFetcherRepo)
        mock_repo.fetch_account_brand_ids_for_client.return_value = [1, 2]
        mock_repo.fetch_available_dates.side_effect = [
            ["2020-02-01"],
            []
        ]

        result = MetricFetcher(repo=mock_repo).wave_range_to_sync([1, 2], None)

        assert result == {"from": "2020-02-01", "to": "2020-02-01"}


class TestAccountBrandIdsToSync:
    def test_returns_all_account_brand_ids_if_no_account_brand_ids_are_passed_in(self):
        mock_repo = Mock(spec=MetricFetcherRepo)
        mock_repo.fetch_account_brand_ids_for_client.return_value = [1, 2]

        result = MetricFetcher(repo=mock_repo).account_brand_ids_to_sync(None)

        assert result == [1, 2]

    def test_raises_if_no_accounts_are_found(self):
        mock_repo = Mock(spec=MetricFetcherRepo)
        mock_repo.fetch_account_brand_ids_for_client.return_value = []

        with pytest.raises(ValueError, match="no accounts found"):
            MetricFetcher(repo=mock_repo).account_brand_ids_to_sync(None)

    def test_filters_out_account_brand_ids_that_are_not_available_for_the_client(self):
        mock_repo = Mock(spec=MetricFetcherRepo)
        mock_repo.fetch_account_brand_ids_for_client.return_value = [1]

        result = MetricFetcher(
            repo=mock_repo).account_brand_ids_to_sync([1, 2])

        assert result == [1]

    def test_returns_all_account_brand_ids_that_are_available_for_the_client(self):
        mock_repo = Mock(spec=MetricFetcherRepo)
        mock_repo.fetch_account_brand_ids_for_client.return_value = [1, 2]

        result = MetricFetcher(
            repo=mock_repo).account_brand_ids_to_sync([1, 2])

        assert result == [1, 2]


class TestFetchFor:
    def test_fetchs_data_for_all_accounts_wave_range_and_filter_passed_in(self):
        mock_repo = Mock(spec=MetricFetcherRepo)
        mock_funnel_metric_acc_1 = mock_funnel_metric(account_brand_id=1)
        mock_funnel_metric_acc_2 = mock_funnel_metric(account_brand_id=2)

        mock_repo.fetch_account_brand_ids_for_client.return_value = [1, 2]
        mock_repo.fetch_funnel_data.side_effect = [
            [mock_funnel_metric_acc_1], [mock_funnel_metric_acc_2]]

        result = MetricFetcher(repo=mock_repo).fetch_for(
            [1, 2], "2020-02-01", "2020-03-01", "Total")

        mock_repo.fetch_funnel_data.assert_any_call(
            1, "2020-02-01", "2020-03-01", "Total")
        mock_repo.fetch_funnel_data.assert_any_call(
            2, "2020-02-01", "2020-03-01", "Total")

        assert mock_funnel_metric_acc_1 in result
        assert mock_funnel_metric_acc_2 in result
        assert len(result) == 2

    def test_skips_accounts_if_they_fail_without_a_timeout(self):
        mock_repo = Mock(spec=MetricFetcherRepo)
        mock_funnel_metric_1 = mock_funnel_metric(account_brand_id=1)
        mock_funnel_metric_2 = mock_funnel_metric(account_brand_id=2)

        mock_repo.fetch_funnel_data.side_effect = [
            [mock_funnel_metric_1], []]

        result = MetricFetcher(repo=mock_repo).fetch_for(
            [1, 2], "2020-02-01", "2020-03-01", "Total")

        assert mock_funnel_metric_1 in result
        assert mock_funnel_metric_2 not in result
        assert len(result) == 1

    def test_orders_returned_metrics_by_date_and_then_their_id(self):
        mock_repo = Mock(spec=MetricFetcherRepo)
        mock_funnel_metric_1 = mock_funnel_metric(
            id="xtsraw", wave_date="2020/01/02")
        mock_funnel_metric_2 = mock_funnel_metric(
            id="aatxststst", wave_date="2921/02/01")
        mock_funnel_metric_3 = mock_funnel_metric(
            id="stxststst", wave_date="2020/01/01")
        mock_funnel_metric_4 = mock_funnel_metric(
            id="atxststst", wave_date="2020/01/01")

        mock_repo.fetch_account_brand_ids_for_client.return_value = [1]
        mock_repo.fetch_funnel_data.return_value = [
            mock_funnel_metric_2, mock_funnel_metric_3, mock_funnel_metric_1, mock_funnel_metric_4]

        result = MetricFetcher(repo=mock_repo).fetch_for(
            [1], "2020-01-01", "2020-02-01", "Total")

        assert result == [mock_funnel_metric_4,
                          mock_funnel_metric_3,
                          mock_funnel_metric_1,
                          mock_funnel_metric_2]
