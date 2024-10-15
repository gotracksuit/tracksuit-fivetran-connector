import pytest
from unittest.mock import Mock
from metric_syncer import MetricSyncerRepo, MetricSyncer
from data_classes import FunnelMetrics


def mock_funnel_metric(
        id=1,
        account_brand_id=2,
        brand_id=1,
        brand_name="brand_name",
        filter="filter",
        filter_type="filter_type",
        wave_date="10/10/2020",
        category_name="category_name",
        geography_name="geography_name",
        base=1,
        weight=1.0,
        base_weight=1.0,
        percentage=1.0,
        question_type="question_type"
):
    return FunnelMetrics(
        id=id,
        account_brand_id=account_brand_id,
        brand_id=brand_id,
        brand_name=brand_name,
        filter=filter,
        filter_type=filter_type,
        wave_date=wave_date,
        category_name=category_name,
        geography_name=geography_name,
        base=base,
        weight=weight,
        base_weight=base_weight,
        percentage=percentage,
        question_type=question_type
    )


def test_syncs_data_for_all_accounts_if_no_accounts_passed_in():
    mock_repo = Mock(spec=MetricSyncerRepo)
    mock_funnel_metric_acc_1 = mock_funnel_metric(account_brand_id=1)
    mock_funnel_metric_acc_2 = mock_funnel_metric(account_brand_id=2)

    mock_repo.fetch_account_brand_ids_for_client.return_value = [1, 2]
    mock_repo.fetch_available_dates.return_value = [
        "2020-01-01", "2020-02-01", "2020-03-01"]
    mock_repo.fetch_funnel_data.side_effect = [
        [mock_funnel_metric_acc_1], [mock_funnel_metric_acc_2]]

    result = MetricSyncer(repo=mock_repo).sync_for(None, "2020-01-01")

    mock_repo.fetch_funnel_data.assert_any_call(1, "2020-02-01", "2020-03-01")
    mock_repo.fetch_funnel_data.assert_any_call(2, "2020-02-01", "2020-03-01")

    assert mock_funnel_metric_acc_1 in result
    assert mock_funnel_metric_acc_2 in result
    assert len(result) == 2


def test_skips_any_passed_in_accounts_that_do_not_match_the_accounts_available_for_the_client():
    mock_repo = Mock(spec=MetricSyncerRepo)
    funnel_metric = mock_funnel_metric(account_brand_id=1)

    mock_repo.fetch_account_brand_ids_for_client.return_value = [1]
    mock_repo.fetch_available_dates.return_value = [
        "2020-01-01", "2020-02-01", "2020-03-01"]
    mock_repo.fetch_funnel_data.return_value = [funnel_metric]

    syncer = MetricSyncer(repo=mock_repo)

    funnel_metrics = syncer.sync_for([1, 2], "2020-01-01")

    mock_repo.fetch_funnel_data.assert_called_once_with(
        1, "2020-02-01", "2020-03-01")
    assert funnel_metrics == [funnel_metric]


def test_raised_if_no_accounts_found():
    mock_repo = Mock(spec=MetricSyncerRepo)
    mock_repo.fetch_account_brand_ids_for_client.return_value = []

    syncer = MetricSyncer(repo=mock_repo)

    with pytest.raises(ValueError, match="no accounts found"):
        syncer.sync_for([1], "2020-01-01")


def test_raises_if_any_account_sync_fails():
    mock_repo = Mock(spec=MetricSyncerRepo)
    mock_repo.fetch_account_brand_ids_for_client.return_value = [1]
    mock_repo.fetch_available_dates.return_value = [
        "2020-01-01", "2020-02-01", "2020-03-01"]
    mock_repo.fetch_funnel_data.side_effect = Exception("Failed to sync")

    syncer = MetricSyncer(repo=mock_repo)

    with pytest.raises(Exception, match="Failed to sync"):
        syncer.sync_for([1], "2020-01-01")


def test_skips_account_if_there_is_no_wave_dates_past_the_sync_from_date():
    mock_repo = Mock(spec=MetricSyncerRepo)
    mock_repo.fetch_account_brand_ids_for_client.return_value = [1]
    mock_repo.fetch_available_dates.return_value = [
        "2020-01-01", "2020-02-01", "2020-03-01"]

    syncer = MetricSyncer(repo=mock_repo)

    funnel_metrics = syncer.sync_for([1], "2020-04-01")

    assert funnel_metrics == []

    mock_repo.fetch_funnel_data.assert_not_called()


def test_syncs_data_for_any_wave_dates_past_the_last_synced_date():
    mock_repo = Mock(spec=MetricSyncerRepo)
    mock_repo.fetch_account_brand_ids_for_client.return_value = [1]
    mock_repo.fetch_available_dates.return_value = [
        "2019-12-01", "2020-01-01", "2020-02-01", "2020-03-01"]
    mock_repo.fetch_funnel_data.return_value = "funnel_metrics"

    MetricSyncer(repo=mock_repo).sync_for([1], "2019-12-01")

    mock_repo.fetch_funnel_data.assert_called_once_with(
        1, "2020-01-01", "2020-03-01")


def test_syncs_data_for_a_single_month_if_only_one_month_has_passed_since_the_last_sync():
    mock_repo = Mock(spec=MetricSyncerRepo)
    mock_repo.fetch_account_brand_ids_for_client.return_value = [1]
    mock_repo.fetch_available_dates.return_value = ["2020-01-01", "2020-02-01"]
    mock_repo.fetch_funnel_data.return_value = "funnel_metrics"

    MetricSyncer(repo=mock_repo).sync_for([1], "2020-01-01")

    mock_repo.fetch_funnel_data.assert_called_once_with(
        1, "2020-02-01", "2020-02-01")
