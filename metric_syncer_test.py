import pytest
from unittest.mock import Mock
from data_classes import mock_funnel_metric
from metric_syncer import MetricSyncer, MetricSyncerRepo


def test_syncs_all_metrics():
    mock_repo = Mock(spec=MetricSyncerRepo)
    syncer = MetricSyncer(mock_repo)
    metrics = [mock_funnel_metric(id=str(i+1)) for i in range(10)]

    state = {}
    last_known_synced_record = None
    to_sync = "2020-12-01"

    list(syncer.sync_metrics(
        metrics, state, last_known_synced_record))

    assert mock_repo.get_syncable_metric.call_count == 10


def test_creates_a_checkpoint_every_100_metric():
    mock_repo = Mock(spec=MetricSyncerRepo)
    syncer = MetricSyncer(mock_repo)
    metrics = [mock_funnel_metric(id=str(i+1)) for i in range(100)]

    state = {}
    last_known_synced_record = None
    to_sync = "2020-12-01"

    list(syncer.sync_metrics(
        metrics, state, last_known_synced_record))

    mock_repo.get_checkpoint.assert_called_once_with(
        {"last_known_synced_record": "100"})


def test_will_not_create_a_checkpoint_if_there_are_less_than_100_metrics():
    mock_repo = Mock(spec=MetricSyncerRepo)
    syncer = MetricSyncer(mock_repo)
    metrics = [mock_funnel_metric(id=str(i+1)) for i in range(99)]

    state = {}
    last_known_synced_record = None

    list(syncer.sync_metrics(
        metrics, state, last_known_synced_record))

    mock_repo.get_checkpoint.assert_not_called()


def test_continues_from_last_known_synced_record_if_one_exists():
    mock_repo = Mock(spec=MetricSyncerRepo)
    syncer = MetricSyncer(mock_repo)
    metrics = [mock_funnel_metric(id=str(i+1)) for i in range(10)]

    state = {"last_known_synced_record": "5"}
    last_known_synced_record = "5"

    list(syncer.sync_metrics(
        metrics, state, last_known_synced_record))

    mock_repo.get_syncable_metric.assert_any_call(metrics[5])
    assert mock_repo.get_syncable_metric.call_count == 5


def test_starts_from_the_beginning_if_last_known_synced_record_is_not_found():
    mock_repo = Mock(spec=MetricSyncerRepo)
    syncer = MetricSyncer(mock_repo)
    metrics = [mock_funnel_metric(id=str(i+1)) for i in range(10)]

    state = {"last_known_synced_record": None}
    last_known_synced_record = None

    list(syncer.sync_metrics(
        metrics, state, last_known_synced_record))

    mock_repo.get_syncable_metric.assert_any_call(metrics[0])
    assert mock_repo.get_syncable_metric.call_count == 10


def test_saves_the_last_synced_date_when_the_sync_is_successful():
    mock_repo = Mock(spec=MetricSyncerRepo)
    syncer = MetricSyncer(mock_repo)
    metrics = [mock_funnel_metric(id=str(i+1)) for i in range(2)]

    state = {"last_known_synced_record": None}
    last_known_synced_record = None
    to_sync = "2020-12-01"

    list(syncer.sync(
        metrics, state, last_known_synced_record, to_sync))

    mock_repo.get_checkpoint.assert_called_once_with(
        {"last_known_synced_record": None, "last_date_synced_to": to_sync})


def test_does_not_save_the_last_synced_date_when_the_sync_is_not_successful():
    mock_repo = Mock(spec=MetricSyncerRepo)
    syncer = MetricSyncer(mock_repo)
    metrics = [mock_funnel_metric(id=str(i+1)) for i in range(2)]

    mock_repo.get_syncable_metric.side_effect = [ValueError("error")]

    state = {"last_known_synced_record": None}
    last_known_synced_record = None
    to_sync = "2020-12-01"

    with pytest.raises(ValueError):
        list(syncer.sync(
            metrics, state, last_known_synced_record, to_sync))

    mock_repo.get_checkpoint.assert_not_called()


def test_sets_the_last_known_synced_record_to_none_when_the_sync_is_successful():
    mock_repo = Mock(spec=MetricSyncerRepo)
    syncer = MetricSyncer(mock_repo)
    metrics = [mock_funnel_metric(id=str(i+1)) for i in range(2)]

    state = {"last_known_synced_record": "1"}
    last_known_synced_record = "1"
    to_sync = "2020-12-01"

    list(syncer.sync(
        metrics, state, last_known_synced_record, to_sync))

    assert state["last_known_synced_record"] is None
