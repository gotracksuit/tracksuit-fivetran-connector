import pytest
from unittest.mock import Mock
from data_classes import mock_funnel_metric
from metric_syncer import MetricSyncer, MetricSyncerRepo


class TestCheckpoints:
    def test_creates_a_checkpoint_every_100_metric(self):
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

    def test_will_not_create_a_checkpoint_if_there_are_less_than_100_metrics(self):
        mock_repo = Mock(spec=MetricSyncerRepo)
        syncer = MetricSyncer(mock_repo)
        metrics = [mock_funnel_metric(id=str(i+1)) for i in range(99)]

        state = {}
        last_known_synced_record = None
        to_sync = "2020-12-01"

        list(syncer.sync_metrics(
            metrics, state, last_known_synced_record))

        mock_repo.get_checkpoint.assert_not_called()

    def test_continues_from_last_known_synced_record_if_one_exists(self):
        mock_repo = Mock(spec=MetricSyncerRepo)
        syncer = MetricSyncer(mock_repo)
        metrics = [mock_funnel_metric(id=str(i+1)) for i in range(10)]

        state = {"last_known_synced_record": "5"}
        last_known_synced_record = "5"

        list(syncer.sync_metrics(
            metrics, state, last_known_synced_record))

        mock_repo.get_syncable_metric.assert_any_call(metrics[5])
        assert mock_repo.get_syncable_metric.call_count == 5
