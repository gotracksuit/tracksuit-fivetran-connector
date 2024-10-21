import json


class MetricSyncerRepo:
    def __init__(self, common, sdk, operation):
        self.common = common
        self.sdk = sdk
        self.operation = operation

    def get_syncable_metric(self, metric):
        self.operation = self.sdk.Operation()
        record = self.sdk.Record()
        record.type = self.common.OpType.UPSERT
        record.table_name = "funnel_metrics"

        val_id = self.common.ValueType()
        val_id.string = metric.id
        record.data["id"].CopyFrom(val_id)

        val_account_brand_id = self.common.ValueType()
        val_account_brand_id.int = metric.account_brand_id
        record.data["account_brand_id"].CopyFrom(val_account_brand_id)

        val_brand_id = self.common.ValueType()
        val_brand_id.int = metric.brand_id
        record.data["brand_id"].CopyFrom(val_brand_id)

        val_brand_name = self.common.ValueType()
        val_brand_name.string = metric.brand_name
        record.data["brand_name"].CopyFrom(val_brand_name)

        val_filter = self.common.ValueType()
        val_filter.string = metric.filter
        record.data["filter"].CopyFrom(val_filter)

        val_filter_type = self.common.ValueType()
        val_filter_type.string = metric.filter_type
        record.data["filter_type"].CopyFrom(val_filter_type)

        val_wave_date = self.common.ValueType()
        val_wave_date.string = metric.wave_date
        record.data["wave_date"].CopyFrom(val_wave_date)

        val_category = self.common.ValueType()
        val_category.string = metric.category_name
        record.data["category_name"].CopyFrom(val_category)

        val_geography = self.common.ValueType()
        val_geography.string = metric.geography_name
        record.data["geography_name"].CopyFrom(val_geography)

        val_base = self.common.ValueType()
        val_base.int = metric.base
        record.data["base"].CopyFrom(val_base)

        val_weight = self.common.ValueType()
        val_weight.double = metric.weight
        record.data["weight"].CopyFrom(val_weight)

        val_base_weight = self.common.ValueType()
        val_base_weight.double = metric.base_weight
        record.data["base_weight"].CopyFrom(val_base_weight)

        val_percentage = self.common.ValueType()
        val_percentage.double = metric.percentage
        record.data["percentage"].CopyFrom(val_percentage)

        val_question_type = self.common.ValueType()
        val_question_type.string = metric.question_type
        record.data["question_type"].CopyFrom(val_question_type)

        self.operation.record.CopyFrom(record)
        return self.sdk.UpdateResponse(operation=self.operation)

    def get_checkpoint(self, state):
        checkpoint = self.sdk.Checkpoint()
        checkpoint.state_json = json.dumps(state)
        checkpoint_operation = self.sdk.Operation()
        checkpoint_operation.checkpoint.CopyFrom(checkpoint)
        return self.sdk.UpdateResponse(operation=checkpoint_operation)

    def log(self, message):
        log = self.sdk.LogEntry()
        log.level = self.sdk.LogLevel.INFO
        log.message = message
        return self.sdk.UpdateResponse(log_entry=log)


class MetricSyncer:
    def __init__(self, repo: MetricSyncerRepo):
        self.repo = repo

    def sync_metrics(self, funnel_metrics, state, last_known_synced_record):
        sync = True
        if last_known_synced_record:
            sync = False

        metrics_since_checkpoint = 0
        for metric in funnel_metrics:
            if not sync:
                if metric.id == last_known_synced_record:
                    print("Resuming sync")
                    sync = True
                continue

            yield self.repo.get_syncable_metric(metric)
            metrics_since_checkpoint += 1
            if metrics_since_checkpoint >= 100:
                state["last_known_synced_record"] = metric.id
                yield self.repo.get_checkpoint(state)
                metrics_since_checkpoint = 0

    def sync(self, funnel_metrics, state, last_known_synced_record, to_sync):
        yield from self.sync_metrics(funnel_metrics, state, last_known_synced_record)

        state["last_known_synced_record"] = None
        state["last_date_synced_to"] = to_sync
        yield self.repo.get_checkpoint(state)

        yield self.repo.log("Sync completed")
