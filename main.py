from metric_fetcher import MetricFetcher, MetricFetcherRepo
import grpc
from concurrent import futures
import json
import sys
from dotenv import load_dotenv

sys.path.append('sdk_pb2')

from sdk_pb2 import connector_sdk_pb2, common_pb2, connector_sdk_pb2_grpc  # noqa: E402


load_dotenv()


class ConnectorService(connector_sdk_pb2_grpc.ConnectorServicer):
    def ConfigurationForm(self, request, context):
        form_fields = common_pb2.ConfigurationFormResponse(schema_selection_supported=False,
                                                           table_selection_supported=False)
        form_fields.fields.add(name="jwt",
                               label="JWT Token",
                               description="Tokens can be generated on the Tracksuit app under settings/account settings/tokens",
                               required=True,
                               text_field=common_pb2.TextField.Password
                               )
        form_fields.fields.add(name="account_brand_ids",
                               label="Account brand IDs",
                               description="""Account brand IDs that's metrics should be synced for.
                                            The account brand IDs should be seperated by a comma \\(,\\).
                                            Note: putting nothing in this field will sync data for all account brands""",
                               required=False,
                               text_field=common_pb2.TextField.PlainText
                               )
        form_fields.fields.add(name="filters",
                               label="Filters",
                               description="""Filters the funnel metrics category of funnel metrics returned.
                                                Note: Total is the metrics with no filters applied,
                                                All will return all possible results for every filter""",
                               required=False,
                               dropdown_field=common_pb2.DropdownField(
                                   dropdown_field=["All", "Total", "Age", "Gender"])
                               )

        # add setup tests
        form_fields.tests.add(name="connection_test", label="Tests connection")

        return form_fields

    def Test(self, request, context):
        configuration = request.configuration
        # Name of the test to be run
        test_name = request.name
        print("Configuration: ", configuration)
        print("Test name: ", test_name)
        return common_pb2.TestResponse(success=True)

    def Schema(self, request, context):
        table_list = common_pb2.TableList()

        t1 = table_list.tables.add(name="funnel_metrics")
        t1.columns.add(name="id", type=common_pb2.DataType.STRING,
                       primary_key=True)
        t1.columns.add(name="account_brand_id", type=common_pb2.DataType.INT)
        t1.columns.add(name="brand_id", type=common_pb2.DataType.INT)
        t1.columns.add(name="brand_name", type=common_pb2.DataType.STRING)
        t1.columns.add(name="filter", type=common_pb2.DataType.STRING)
        t1.columns.add(name="filter_type", type=common_pb2.DataType.STRING)
        t1.columns.add(name="wave_date", type=common_pb2.DataType.STRING)
        t1.columns.add(name="category_name", type=common_pb2.DataType.STRING)
        t1.columns.add(name="geography_name", type=common_pb2.DataType.STRING)
        t1.columns.add(name="base", type=common_pb2.DataType.INT)
        t1.columns.add(name="weight", type=common_pb2.DataType.DOUBLE)
        t1.columns.add(name="base_weight", type=common_pb2.DataType.DOUBLE)
        t1.columns.add(name="percentage", type=common_pb2.DataType.DOUBLE)
        t1.columns.add(name="question_type", type=common_pb2.DataType.STRING)

        return connector_sdk_pb2.SchemaResponse(without_schema=table_list)

    def Update(self, request, context):
        try:
            state_json = {}
            if request.HasField('state_json'):
                state_json = request.state_json
                state = json.loads(state_json)

            jwt_token = request.configuration.get("jwt", "")
            account_brand_ids_requested = request.configuration.get(
                "account_brand_ids", None)

            if account_brand_ids_requested is not None:
                account_brand_ids_requested = account_brand_ids_requested.split(
                    ",")

            print(account_brand_ids_requested)

            filters = request.configuration.get("filters", None)

            last_known_synced_record = state.get(
                "last_known_synced_record", None)
            last_date_synced_to = state.get(
                "last_date_synced_to", None)

            fetcherRepo = MetricFetcherRepo(jwt_token)
            fetcher = MetricFetcher(fetcherRepo)

            account_brand_ids = fetcher.account_brand_ids_to_sync(
                account_brand_ids_requested)

            wave_range = fetcher.wave_range_to_sync(
                account_brand_ids, last_date_synced_to)

            print("Wave Range: ", wave_range)

            funnel_metrics = fetcher.fetch_for(
                account_brand_ids, wave_range["from"], wave_range["to"], filters)

            metrics_since_checkpoint = 0
            sync = True
            if last_known_synced_record:
                sync = False

            for metric in funnel_metrics:
                if not sync:
                    if metric.id == last_known_synced_record:
                        print("Resuming sync")
                        sync = True
                        continue

                operation = connector_sdk_pb2.Operation()
                record = connector_sdk_pb2.Record()
                record.type = common_pb2.OpType.UPSERT
                record.table_name = "funnel_metrics"

                val_id = common_pb2.ValueType()
                val_id.string = metric.id
                record.data["id"].CopyFrom(val_id)

                val_account_brand_id = common_pb2.ValueType()
                val_account_brand_id.int = metric.account_brand_id
                record.data["account_brand_id"].CopyFrom(val_account_brand_id)

                val_brand_id = common_pb2.ValueType()
                val_brand_id.int = metric.brand_id
                record.data["brand_id"].CopyFrom(val_brand_id)

                val_brand_name = common_pb2.ValueType()
                val_brand_name.string = metric.brand_name
                record.data["brand_name"].CopyFrom(val_brand_name)

                val_filter = common_pb2.ValueType()
                val_filter.string = metric.filter
                record.data["filter"].CopyFrom(val_filter)

                val_filter_type = common_pb2.ValueType()
                val_filter_type.string = metric.filter_type
                record.data["filter_type"].CopyFrom(val_filter_type)

                val_wave_date = common_pb2.ValueType()
                val_wave_date.string = metric.wave_date
                record.data["wave_date"].CopyFrom(val_wave_date)

                val_category = common_pb2.ValueType()
                val_category.string = metric.category_name
                record.data["category_name"].CopyFrom(val_category)

                val_geography = common_pb2.ValueType()
                val_geography.string = metric.geography_name
                record.data["geography_name"].CopyFrom(val_geography)

                val_base = common_pb2.ValueType()
                val_base.int = metric.base
                record.data["base"].CopyFrom(val_base)

                val_weight = common_pb2.ValueType()
                val_weight.double = metric.weight
                record.data["weight"].CopyFrom(val_weight)

                val_base_weight = common_pb2.ValueType()
                val_base_weight.double = metric.base_weight
                record.data["base_weight"].CopyFrom(val_base_weight)

                val_percentage = common_pb2.ValueType()
                val_percentage.double = metric.percentage
                record.data["percentage"].CopyFrom(val_percentage)

                val_question_type = common_pb2.ValueType()
                val_question_type.string = metric.question_type
                record.data["question_type"].CopyFrom(val_question_type)

                operation.record.CopyFrom(record)
                yield connector_sdk_pb2.UpdateResponse(operation=operation)

                metrics_since_checkpoint += 1
                if metrics_since_checkpoint >= 100:
                    checkpoint = connector_sdk_pb2.Checkpoint()
                    state["last_known_synced_record"] = metric.id
                    checkpoint.state_json = json.dumps(state)
                    checkpoint_operation = connector_sdk_pb2.Operation()
                    checkpoint_operation.checkpoint.CopyFrom(checkpoint)
                    yield connector_sdk_pb2.UpdateResponse(operation=checkpoint_operation)
                    metrics_since_checkpoint = 0

            checkpoint = connector_sdk_pb2.Checkpoint()
            state["last_known_synced_record"] = None
            state["last_date_synced_to"] = wave_range["to"]
            checkpoint.state_json = json.dumps(state)
            checkpoint_operation = connector_sdk_pb2.Operation()
            checkpoint_operation.checkpoint.CopyFrom(checkpoint)
            yield connector_sdk_pb2.UpdateResponse(operation=checkpoint_operation)

            log = connector_sdk_pb2.LogEntry()
            log.level = connector_sdk_pb2.LogLevel.INFO
            log.message = "Sync Done"
            yield connector_sdk_pb2.UpdateResponse(log_entry=log)
        except Exception as e:
            log = connector_sdk_pb2.LogEntry()
            log.level = connector_sdk_pb2.LogLevel.SEVERE
            log.message = str(e)
            yield connector_sdk_pb2.UpdateResponse(log_entry=log)


def start_server():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    connector_sdk_pb2_grpc.add_ConnectorServicer_to_server(
        ConnectorService(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    print("Server started...")
    server.wait_for_termination()
    print("Server terminated.")


if __name__ == '__main__':
    print("Starting the server...")
    start_server()
