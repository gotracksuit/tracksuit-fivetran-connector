import grpc
from concurrent import futures
import json
import sys

from api import Api

sys.path.append('sdk_pb2')

from sdk_pb2 import connector_sdk_pb2_grpc
from sdk_pb2 import common_pb2
from sdk_pb2 import connector_sdk_pb2


class ConnectorService(connector_sdk_pb2_grpc.ConnectorServicer):
    def __init__(self):
        self.api = None

    def ConfigurationForm(self, request, context):
        form_fields = common_pb2.ConfigurationFormResponse(schema_selection_supported=False,
                                                           table_selection_supported=False)
        # TODO: description
        form_fields.fields.add(name="jwt", label="JWT Token", description="Can be found at xyz", required=True,
                               text_field=common_pb2.TextField.Password)

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
        t1.columns.add(name="id", type=common_pb2.DataType.INT, primary_key=True)
        t1.columns.add(name="account_brand_id", type=common_pb2.DataType.INT)
        t1.columns.add(name="brand_id", type=common_pb2.DataType.INT)
        t1.columns.add(name="brand_name", type=common_pb2.DataType.STRING)
        t1.columns.add(name="filter", type=common_pb2.DataType.STRING)
        t1.columns.add(name="filter_type", type=common_pb2.DataType.STRING)
        t1.columns.add(name="wave_date", type=common_pb2.DataType.STRING)
        t1.columns.add(name="question_type", type=common_pb2.DataType.STRING)
        t1.columns.add(name="category_id", type=common_pb2.DataType.INT)
        t1.columns.add(name="category_name", type=common_pb2.DataType.STRING)
        t1.columns.add(name="geography_id", type=common_pb2.DataType.INT)
        t1.columns.add(name="geography_name", type=common_pb2.DataType.STRING)
        t1.columns.add(name="base", type=common_pb2.DataType.STRING)
        t1.columns.add(name="weight", type=common_pb2.DataType.DOUBLE)
        t1.columns.add(name="base_weight", type=common_pb2.DataType.DOUBLE)
        t1.columns.add(name="percentage", type=common_pb2.DataType.DOUBLE)

        return connector_sdk_pb2.SchemaResponse(without_schema=table_list)

    def Update(self, request, context):
        state_json = "{}"
        if request.HasField('state_json'):
            state_json = request.state_json

        state = json.loads(state_json)
        if state.get("cursor") is None:
            state["cursor"] = 0

        print("State JSON: ", state_json)
        print("State: ", state)
        print("Starting Sync")

        jwt_token = request.configuration.get("jwt", "")

        if True:
            operation = connector_sdk_pb2.Operation()
            funnel_metrics = Api(jwt_token).get_funnel_metrics_for_initial_sync("2024-04-01", "2024-06-01")

            for metric in funnel_metrics:
                print(f"Upserting record {metric.brand_name}")
                record = connector_sdk_pb2.Record()
                record.type = common_pb2.OpType.UPSERT
                record.table_name = "funnel_metrics"

                val_id = common_pb2.ValueType()
                val_id.int = metric.id
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

                val_population = common_pb2.ValueType()
                val_population.int = metric.population
                record.data["population"].CopyFrom(val_population)

                val_category = common_pb2.ValueType()
                val_category.int = metric.category
                record.data["category"].CopyFrom(val_category)

                val_geography = common_pb2.ValueType()
                val_geography.int = metric.geography
                record.data["geography"].CopyFrom(val_geography)

                val_base = common_pb2.ValueType()
                val_base.string = metric.base
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
                print("Upserting now record")
                yield connector_sdk_pb2.UpdateResponse(operation=operation)

        log = connector_sdk_pb2.LogEntry()
        log.level = connector_sdk_pb2.LogLevel.INFO
        yield connector_sdk_pb2.UpdateResponse(log_entry=log)


def start_server():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    connector_sdk_pb2_grpc.add_ConnectorServicer_to_server(ConnectorService(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    print("Server started...")
    server.wait_for_termination()
    print("Server terminated.")


if __name__ == '__main__':
    print("Starting the server...")
    start_server()
