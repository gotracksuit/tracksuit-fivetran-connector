import grpc
from concurrent import futures
import json
import sys

from api import Api
from data_classes import FunnelMetrics

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
        t1.columns.add(name="wave_date", type=common_pb2.DataType.STRING)
        t1.columns.add(name="brand_id", type=common_pb2.DataType.INT)
        t1.columns.add(name="brand_name", type=common_pb2.DataType.STRING)
        t1.columns.add(name="is_account_brand", type=common_pb2.DataType.STRING)
        t1.columns.add(name="question_type", type=common_pb2.DataType.STRING)
        t1.columns.add(name="percentage", type=common_pb2.DataType.DOUBLE)
        t1.columns.add(name="population", type=common_pb2.DataType.INT)
        t1.columns.add(name="category_id", type=common_pb2.DataType.INT)
        t1.columns.add(name="category_name", type=common_pb2.DataType.STRING)
        t1.columns.add(name="geography_id", type=common_pb2.DataType.INT)
        t1.columns.add(name="geography_name", type=common_pb2.DataType.STRING)
        t1.columns.add(name="sample_size_quality", type=common_pb2.DataType.STRING)

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
            funnel_metrics = [
                FunnelMetrics(
                    account_brand_id=1,
                    wave_date="2024-06-01",
                    brand_id=101,
                    brand_name="Brand1",
                    is_account_brand="True",
                    question_type="UNPROMPTED_AWARENESS",
                    percentage=50.0,
                    population=1000,
                    category_id=1,
                    category_name="Category1",
                    geography_id=1,
                    geography_name="Geography1",
                    sample_size_quality="High"
                )
            ]

            index = 1
            for metric in funnel_metrics:
                print(f"Upserting record {metric.brand_name}")
                record = connector_sdk_pb2.Record()
                record.type = common_pb2.OpType.UPSERT
                record.table_name = "funnel_metrics"

                # TODO: ID should come from the data source
                val_id = common_pb2.ValueType()
                val_id.int = index
                index += 1
                record.data["id"].CopyFrom(val_id)

                val_account_brand_id = common_pb2.ValueType()
                val_account_brand_id.int = metric.account_brand_id
                record.data["account_brand_id"].CopyFrom(val_account_brand_id)

                val_wave_date = common_pb2.ValueType()
                val_wave_date.string = metric.wave_date
                record.data["wave_date"].CopyFrom(val_wave_date)

                val_brand_id = common_pb2.ValueType()
                val_brand_id.int = metric.brand_id
                record.data["brand_id"].CopyFrom(val_brand_id)

                val_brand_name = common_pb2.ValueType()
                val_brand_name.string = metric.brand_name
                record.data["brand_name"].CopyFrom(val_brand_name)

                # TODO: is_account_brand is a boolean, how do I represent it?
                val_is_account_brand = common_pb2.ValueType()
                val_is_account_brand.string = metric.is_account_brand
                record.data["is_account_brand"].CopyFrom(val_is_account_brand)

                val_question_type = common_pb2.ValueType()
                val_question_type.string = metric.question_type
                record.data["question_type"].CopyFrom(val_question_type)

                val_percentage = common_pb2.ValueType()
                val_percentage.double = metric.percentage
                record.data["percentage"].CopyFrom(val_percentage)

                val_population = common_pb2.ValueType()
                val_population.int = metric.population
                record.data["population"].CopyFrom(val_population)

                val_category_id = common_pb2.ValueType()
                val_category_id.int = metric.category_id
                record.data["category_id"].CopyFrom(val_category_id)

                val_category_name = common_pb2.ValueType()
                val_category_name.string = metric.category_name
                record.data["category_name"].CopyFrom(val_category_name)

                val_geography_id = common_pb2.ValueType()
                val_geography_id.int = metric.geography_id
                record.data["geography_id"].CopyFrom(val_geography_id)

                val_geography_name = common_pb2.ValueType()
                val_geography_name.string = metric.geography_name
                record.data["geography_name"].CopyFrom(val_geography_name)

                val_sample_size_quality = common_pb2.ValueType()
                val_sample_size_quality.string = metric.sample_size_quality
                record.data["sample_size_quality"].CopyFrom(val_sample_size_quality)

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
