from metric_fetcher import MetricFetcher, MetricFetcherRepo
from metric_syncer import MetricSyncer, MetricSyncerRepo
import grpc
from concurrent import futures
import json
import sys
import argparse
from dotenv import load_dotenv
import os
from logger import sumoLogger

script_dir = os.path.dirname(os.path.abspath(__file__))
target_path = os.path.join(script_dir, 'sdk_pb2')
sys.path.append(target_path)

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

        # used for testing the connection locally
        form_fields.tests.add(name="connection_test", label="Tests connection")

        return form_fields

    def Test(self, request, context):
        test_name = request.name
        print("Test name: ", test_name)
        if test_name == "connection_test":
            try:
                jwt_token = request.configuration.get("jwt", "")
                account_brand_ids_requested = self.account_brand_ids_requested(request.configuration.get(
                    "account_brand_ids", ''))
                fetcherRepo = MetricFetcherRepo(jwt_token)
                fetcher = MetricFetcher(fetcherRepo)

                fetcher.account_brand_ids_to_sync(
                    account_brand_ids_requested)

                return common_pb2.TestResponse(success=True)
            except Exception as e:
                return common_pb2.TestResponse(success=False, failure=str(e))

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

    def account_brand_ids_requested(self, account_brand_ids):
        if account_brand_ids == "":
            return None

        return account_brand_ids.split(",")

    def Update(self, request, context):
        try:
            state = {}
            if request.HasField('state_json'):
                state_json = request.state_json
                state = json.loads(state_json)

            jwt_token = request.configuration.get("jwt", "")
            account_brand_ids_requested = self.account_brand_ids_requested(request.configuration.get(
                "account_brand_ids", ''))

            print(account_brand_ids_requested)

            filters = request.configuration.get("filters", None)

            last_known_synced_record = state.get(
                "last_known_synced_record", None)
            last_date_synced_to = state.get(
                "last_date_synced_to", None)

            fetcher_repo = MetricFetcherRepo(jwt_token)
            fetcher = MetricFetcher(fetcher_repo)

            operation = connector_sdk_pb2.Operation()
            syncer_repo = MetricSyncerRepo(
                common_pb2, connector_sdk_pb2, operation)
            syncer = MetricSyncer(syncer_repo)

            account_brand_ids = fetcher.account_brand_ids_to_sync(
                account_brand_ids_requested)

            wave_range = fetcher.wave_range_to_sync(
                account_brand_ids, last_date_synced_to)

            if wave_range is None:
                return

            print("Wave Range to sync: ", wave_range)

            funnel_metrics = fetcher.fetch_for(
                account_brand_ids, wave_range["from"], wave_range["to"], filters)

            yield from syncer.sync(funnel_metrics, state, last_known_synced_record, wave_range["to"])
        except Exception as e:
            sumoLogger.error(str(e))
            raise e


def start_server():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=50051,
                        help="The server port")
    args = parser.parse_args()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    connector_sdk_pb2_grpc.add_ConnectorServicer_to_server(
        ConnectorService(), server)
    server.add_insecure_port(f'[::]:{args.port}')
    server.start()
    print(f"Server started at port {args.port}...")
    server.wait_for_termination()
    print("Server terminated.")


if __name__ == '__main__':
    print("Starting the server...")
    start_server()
