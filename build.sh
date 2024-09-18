#!/bin/bash
# Creates virtual environment to install the packages and to run the connector.
python3 -m venv connector_run
source connector_run/bin/activate

# install the added packages
pip install -r requirements.txt

# Generate the localdb
mkdir -p localdb

# Generates the required gRPC Python files using protos into `sdk_pb2` folder
mkdir -p sdk_pb2
python -m grpc_tools.protoc \
       --proto_path=./protos/ \
       --python_out=sdk_pb2 \
       --pyi_out=sdk_pb2 \
       --grpc_python_out=sdk_pb2 protos/*.proto
deactivate