# Python Connector Example

## Pre-requisites
- Python 3.9 or later

## Notes
The local testing scripts are set up for MacOS. Some commands will need to be adjusted for other operating systems 

## Building and Running the Connector

Run the build.sh file to install python dependencies in virtual environment. (Note: this will set up a virtual environment and install the project dependencies)
```bash
sh build.sh
```

Execute `run.sh` to run the connector
```bash
sh run.sh
```

## Local Testing
To test locally ensure you are running the connector (see above). Then run the following command:

Create a .env file in the root of the project with the following content:
```plaintext
# .env
LOCALDB_PATH=/path/fivetran-connect-py/localdb
GRPC_HOSTNAME=host.docker.internal
```
Replace `/path/fivetran-connect-py/localdb` with the absolute path to the `localdb` directory in this repo.

You can run the following commands to create the required .env file:
```bash
  touch .env
  sed -i '' -e '/^LOCALDB_PATH=/d' .env
  sed -i '' -e '/^GRPC_HOSTNAME=/d' .env
  echo "LOCALDB_PATH=$(pwd)/localdb" >> .env
  echo "GRPC_HOSTNAME=host.docker.internal" >> .env
```

```bash
docker compose build
docker compose run fivetran-connector
```

Once running you will be prompted to enter the required information to test the connector.

### Checking the results

Using DBeaver create a DuckDB connection and point it to the localdb/warehouse.db file. You will then see the tables in /warehouse/tester.