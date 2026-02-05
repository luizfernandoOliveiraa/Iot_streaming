import logging
from zerobus.sdk.sync import ZerobusSdk
from zerobus.sdk.shared import RecordType, StreamConfigurationOptions, TableProperties
from datetime import timezone, datetime
import dotenv
import os

dotenv.load_dotenv()


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

## configuration for the stream and table

server_endpoint = os.getenv("DATABRICKS_ENDPOINT")
workspace_url = os.getenv("WORKSPACE_URL")
table_name = os.getenv("TABLE_NAME")
client_id = os.getenv("CLIENT_ID")
client_secret = os.getenv("CLIENT_SECRET")

# Initializing the Zerobus SDK

sdk = ZerobusSdk(server_endpoint, workspace_url)

table_properties = TableProperties(table_name)

options = StreamConfigurationOptions(record_type=RecordType.JSON)

stream = sdk.create_stream(client_id, client_secret, table_properties, options)

try:
    for i in range(10):
        ts_micros = int(datetime.now(timezone.utc).timestamp() * 1_000_000)

        record = {
            "event_id": f"event_{i}",
            "device_id": f"device_{i}",
            "temperature": 20.2 + i,
            "vibration": 0.5 + i,
            "event_timestamp": ts_micros,
            "ingestion_timestamp": ts_micros,
        }
        ack = stream.ingest_record(record)
        logging.info("Record %d ingested with ack: %s", i, ack)

finally:
    stream.close()
