import os
import logging
import azure.functions as func
from azure.cosmos import CosmosClient, exceptions
from azure.storage.blob import BlobServiceClient
from datetime import datetime, timedelta

def main(timer: func.TimerRequest) -> None:
    # --- Configuration ---
    COSMOS_DB_ENDPOINT = os.environ["COSMOS_DB_ENDPOINT"]
    COSMOS_DB_KEY = os.environ["COSMOS_DB_KEY"]
    STORAGE_CONNECTION_STRING = os.environ["STORAGE_CONNECTION_STRING"]
    DATABASE_NAME = "BillingDB"
    CONTAINER_NAME = "BillingRecords"
    ARCHIVE_CONTAINER_NAME = "archived-billing-records"

    # --- Initialize Clients ---
    cosmos_client = CosmosClient(COSMOS_DB_ENDPOINT, COSMOS_DB_KEY)
    database = cosmos_client.get_database_client(DATABASE_NAME)
    container = database.get_container_client(CONTAINER_NAME)

    blob_service_client = BlobServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
    blob_container_client = blob_service_client.get_container_client(ARCHIVE_CONTAINER_NAME)

    # 1. Define the cutoff date (3 months ago)
    cutoff_date = datetime.utcnow() - timedelta(days=90)
    cutoff_timestamp = int(cutoff_date.timestamp())

    # 2. Query Cosmos DB for records older than the cutoff
    query = f"SELECT * FROM c WHERE c._ts < {cutoff_timestamp}"
    
    try:
        old_records = list(container.query_items(query, enable_cross_partition_query=True))
        logging.info(f"Found {len(old_records)} records to archive.")

        for record in old_records:
            record_id = record.get("id")
            partition_key = record.get("your_partition_key") # IMPORTANT: Replace with your actual partition key field

            if not record_id or not partition_key:
                logging.warning(f"Skipping record due to missing id or partition key: {record}")
                continue

            try:
                # 3. Copy the record to Blob Storage
                blob_client = blob_container_client.get_blob_client(f"{record_id}.json")
                blob_client.upload_blob(str(record), overwrite=True)

                # 4. Verify blob creation and delete from Cosmos DB
                if blob_client.exists():
                    container.delete_item(item=record_id, partition_key=partition_key)
                    logging.info(f"Successfully archived and deleted record: {record_id}")

            except Exception as e:
                logging.error(f"Failed to process record {record_id}: {e}")

    except exceptions.CosmosResourceNotFoundError:
        logging.info("Container or database not found.")
    except Exception as e:
        logging.error(f"An error occurred during the archival process: {e}")
