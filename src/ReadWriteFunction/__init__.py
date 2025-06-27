import os
import logging
import json
import azure.functions as func
from azure.cosmos import CosmosClient, exceptions
from azure.storage.blob import BlobServiceClient

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

def main(req: func.HttpRequest) -> func.HttpResponse:
    if req.method == "POST":
        return create_record(req)
    elif req.method == "GET":
        return get_record(req)
    else:
        return func.HttpResponse("Method not allowed.", status_code=405)

def create_record(req: func.HttpRequest) -> func.HttpResponse:
    try:
        req_body = req.get_json()
        container.create_item(body=req_body)
        return func.HttpResponse(json.dumps(req_body), status_code=201, mimetype="application/json")
    except ValueError:
        return func.HttpResponse("Invalid JSON.", status_code=400)
    except Exception as e:
        logging.error(f"Error creating record: {e}")
        return func.HttpResponse("Could not create record.", status_code=500)

def get_record(req: func.HttpRequest) -> func.HttpResponse:
    record_id = req.route_params.get("id")
    if not record_id:
        return func.HttpResponse("Please provide a record ID.", status_code=400)
    
    # IMPORTANT: Replace with your actual partition key logic.
    # For this example, we assume the id is also the partition key.
    partition_key = record_id 
    
    # 1. Try to get from Cosmos DB first
    try:
        item = container.read_item(item=record_id, partition_key=partition_key)
        return func.HttpResponse(json.dumps(item), mimetype="application/json")
    except exceptions.CosmosResourceNotFoundError:
        # 2. If not in Cosmos DB, try Blob Storage
        logging.info(f"Record {record_id} not in Cosmos DB. Checking archive.")
        try:
            blob_client = blob_container_client.get_blob_client(f"{record_id}.json")
            archived_record = blob_client.download_blob().readall()
            return func.HttpResponse(archived_record, mimetype="application/json")
        except Exception:
            return func.HttpResponse("Record not found in hot or cold storage.", status_code=404)
    except Exception as e:
        logging.error(f"Error retrieving record {record_id}: {e}")
        return func.HttpResponse("An error occurred.", status_code=500)
