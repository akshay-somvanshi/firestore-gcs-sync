from google.cloud import bigquery, documentai_v1, storage
import uuid
from google.protobuf.json_format import MessageToJson
from google.protobuf.json_format import MessageToDict
from datetime import datetime
import json

# Variables for bigquery
project_id = 'dash-beta-e61d0'
dataset_id = 'dash_beta_database'
table = 'document'

# Variables for documentAI
location = 'eu'
processor_id = '77891d67fed69b99'
bucker_name = 'dash-beta-e61d0.firebasestorage.app'
file_path = 'gs://dash-beta-e61d0.firebasestorage.app/'
mime_type = 'application/pdf'

# Connect to the clients
client_bq = bigquery.Client(project=project_id)
client_storage = storage.Client()
# The client defaults to us endpoint -> must configure to eu. 
client_docAi = documentai_v1.DocumentProcessorServiceClient(
    client_options={"api_endpoint": "eu-documentai.googleapis.com"}
)

# Retrieve the bucket with user files 
bucket = client_storage.get_bucket(bucker_name)
users = bucket.list_blobs()

for user in users:
    # Blob to binary for processing 
    url = user.name
    # DocumentAI is only used to parse pdfs
    if not url.endswith(".pdf"):
        continue

    parts = url.split("/")
    # The string is structured as users/user_id/uploads/file.pdf
    user_id = parts[1]

    # Get the processor path
    processor_name = client_docAi.processor_path(project_id, location, processor_id)

    # Read file bytes
    pdf_bytes = user.download_as_bytes()

    # Process with Document AI
    request = {
        "name": processor_name,
        "raw_document": {"content": pdf_bytes, "mime_type": "application/pdf"},
    }

    result = client_docAi.process_document(request=request)

    # Convert to JSON string
    json_string = type(result).to_json(result)

    # Parse into a Python dict
    full_json = json.loads(json_string)

    trimmed_json = {
    "document": {
        "text": full_json["document"].get("text", ""),
        }
    }

    document_id = str(uuid.uuid4())

    row_to_insert = [{
    "document_id": document_id,
    "document_type": "electricity bill",
    "raw_data": json.dumps(trimmed_json),  # from MessageToJson
    "uploaded_time": datetime.utcnow().isoformat(),
    "user_id": user_id
    }]

    # truncate = "TRUNCATE TABLE `dash-beta-e61d0.dash_beta_database.document`;"
    # result = client_bq.query(truncate).result()

    table_ref = client_bq.dataset(dataset_id).table(table)
    # client_bq.insert_rows_json(table_ref, row_to_insert)
    errors = client_bq.insert_rows_json(table_ref, row_to_insert)
    if errors:
        print("Insert errors:", errors)
    else:
        print("Insert successful")


    
