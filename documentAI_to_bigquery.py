from google.cloud import bigquery, documentai_v1, storage

# Variables for bigquery
project_id = 'dash-beta- e61d0'
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
client_docAi = documentai_v1.DocumentProcessorServiceClient()

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
    # Get the processor reference
    request = documentai_v1.GetProcessorRequest(processor_name, raw_document=user)
    processor = client_docAi.get_processor(request)

    
