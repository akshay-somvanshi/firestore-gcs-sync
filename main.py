import firebase_admin
from google.cloud import storage
from firebase_admin import credentials, firestore
import json

def create_bucket(bucket_name):
    storage_client = storage.Client()
    bucket = storage_client.create_bucket(bucket_name)
    print(f"Bucket {bucket.name} created")
    return bucket

# Authenticate connection to firebase
cred = credentials.Certificate("serviceAccountKey.json")
firebase_admin.initialize_app(cred)

# Connect to Firestore
db = firestore.client()

# Create a json file with an empty list and we add dictionaries as items to this list
# Json can only hold one object
messages = []
with open('messages.json', 'w') as file:
    json.dump(messages, file)

# Get all of the message documents and store it in the json
docs = db.collection('messages').get()
for doc in docs:
    # In read and write mode
    with open('messages.json', 'r+') as file:
        # Load existing data
        data = json.load(file)

        # Append new data to the list
        data.append(doc.to_dict())

        # Move cursor to beginning of file 
        file.seek(0)

        # Write the updated data (default str is for timestamp)
        json.dump(data, file, default=str)

with open("messages.json", "r") as file:
    r = json.load(file)
    print(r)

# bucket = create_bucket("messages_bucket")
storage_client = storage.Client()
bucket = storage_client.get_bucket("dash-beta-e61d0-messages")
blob = bucket.blob('message')
blob.upload_from_filename("messages.json")
print("Uploaded messages!")


