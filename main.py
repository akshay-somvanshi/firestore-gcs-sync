import firebase_admin
from google.cloud import storage
from firebase_admin import credentials, firestore
import json
from collections import defaultdict
from datetime import datetime, time
import os 

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

# Automatically assigns default value to keys that dont exist -> avoid keyError
grouped_messages = defaultdict(list)

# Get all of the message documents and store it in the json
docs = db.collection('messages').get()

for doc in docs:
    # Store the messages based on the userID: every message from a userID is grouped. 
    grouped_messages[doc.to_dict()['uid']].append(doc.to_dict())

# Get todays date 
today = datetime.now()
cutoff_early = time(9, 0)
cutoff_late = time(15, 0)

# Connect to google cloud
storage_client = storage.Client()
bucket = storage_client.get_bucket("dash-beta-e61d0-messages")

for user, item in grouped_messages.items():
    # A late messages list which is instantiated per user 
    late_msgs = []

    # Safeguard: in case of an empty message (if the user still writes a message without any remaining chat searches left)
    if all('timeStamp' in msg for msg in item):
        item.sort(key=lambda msg: datetime.fromisoformat(str(msg['timeStamp'])), reverse=True)
    else:
        print(f"No timestamp found for user {user}")

    # Make the user folders 
    os.makedirs(f'chats/{user}', exist_ok=True)

    # Create a shallow copy of item, so we can remove items from it. 
    for msg in item[:]: 
        # If the message was sent after the latest time
        if(msg['timeStamp'].time() >= time(15,0)):
            late_msgs.append(msg)
            item.remove(msg)
    
    # Create the date and time strings to pass as session names
    date_str = today.date().isoformat()
    time_str = cutoff_early.strftime('%H-%M')

    # Create the local path to store the json
    local_path = f'chats/{user}/{date_str}_{time_str}.json'

    with open(local_path, 'w') as file:
        # Write the data (default str is for timestamp)
        json.dump(item, file, default=str, indent=2)    

    # Create a similar path in GCS and store the files there
    gcs_path = f'chats/{user}/{date_str}_{time_str}.json'
    blob = bucket.blob(gcs_path)
    blob.upload_from_filename(local_path)

    time_str = cutoff_late.strftime('%H-%M')

    # If the user has no late messages, dont write any file
    if late_msgs:
        # Update the local path and GCS path for late messages 
        local_path = f'chats/{user}/{date_str}_{time_str}.json'
        gcs_path = f'chats/{user}/{date_str}_{time_str}.json'

        with open(local_path, 'w') as file:
            json.dump(late_msgs, file, default=str, indent=2)

        blob = bucket.blob(gcs_path)
        blob.upload_from_filename(local_path)

print("Uploaded messages!")


