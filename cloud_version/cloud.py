import firebase_admin
from google.cloud import storage
from firebase_admin import credentials, firestore
import json
from collections import defaultdict
from datetime import datetime, time
import os 
import io
import functions_framework

def create_bucket(bucket_name):
    storage_client = storage.Client()
    bucket = storage_client.create_bucket(bucket_name)
    print(f"Bucket {bucket.name} created")
    return bucket

# Do not need credentials to log on cloud run
firebase_admin.initialize_app()

# Connect to Firestore
db = firestore.client()

@functions_framework.cloud_event
def exportMessages(cloud_event):
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

        # Create a shallow copy of item, so we can remove items from it. 
        for msg in item[:]: 
            # If the message was sent after the latest time
            if(msg['timeStamp'].time() >= time(15,0)):
                late_msgs.append(msg)
                item.remove(msg)
        
        # Create the date and time strings to pass as session names
        date_str = today.date().isoformat()
        time_str = cutoff_early.strftime('%H-%M')

        # Store the messages in memory buffers (RAM) as we cannot create local files
        buffer_early = io.BytesIO(json.dumps(item, default=str, indent=2).encode('utf-8'))

        # Create a path in GCS and store the files there
        gcs_path = f'chats/{user}/{date_str}_{time_str}.json'
        blob = bucket.blob(gcs_path)
        blob.upload_from_file(buffer_early, content_type="application/json")

        time_str = cutoff_late.strftime('%H-%M')

        # If the user has no late messages, dont write any file
        if late_msgs:
            # Update the GCS path for late messages 
            gcs_path = f'chats/{user}/{date_str}_{time_str}.json'

            buffer_late = io.BytesIO(json.dumps(late_msgs, default=str, indent=2).encode('utf-8'))

            blob = bucket.blob(gcs_path)
            blob.upload_from_file(buffer_late, content_type="application/json")

    print("Uploaded messages!")


