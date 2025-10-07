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
    
    with open(f'chats/{user}/early_session.json', 'w') as file:
        # Write the data (default str is for timestamp)
        json.dump(item, file, default=str, indent=2)    

    with open(f'chats/{user}/late_session.json', 'w') as file:
        json.dump(late_msgs, file, default=str, indent=2)

# print(dict(grouped_messages))

# bucket = create_bucket("messages_bucket")
# storage_client = storage.Client()
# bucket = storage_client.get_bucket("dash-beta-e61d0-messages")
# blob = bucket.blob(f'chat/{user}/{user}.json')
# blob.upload_from_filename(f'{user}.json')
# print("Uploaded messages!")


