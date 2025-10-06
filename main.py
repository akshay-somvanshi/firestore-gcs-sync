import firebase_admin
from google.cloud import storage
from firebase_admin import credentials, firestore
import json
from collections import defaultdict

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
    # Safeguard: in case of an empty message (if the user still writes a message without any remaining chat searches left)
    if all('timeStamp' in msg for msg in item):
        item.sort(key=lambda msg: msg['timeStamp'], reverse=True)
    else:
        print(f"No timestamp found for user {user}")

    with open(f'{user}.json', 'w') as file:
        # Write the data (default str is for timestamp)
        json.dump(item, file, default=str, indent=2)    


# print(dict(grouped_messages))

# bucket = create_bucket("messages_bucket")
# storage_client = storage.Client()
# bucket = storage_client.get_bucket("dash-beta-e61d0-messages")
# blob = bucket.blob(f'chat/{user}/{user}.json')
# blob.upload_from_filename(f'{user}.json')
# print("Uploaded messages!")


