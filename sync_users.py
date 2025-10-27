from firebase_admin import credentials, firestore
import firebase_admin
from google.cloud import bigquery

# Authenticate and connect to firestore
cred = credentials.Certificate('serviceAccountKey.json')
firebase_admin.initialize_app(cred)
db = firestore.client()

# Connect to BigQuery
project_id = 'dash-beta-e61d0'
dataset_id = 'dash_beta_database'
table = 'user'

client_bq = bigquery.Client(project_id)

# Get all users from the users table
docs = db.collection('users').get()

for doc in docs:
    # Extract the information to add to BigQuery table
    doc_dict = doc.to_dict()
    user_id = doc_dict['uid']
    industry = doc_dict['company_industry']
    company = doc_dict['company_name']
    if(doc_dict['operate_in_uk'] == 'true'):
        region = 'UK'
    else:
        region = 'Non-UK'

    query_user = f"""
            SELECT COUNT(*) FROM `{project_id}.{dataset_id}.{table}`
            WHERE user_id = @user_id;"""
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("user_id", "STRING", user_id)
        ]
    )

    result = client_bq.query(query_user, job_config=job_config).result()
    # If there is no row returned -> user does not exist and we can insert this new user
    if(result.num_results == 0):
        insert_query = f"""
            INSERT INTO `{project_id}.{dataset_id}.{table}`
            (user_id, industry, company_name, region)
            VALUES (@user_id, @industry, @company_name, @region);"""
        
        insert_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("user_id", "STRING", user_id),
                bigquery.ScalarQueryParameter("industry", "STRING", industry),
                bigquery.ScalarQueryParameter("company_name", "STRING", company),
                bigquery.ScalarQueryParameter("region", "STRING", region)
            ]
        )
        client_bq.query(insert_query, job_config=insert_config).result() #.result() to have the query finish (asynchronous)
