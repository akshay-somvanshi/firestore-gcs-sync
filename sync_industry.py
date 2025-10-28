from google.cloud import bigquery

project_id = 'dash-beta-e61d0'
dataset_id = 'dash_beta_database'
user_table = 'user'
document_table = 'document'

# Connect to client
client_bq = bigquery.Client(project_id)

# Get the user information including the industry
query_user = f"""
        SELECT user_id as user_id, industry as industry 
        FROM `{project_id}.{dataset_id}.{user_table}`"""

user_info = client_bq.query(query_user).result()

for row in user_info:
    user_id = row.user_id
    # Access the industry information from the query
    industry = row.industry
    match industry:
        case "Manufacturing":
            industry_table = 'industry_manufacturing'
        case "Banks":
            industry_table = 'industry_bank'

    query_document = f"""
        SELECT `{project_id}.{dataset_id}.{document_table}`.document_id as document_id, `{project_id}.{dataset_id}.{document_table}`.parsed_data as data
        FROM `{project_id}.{dataset_id}.{document_table}` 
        JOIN `{project_id}.{dataset_id}.{user_table}` 
        ON `{project_id}.{dataset_id}.{document_table}`.user_id = `{project_id}.{dataset_id}.{user_table}`.user_id
        WHERE `{project_id}.{dataset_id}.{document_table}`.user_id = @user_id"""

    job_config = bigquery.QueryJobConfig(
        query_parameters = [
            bigquery.ScalarQueryParameter("user_id", "STRING", user_id)
        ]
    )

    result = client_bq.query(query_document, job_config=job_config).result()
    print(result)
