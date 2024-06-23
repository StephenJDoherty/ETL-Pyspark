import psycopg2
import pandas as pd
from io import StringIO

# AWS S3
import boto3
from botocore.exceptions import ClientError

# GCP Cloud Storage
from google.cloud import storage

# Azure Blob Storage
from azure.storage.blob import BlobServiceClient

# PostgreSQL Connection Details
POSTGRES_HOST = "your_postgres_host"
POSTGRES_PORT = "your_postgres_port"
POSTGRES_DB = "your_postgres_database"
POSTGRES_USER = "your_postgres_user"
POSTGRES_PASSWORD = "your_postgres_password"

# AWS S3 Credentials
AWS_ACCESS_KEY_ID = "your_aws_access_key_id"
AWS_SECRET_ACCESS_KEY = "your_aws_secret_access_key"
AWS_BUCKET_NAME = "your_aws_bucket_name"

# GCP Cloud Storage Credentials
GCP_PROJECT_ID = "your_gcp_project_id"
GCP_BUCKET_NAME = "your_gcp_bucket_name"

# Azure Blob Storage Credentials
AZURE_CONNECTION_STRING = "your_azure_connection_string"
AZURE_CONTAINER_NAME = "your_azure_container_name"

def load_to_s3(df, table_name):
    """Loads data to AWS S3."""
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    s3 = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )

    try:
        s3.upload_fileobj(
            csv_buffer,
            AWS_BUCKET_NAME,
            f"{table_name}.csv",
            ExtraArgs={"ACL": "bucket-owner-full-control"},
        )
        print(f"Data loaded to S3 bucket: {AWS_BUCKET_NAME}/{table_name}.csv")
    except ClientError as e:
        print(f"Error loading data to S3: {e}")

def load_to_gcs(df, table_name):
    """Loads data to GCP Cloud Storage."""
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    storage_client = storage.Client(project=GCP_PROJECT_ID)
    bucket = storage_client.bucket(GCP_BUCKET_NAME)
    blob = bucket.blob(f"{table_name}.csv")
    blob.upload_from_string(csv_buffer.getvalue())
    print(f"Data loaded to GCS bucket: {GCP_BUCKET_NAME}/{table_name}.csv")

def load_to_azure(df, table_name):
    """Loads data to Azure Blob Storage."""
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    blob_client = blob_service_client.get_blob_client(container=AZURE_CONTAINER_NAME, blob=f"{table_name}.csv")
    blob_client.upload_blob(csv_buffer.getvalue(), overwrite=True)
    print(f"Data loaded to Azure Blob Storage: {AZURE_CONTAINER_NAME}/{table_name}.csv")

def load_postgres_to_data_lakes(table_name):
    """Loads data from PostgreSQL to data lakes."""
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
        )
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {table_name}")
        df = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])

        # Load to S3
        load_to_s3(df, table_name)

        # Load to GCS
        load_to_gcs(df, table_name)

        # Load to Azure
        load_to_azure(df, table_name)

        cursor.close()
        conn.close()
        print(f"Data loaded from PostgreSQL table '{table_name}' to data lakes.")

    except Exception as e:
        print(f"Error loading data: {e}")

if __name__ == "__main__":
    table_name = "your_postgres_table_name"  # Replace with your table name
    load_postgres_to_data_lakes(table_name)
