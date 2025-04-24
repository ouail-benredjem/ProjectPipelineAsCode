import boto3
import os
from dotenv import load_dotenv

load_dotenv()

def upload_to_minio(file_path, bucket_name, object_name):
    s3 = boto3.client(
        's3',
        endpoint_url=os.getenv("MINIO_URL"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
        region_name='us-east-1'
    )

    # Create bucket if not exists
    buckets = [bucket['Name'] for bucket in s3.list_buckets()['Buckets']]
    if bucket_name not in buckets:
        s3.create_bucket(Bucket=bucket_name)

    # Upload file
    s3.upload_file(file_path, bucket_name, object_name)
    print(f"✅ Fichier {file_path} uploadé dans MinIO -> {bucket_name}/{object_name}")

if __name__ == "__main__":
    upload_to_minio(
        file_path="yellow_tripdata_2023-01.parquet",
        bucket_name="yellow-taxi",
        object_name="2023/yellow_tripdata_2023-01.parquet"
    )
