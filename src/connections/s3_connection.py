import boto3
import pandas as pd
import logging
from src.logger import logging
from io import StringIO
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

import boto3, os
from dotenv import load_dotenv

load_dotenv()


# # Configure logging
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

aws_access_key = os.getenv("aws_access_key")
aws_secret_key = os.getenv("aws_secret_access")
region_name = os.getenv("aws_default_region")
bucket_name = os.getenv("bucket_name")


s3 = boto3.client("s3",
    aws_access_key_id=os.getenv("aws_access_key"),
    aws_secret_access_key=os.getenv("aws_secret_access"),
    region_name=os.getenv("aws_default_region")
)

print(s3.list_buckets())

print(type(aws_access_key), type(aws_secret_key), type(bucket_name), type(region_name))
print(aws_access_key, aws_secret_key, bucket_name, region_name)

class s3_operations:
    def __init__(self, bucket_name, aws_access_key, aws_secret_key, region_name="eu-north-1"):
        """
        Initialize the s3_operations class with AWS credentials and S3 bucket details.
        """
        self.bucket_name = bucket_name
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=region_name
        )
        logging.info("Data Ingestion from S3 bucket initialized")

    def fetch_file_from_s3(self, file_key):
        """
        Fetches a CSV file from the S3 bucket and returns it as a Pandas DataFrame.
        :param file_key: S3 file path (e.g., 'data/data.csv')
        :return: Pandas DataFrame
        """
        
        print("Access Key:", aws_access_key[:4] + "..." if aws_access_key else None)
        print("Secret Key:", aws_secret_key[:4] + "..." if aws_secret_key else None)
        print("Bucket Name:", bucket_name)
        try:
            logging.info(f"Fetching file '{file_key}' from S3 bucket '{self.bucket_name}'...")
            obj = self.s3_client.get_object(Bucket=self.bucket_name, Key=file_key)
            df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
            logging.info(f"Successfully fetched and loaded '{file_key}' from S3 that has {len(df)} records.")
            return df
        except Exception as e:
            logging.exception(f"❌ Failed to fetch '{file_key}' from S3: {e}")
            return None
        

# Example usage
if __name__ == "__main__":
    # Replace these with your actual AWS credentials and S3 details
    BUCKET_NAME = bucket_name
    AWS_ACCESS_KEY = aws_access_key
    AWS_SECRET_KEY = aws_secret_key
    FILE_KEY = "data.csv"  # Path inside S3 bucket

    data_ingestion = s3_operations(BUCKET_NAME, AWS_ACCESS_KEY, AWS_SECRET_KEY)
    df = data_ingestion.fetch_file_from_s3(FILE_KEY)

    if df is not None:
        print(f"Data fetched with {len(df)} records..")  # Display first few rows of the fetched DataFrame
