import boto3
import pandas as pd
import os
from botocore.exceptions import NoCredentialsError, ClientError


credentials = pd.read_csv('IAM Access Keys.csv')

AWS_S3_BUCKET_NAME='olist--datalake'
AWS_REGION='ap-south-1'
AWS_ACCESS_KEY = credentials["Access key ID"].iloc[0]
AWS_SECRET_KEY = credentials["Secret access key"].iloc[0]
LOCAL_FILE="dataset/static/olist_customers_dataset.csv"
NAME_FOR_S3="raw_batch/olist_customers_dataset.csv"


try:
    s3_client = boto3.client(
        service_name="s3",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )
except Exception as e:
    print("Failed to create S3 client:", e)
    raise SystemExit


folder = "dataset/static"

if not os.path.exists(folder):
    print(f"Folder '{folder}' does not exist.")
    raise SystemExit

for dataset in os.listdir(folder):

    LOCAL_FILE = f"{folder}/{dataset}"
    NAME_FOR_S3 = f"raw_batch/{dataset}"

    if not os.path.isfile(LOCAL_FILE):
        print(f"⚠ Skipping '{dataset}' (not a file).")
        continue

    print(f"⬆Starting upload of '{dataset[:-4]}'...")

    try:
        s3_client.upload_file(LOCAL_FILE, AWS_S3_BUCKET_NAME, NAME_FOR_S3)
        print(f"Upload completed: {dataset}")

    except FileNotFoundError:
        print(f"Local file not found: {LOCAL_FILE}")

    except NoCredentialsError:
        print("AWS credentials not found or incorrect.")

    except ClientError as e:
        print(f"AWS ClientError while uploading '{dataset}': {e}")

    except Exception as e:
        print(f"Unexpected error while uploading '{dataset}': {e}")

print("\n All uploads attempted.")

