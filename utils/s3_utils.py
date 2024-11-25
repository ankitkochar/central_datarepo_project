# Library
import boto3
import os
import io
from botocore.client import Config
from dotenv import load_dotenv
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

# Initialization
load_dotenv()

aws_access_key = os.environ.get("AWS_ACCESS_KEY")
aws_secret_key = os.environ.get("AWS_SECRET_KEY")
aws_region = os.environ.get("AWS_REGION")


# Helper Functions
def save_pdf_to_s3(doc_url, inst_id, content):

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        config=Config(signature_version="s3v4"),
        region_name=aws_region,
    )

    doc_name = f"{doc_url.split('/')[-1].lower()}"
    bucket = "cld-data-extraction"
    s3_path = "central_repo_data"

    try:
        # uploads the file to s3
        s3_client.upload_fileobj(
            io.BytesIO(content), bucket, f"{s3_path}/{inst_id}/documents/{doc_name}"
        )
        return f"https://cld-data-extraction.s3.amazonaws.com/{s3_path}/{inst_id}/documents/{doc_name}"
    except Exception as e:
        print(f"File not uploaded", e)


def upload_html_to_s3(inst_id, content, file):

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        config=Config(signature_version="s3v4"),
        region_name=aws_region,
    )

    bucket = "cld-data-extraction"
    s3_path = "central_repo_data"
    file_name = f"{s3_path}/{inst_id}/htmls/{file}"

    try:
        s3_client.put_object(
            Bucket=bucket, Key=file_name, Body=content, ContentType="text/html"
        )
        return f"https://cld-data-extraction.s3.amazonaws.com/{file_name}"
    except Exception as e:
        print(f"File not uploaded", e)


def check_folder_exists(bucket_name, folder_path):
    try:
        s3 = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            config=Config(signature_version="s3v4"),
            region_name=aws_region,
        )

        result = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_path)
        return "Contents" in result
    except NoCredentialsError:
        print("Credentials not available")
        return False
    except PartialCredentialsError:
        print("Incomplete credentials provided")
        return False
    except Exception as e:
        print(f"An error occurred: {e}")
        return False
