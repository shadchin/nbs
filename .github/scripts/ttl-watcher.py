import os
import argparse
import boto3
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse
from botocore.exceptions import NoCredentialsError, ClientError
from concurrent.futures import ThreadPoolExecutor
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def should_delete(key, creation_date, now, default_ttl, test_data_ttl):
    is_test_data = 'test_data' in key.split('/')
    ttl = test_data_ttl if is_test_data else default_ttl
    expiration_date = creation_date + timedelta(days=ttl)

    logging.info(f"Evaluating {key}: creation_date={creation_date}, expiration_date={expiration_date}, now={now}")

    if now >= expiration_date:
        logging.info(f"Object {key} is marked for deletion. TTL: {'TEST_DATA_TTL' if is_test_data else 'DEFAULT_TTL'}")
        return True
    else:
        logging.info(f"Object {key} is not expired. TTL: {'TEST_DATA_TTL' if is_test_data else 'DEFAULT_TTL'}, Expiration Date: {expiration_date}")
        return False

def delete_expired_objects(bucket_name, keys_to_delete, dry_run):
    if dry_run:
        logging.info(f"[Dry Run] Would delete: {keys_to_delete}")
        return

    s3 = boto3.client('s3')
    delete_objects = [{'Key': key} for key in keys_to_delete]
    response = s3.delete_objects(Bucket=bucket_name, Delete={'Objects': delete_objects})
    return response

def process_objects(bucket_name, objects, now, default_ttl, test_data_ttl, dry_run):
    keys_to_delete = [obj['Key'] for obj in objects if should_delete(obj['Key'], obj['LastModified'], now, default_ttl, test_data_ttl)]
    if keys_to_delete:
        delete_response = delete_expired_objects(bucket_name, keys_to_delete, dry_run)
        if not dry_run:
            logging.info(f"Deleted objects: {delete_response}")

def recursively_list_objects(s3_client, bucket_name, prefix):
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        for content in page.get('Contents', []):
            logging.info(f"Found object: {content['Key']} with last modified: {content['LastModified']}")
            yield content
        for subdir in page.get('CommonPrefixes', []):
            logging.info(f"Traversing into subdir: {subdir['Prefix']}")
            yield from recursively_list_objects(s3_client, bucket_name, subdir['Prefix'])

def delete_expired_dirs(bucket_name, base_prefix, default_ttl, test_data_ttl, dry_run):
    s3 = boto3.client('s3')
    now = datetime.now(timezone.utc)

    try:
        with ThreadPoolExecutor() as executor:
            for content in recursively_list_objects(s3, bucket_name, base_prefix):
                executor.submit(process_objects, bucket_name, [content], now, default_ttl, test_data_ttl, dry_run)

    except NoCredentialsError:
        logging.error("Error: No AWS credentials found")
    except ClientError as e:
        logging.error(f"Error processing S3 bucket: {e}")
    except Exception as e:
        logging.error(f"Other exception: {e}")


def parse_s3_path(s3_path):
    parsed_url = urlparse(s3_path)
    if parsed_url.scheme != 's3':
        raise ValueError("URL must be an S3 URL (s3://bucket/prefix)")
    bucket_name = parsed_url.netloc
    base_prefix = parsed_url.path.lstrip('/')
    return bucket_name, base_prefix

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Delete expired directories from an S3 bucket based on TTL.')
    parser.add_argument('s3_path', help='S3 path in the format s3://bucket/prefix')
    parser.add_argument('--default-ttl', type=int, default=int(os.environ.get('DEFAULT_TTL', 30)), help='Default TTL in days')
    parser.add_argument('--test-data-ttl', type=int, default=int(os.environ.get('TEST_DATA_TTL', 7)), help='TTL for test_data directories in days')
    parser.add_argument('--dry-run', action='store_true', help='Perform a dry run without actually deleting objects')

    args = parser.parse_args()

    bucket_name, base_prefix = parse_s3_path(args.s3_path)
    default_ttl = args.default_ttl
    test_data_ttl = args.test_data_ttl
    dry_run = args.dry_run

    logging.info(f"Starting script with parameters: Bucket: {bucket_name}, Prefix: {base_prefix}, Default TTL: {default_ttl}, Test Data TTL: {test_data_ttl}, Dry Run: {dry_run}")
    delete_expired_dirs(bucket_name, base_prefix, default_ttl, test_data_ttl, dry_run)
