import boto3

# from dags.ct_gov.include.config import config
from ct_gov.include.config import config

s3_client = boto3.client(
    's3',
    aws_access_key_id=config.AWS_ACCESS_KEY_ID,
    aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY,
    region_name=config.AWS_REGION,
)

response = s3_client.create_bucket(
    Bucket='clinexa-bkt-tst',
    CreateBucketConfiguration={
        'LocationConstraint': config.AWS_REGION
    }
)

# print(response)