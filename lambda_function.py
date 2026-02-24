import urllib.request
import json
import boto3
from datetime import datetime
import os

s3 = boto3.client('s3')
BUCKET_NAME = os.environ.get("BUCKET_NAME")

def lambda_handler(event, context):
    url = "https://open.er-api.com/v6/latest/USD"

    with urllib.request.urlopen(url) as response:
        data = json.loads(response.read().decode())

    timestamp = datetime.utcnow().strftime("%Y-%m-%d-%H-%M-%S")
    file_name = f"exchange-rates-{timestamp}.json"

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=file_name,
        Body=json.dumps(data),
        ContentType='application/json'
    )

    return {
        'statusCode': 200,
        'body': f"Uploaded {file_name} to S3"
    }