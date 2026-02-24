import urllib.request
import json
import boto3
from datetime import datetime
import os

# Create S3 client (region auto-detected)
s3 = boto3.client('s3')

# Use environment variable for bucket name (BEST PRACTICE)
BUCKET_NAME = os.environ.get("BUCKET_NAME")

def lambda_handler(event, context):
    try:
        url = "https://open.er-api.com/v6/latest/USD"

        # Fetch exchange rate data
        with urllib.request.urlopen(url) as response:
            data = json.loads(response.read().decode())

        # Create timestamp for file name
        timestamp = datetime.utcnow().strftime("%Y-%m-%d-%H-%M-%S")
        file_name = f"exchange-rates-{timestamp}.json"

        # Upload file to S3
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=file_name,
            Body=json.dumps(data),
            ContentType="application/json"
        )

        return {
            "statusCode": 200,
            "body": f"{file_name} uploaded successfully!"
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": str(e)
        }