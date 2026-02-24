import urllib.request
import json
import boto3
import csv
import io
from datetime import datetime
import os

# Create S3 client
s3 = boto3.client('s3')

# Bucket name from environment variable
BUCKET_NAME = os.environ.get("BUCKET_NAME")

def lambda_handler(event, context):
    try:
        # 1️⃣ Fetch JSON data from API
        url = "https://open.er-api.com/v6/latest/USD"
        with urllib.request.urlopen(url) as response:
            data = json.loads(response.read().decode())

        # 2️⃣ Convert JSON to CSV in memory
        output = io.StringIO()
        writer = csv.writer(output)

        # Write CSV header
        writer.writerow(["Currency", "Rate"])

        # Extract rates from JSON
        rates = data.get("rates", {})
        for currency, rate in rates.items():
            writer.writerow([currency, rate])

        csv_content = output.getvalue()
        output.close()

        # 3️⃣ Create timestamped filename
        timestamp = datetime.utcnow().strftime("%Y-%m-%d-%H-%M-%S")
        file_name = f"exchange-rates-{timestamp}.csv"

        # 4️⃣ Upload CSV to S3
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=file_name,
            Body=csv_content,
            ContentType="text/csv"
        )

        return {
            "statusCode": 200,
            "body": f"CSV file {file_name} uploaded successfully!"
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": str(e)
        }