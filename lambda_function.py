import urllib.request
import json
import boto3
import csv
import io
import pandas as pd
from datetime import datetime
import os

# S3 client
s3 = boto3.client('s3')
BUCKET_NAME = os.environ.get("BUCKET_NAME")

def lambda_handler(event, context):
    try:
        # 1️⃣ Fetch JSON data
        url = "https://open.er-api.com/v6/latest/USD"
        with urllib.request.urlopen(url) as response:
            data = json.loads(response.read().decode())

        rates = data.get("rates", {})

        # 2️⃣ Convert to CSV
        output_csv = io.StringIO()
        writer = csv.writer(output_csv)
        writer.writerow(["Currency", "Rate"])
        for currency, rate in rates.items():
            writer.writerow([currency, rate])
        csv_content = output_csv.getvalue()
        output_csv.close()

        # 3️⃣ Convert to Parquet
        df = pd.DataFrame(list(rates.items()), columns=["Currency", "Rate"])
        output_parquet = io.BytesIO()
        df.to_parquet(output_parquet, engine='pyarrow', index=False)
        parquet_content = output_parquet.getvalue()
        output_parquet.close()

        # 4️⃣ Timestamped filenames
        timestamp = datetime.utcnow().strftime("%Y-%m-%d-%H-%M-%S")
        csv_file_name = f"exchange-rates-{timestamp}.csv"
        parquet_file_name = f"exchange-rates-{timestamp}.parquet"

        # 5️⃣ Upload CSV to S3
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=csv_file_name,
            Body=csv_content,
            ContentType='text/csv'
        )

        # 6️⃣ Upload Parquet to S3
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=parquet_file_name,
            Body=parquet_content,
            ContentType='application/octet-stream'
        )

        return {
            "statusCode": 200,
            "body": f"CSV file {csv_file_name} and Parquet file {parquet_file_name} uploaded successfully!"
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": str(e)
        }