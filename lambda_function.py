import urllib.request
import json

def lambda_handler(event, context):
    url = "https://open.er-api.com/v6/latest/USD"

    with urllib.request.urlopen(url) as response:
        data = json.loads(response.read().decode())

    print(data)
    
    return {
        'statusCode': 200,
        'body': json.dumps(data)
    }