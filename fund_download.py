import argparse
import requests
import zipfile
import io
import pandas as pd
import json
import os
import boto3
from botocore.exceptions import NoCredentialsError

def download_fundamental_data(api_token, directory, num_stocks, access_key, secret_key, bucket_name):
    url = "https://apimedia.tiingo.com/docs/tiingo/daily/supported_tickers.zip"

    response = requests.get(url)
    if response.status_code == 200:
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            file_names = z.namelist()
            with z.open(file_names[0]) as csv_file:
                df = pd.read_csv(csv_file)
    else:
        print(f"Failed to download file: {response.status_code}")
        return

    df = df[df['priceCurrency'] == 'USD']
    df = df[df['exchange'].isin(['NASDAQ', 'NYSE'])]
    df = df[df['startDate'].notna()]
    df = df[df['endDate'].notna()]
    df = df[df['endDate'].str[:4] == '2025']
    df = df.reset_index(drop=True)

    os.makedirs(directory, exist_ok=True)

    for i in range(min(num_stocks, len(df)) if num_stocks != -1 else len(df)):
        try:
            try:
                ticker = df['ticker'][i].lower()
            except:
                ticker = df['ticker'][i]
            headers = {'Content-Type': 'application/json'}

            # Request fundamentals data
            requestResponse = requests.get(
                f"https://api.tiingo.com/tiingo/fundamentals/{ticker}/daily?token={api_token}",
                headers=headers,
                timeout=10
            )

            if requestResponse.status_code != 200:
                print(f"Warning: API request failed for {ticker}. Status Code: {requestResponse.status_code}")
                continue

            json_data = requestResponse.json()

            if not json_data:
                print(f"No data returned for {ticker}")
                continue

            if bucket_name:
                # Upload to S3 if bucket specified
                s3 = boto3.client('s3')
                try:
                    s3.put_object(
                        Bucket=bucket_name,
                        Key=f"fundimental_data/{ticker}_data.json",
                        Body=json.dumps(json_data, indent=4),
                        ContentType='application/json'
                    )
                    print(f"Successfully uploaded {ticker}_data.json to S3.")
                except NoCredentialsError:
                    print("Error: AWS credentials not found.")
            else:
                # Save locally if no bucket
                with open(os.path.join(directory, f"{ticker}_data.json"), "w") as f:
                    json.dump(json_data, f, indent=4)
                    print(f"Successfully saved {ticker}_data.json locally.")

        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for {ticker}: {e}")
            continue

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download fundamental stock data from Tiingo.")
    parser.add_argument("-api_token", type=str, required=True, help="API token for authentication")
    parser.add_argument("-dir", type=str, required=True, help="Directory to save data")
    parser.add_argument("-num_stocks", type=int, default=-1, help="Number of stocks to download (-1 for all)")

    # AWS credentials and bucket name (optional)
    parser.add_argument("-access_key", type=str, help="AWS Access Key ID (optional)")
    parser.add_argument("-secret_key", type=str, help="AWS Secret Access Key (optional)")
    parser.add_argument("-bucket", type=str, help="S3 Bucket name (optional)")

    args = parser.parse_args()
    download_fundamental_data(args.api_token, args.dir, args.num_stocks, args.access_key, args.secret_key, args.bucket)
