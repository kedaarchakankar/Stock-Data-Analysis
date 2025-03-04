import argparse
import requests
import zipfile
import io
import pandas as pd
import json
import os
import boto3

def download_stock_data(api_token, bucket_name, num_stocks):
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

    # Filter relevant stocks
    df = df[df['priceCurrency'] == 'USD']
    df = df[df['exchange'].isin(['NASDAQ', 'NYSE'])]
    df = df[df['startDate'].notna()]
    df = df[df['endDate'].notna()]
    df = df[df['endDate'].str[:4] == '2025']
    df = df.reset_index(drop=True)

    # Initialize S3 client
    s3 = boto3.client("s3")

    for i in range(min(num_stocks, len(df)) if num_stocks != -1 else len(df)):
        ticker = df['ticker'][i].lower()
        headers = {'Content-Type': 'application/json'}

        try:
            requestResponse = requests.get(
                f"https://api.tiingo.com/tiingo/daily/{ticker}/prices?startDate=1970-01-02&token={api_token}", 
                headers=headers,
                timeout=10  
            )

            if requestResponse.status_code != 200:
                print(f"Warning: API request failed for {ticker}. Status Code: {requestResponse.status_code}")
                continue  

            json_data = requestResponse.json()
            json_filename = f"{ticker}_data.json"

            # Convert data to JSON and upload to S3
            json_bytes = json.dumps(json_data, indent=4).encode("utf-8")
            s3.put_object(Bucket=bucket_name, Key=json_filename, Body=json_bytes, ContentType="application/json")

            print(f"✅ Successfully uploaded {json_filename} to S3 bucket {bucket_name}")

        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for {ticker}: {e}")
            continue  
        except Exception as e:
            print(f"Error uploading {ticker} data to S3: {e}")
            continue  

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download stock data from Tiingo and upload to S3.")
    parser.add_argument("-api_token", type=str, required=True, help="API token for authentication")
    parser.add_argument("-bucket", type=str, required=True, help="S3 bucket name to save data")
    parser.add_argument("-num_stocks", type=int, default=-1, help="Number of stocks to download (-1 for all)")

    args = parser.parse_args()
    download_stock_data(args.api_token, args.bucket, args.num_stocks)
