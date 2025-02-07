import argparse
import requests
import zipfile
import io
import pandas as pd
import json
import os

def download_stock_data(api_token, directory, num_stocks):
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

            with open(os.path.join(directory, f"{ticker}_data.json"), "w") as f:
                json.dump(json_data, f, indent=4)

        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for {ticker}: {e}")
            continue  

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download stock data from Tiingo.")
    parser.add_argument("-api_token", type=str, required=True, help="API token for authentication")
    parser.add_argument("-dir", type=str, required=True, help="Directory to save data")
    parser.add_argument("-num_stocks", type=int, default=-1, help="Number of stocks to download (-1 for all)")

    args = parser.parse_args()
    download_stock_data(args.api_token, args.dir, args.num_stocks)
