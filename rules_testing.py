import pandas as pd
import json
import numpy as np
import boto3
import io
import sys
from datetime import timedelta
import matplotlib.pyplot as plt

S3_BUCKET = 'stonks-1'
S3_PREFIX = 'stock_data/'
s3_client = boto3.client('s3')
transaction_log = []
STOCK='aapl'
FIXED_DOLLAR_AMOUNT = 100
FREQUENCY = 'weekly'
START_DATE = pd.to_datetime("2020-01-01", utc=True)
END_DATE   = pd.to_datetime("2025-01-01", utc=True)


s3_key = f"{S3_PREFIX}{STOCK}_data.json"
obj = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
stock_data = json.loads(obj['Body'].read().decode('utf-8'))
df = pd.DataFrame(stock_data)
df['date'] = pd.to_datetime(df['date'], utc=True)
df = df.sort_values(by='date').reset_index(drop=True)

cum_price_factors = np.zeros(len(df))
cum_factor = 1.0
for i in reversed(range(len(df))):
    cum_factor *= df.loc[i, 'splitFactor']
    cum_price_factors[i] = cum_factor
df['cumulativeFactor'] = cum_price_factors

# Start from the first available trading date
start_year = START_DATE.year
end_year = END_DATE.year

with open('transactions.json', "r") as f:
    transactions = json.load(f)

if (FREQUENCY == 'monthly'):
    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            target_date = pd.Timestamp(year=year, month=month, day=1, tz='UTC')

            # Skip if before available data or beyond max date
            if target_date < df['date'].min() or target_date > df['date'].max():
                continue

            # Get first trading day in this month
            month_data = df[(df['date'].dt.year == year) & (df['date'].dt.month == month)]
            if month_data.empty:
                continue
            date_lookup = month_data['date'].min()

            # Get close price for that date
            close_price = df.loc[df['date'] == date_lookup, 'close'].iloc[0]

            # Calculate quantity
            quantity = round(FIXED_DOLLAR_AMOUNT / close_price, 6)

            # Append transaction
            transactions.append({
                "stock": STOCK,
                "date": date_lookup.strftime("%Y-%m-%d"),
                "action": "buy",
                "quantity": quantity
            })
elif(FREQUENCY == 'weekly'):
    start_date = START_DATE
    end_date = END_DATE

    first_sunday = start_date + pd.offsets.Week(weekday=6)  # 6 = Sunday
    current_date = first_sunday

    while current_date <= end_date:
        # Skip if beyond max date
        if current_date > df['date'].max():
            break

        # If Sunday is not a trading day, find the next one
        week_data = df[df['date'] >= current_date]
        if week_data.empty:
            break
        date_lookup = week_data['date'].min()

        # Get close price for that date
        close_price = df.loc[df['date'] == date_lookup, 'close'].iloc[0]

        # Calculate quantity
        quantity = round(FIXED_DOLLAR_AMOUNT / close_price, 6)

        # Append transaction
        transactions.append({
            "stock": STOCK,
            "date": date_lookup.strftime("%Y-%m-%d"),
            "action": "buy",
            "quantity": quantity
        })

        # Move forward one week
        current_date += timedelta(weeks=1)

# Save updated transactions.json
with open('transactions.json', "w") as f:
    json.dump(transactions, f, indent=2)

print(f"Added monthly buys of ${FIXED_DOLLAR_AMOUNT} for {STOCK} on the 1st of each month")

print(df)