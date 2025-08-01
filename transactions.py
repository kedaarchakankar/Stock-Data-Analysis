import pandas as pd
import json
import numpy as np
import boto3

S3_BUCKET = 'stonks-1'
S3_PREFIX = 'stock_data/'
s3_client = boto3.client('s3')

with open('transactions.json', 'r') as f:
    transactions = json.load(f)

holdings = {}
cash = 0.0
total_input = 0.0
print("Date, Stock, Action, Quantity, Total Stocks, Total Stock Value, Total Cash, Total Invested, Unadjusted")

for tx in transactions:
    stock = tx['stock']
    date = tx['date']
    action = tx['action'].lower()
    quantity = tx['quantity']

    # Load stock data if not already loaded
    if stock not in holdings:
        s3_key = f"{S3_PREFIX}{stock}_data.json"
        obj = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
        stock_data = json.loads(obj['Body'].read().decode('utf-8'))
        df = pd.DataFrame(stock_data)
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values(by='date', ascending=False)

        # Sort by ascending date to keep chronological order
        df = df.sort_values(by='date').reset_index(drop=True)

        cum_price_factors = np.zeros(len(df))
        cum_factor = 1.0
        # Iterate backwards for price adjustment factor (from newest to oldest)
        for i in reversed(range(len(df))):
            cum_factor *= df.loc[i, 'splitFactor']
            cum_price_factors[i] = cum_factor

        df['cumulativeFactor'] = cum_price_factors
        df['adjustedOpen'] = df['open'] #* df['cumulativeFactor']

        holdings[stock] = {
            'adj_quantity': 0.0,
            'df': df
        }

    stock_df = holdings[stock]['df']
    date_parsed = pd.to_datetime(date).strftime('%Y-%m-%dT00:00:00.000Z')
    row = stock_df[stock_df['date'] == date_parsed]

    if row.empty:
        print(f"[ERROR] No data for {stock} on {date}")
        continue

    row_data = row.iloc[0]
    #print(row_data)
    price = row_data['adjustedOpen']
    factor = row_data['cumulativeFactor']
    adj_qty = holdings[stock]['adj_quantity']
    raw_qty = adj_qty / factor  # Convert adjusted quantity to raw units

    if action == 'buy':
        total_cost = price * quantity
        new_adj = quantity * factor

        if total_cost > cash:
            total_input += total_cost - cash
            cash = 0
        else:
            cash -= total_cost


        holdings[stock]['adj_quantity'] += new_adj
        raw_holdings = holdings[stock]['adj_quantity'] / factor
        
        print(f"{date}, {stock.upper()}, {action}, {quantity}, {raw_holdings:.2f}, {raw_holdings:.2f}, {cash:.2f}, {total_input:.2f}, {holdings[stock]['adj_quantity']:.2f}")
        #print(f"[{stock.upper()}] Bought {quantity} shares on {date} at ${price:.2f}. Holdings: {holdings[stock]['adj_quantity']:.2f} (adjusted). Cash: ${cash:.2f}. Invested: ${total_input:.2f}")
    elif action == 'sell':
        if quantity > raw_qty:
            print(f"[ERROR] Cannot sell {quantity} shares of {stock.upper()} on {date}; only {raw_qty:.2f} available.")
            continue

        total_cost = price * quantity
        cash += total_cost
        sell_adj = quantity * factor

        holdings[stock]['adj_quantity'] -= sell_adj
        raw_holdings = holdings[stock]['adj_quantity'] / factor

        print(f"{date}, {stock.upper()}, {action}, {quantity}, {raw_holdings:.2f}, {raw_holdings:.2f}, {cash:.2f}, {total_input:.2f}, {holdings[stock]['adj_quantity']:.2f}")
        #print(f"[{stock.upper()}] Sold {quantity} shares on {date} at ${price:.2f}. Holdings: {holdings[stock]['adj_quantity']:.2f} (adjusted). Cash: ${cash:.2f}. Invested: ${total_input:.2f}")
    else:
        print(f"[ERROR] Invalid action '{action}' for {stock.upper()} on {date}.")

    raw_holdings = holdings[stock]['adj_quantity'] / factor
    #print(f"[SUMMARY] Total {stock.upper()} holdings on {date}: {raw_holdings:.2f} (raw, unadjusted)")

# Summary
print("\nFinal Holdings Summary:")
total_value = 0.0

for stock, data in holdings.items():
    adj_qty = data['adj_quantity']
    df = data['df']

    last_price = df.iloc[-1]['adjustedOpen']
    holding_value = adj_qty * last_price
    total_value += holding_value

    print(f"{stock.upper()}: {adj_qty:.2f} shares x ${last_price:.2f} = ${holding_value:.2f}")

print(f"\nTotal Portfolio Value: ${total_value:.2f}")
