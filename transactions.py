# transactions.py
import pandas as pd
import json
import numpy as np
import boto3
import io
import sys
from datetime import timedelta


S3_BUCKET = 'stonks-1'
S3_PREFIX = 'stock_data/'
s3_client = boto3.client('s3')


def run_transactions():
    # Capture print output
    buffer = io.StringIO()
    sys_stdout = sys.stdout
    sys.stdout = buffer

    with open('transactions.json', 'r') as f:
        transactions = json.load(f)

    holdings = {}
    cash = 0.0
    total_input = 0.0
    print("Date, Buy Date, Stock, Action, Quantity, Total Stocks, Total Stock Value, Total Cash, Total Invested, Unadjusted")

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
            df = df.sort_values(by='date').reset_index(drop=True)

            cum_price_factors = np.zeros(len(df))
            cum_factor = 1.0
            for i in reversed(range(len(df))):
                cum_factor *= df.loc[i, 'splitFactor']
                cum_price_factors[i] = cum_factor

            df['cumulativeFactor'] = cum_price_factors
            df['adjustedOpen'] = df['open']

            holdings[stock] = {
                'adj_quantity': 0.0,
                'df': df
            }

        stock_df = holdings[stock]['df']

        # Try up to 10 increments
        date_dt = pd.to_datetime(date)

        for i in range(11):  # includes the initial date (i=0)
            date_parsed = date_dt.strftime('%Y-%m-%dT00:00:00.000Z')
            row = stock_df[stock_df['date'] == date_parsed]

            if not row.empty:
                break  # found valid date, exit loop

            # If not found, increment by 1 day
            date_dt += timedelta(days=1)

        else:
            # Only runs if loop didn't break (no date found in 10 tries)
            print(f"[ERROR] No data for {stock} from {date} within 10 days")
            continue

        row_data = row.iloc[0]
        price = row_data['adjustedOpen']
        factor = row_data['cumulativeFactor']
        adj_qty = holdings[stock]['adj_quantity']
        raw_qty = adj_qty / factor

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

            print(f"{date}, {date_dt.strftime('%Y-%m-%d')}, {stock.upper()}, {action}, {quantity}, {price:.2f}, {raw_holdings:.2f}, {raw_holdings*price:.2f}, {cash:.2f}, {total_input:.2f}, {holdings[stock]['adj_quantity']:.2f}")

        elif action == 'sell':
            if quantity > raw_qty:
                print(f"[ERROR] Cannot sell {quantity} shares of {stock.upper()} on {date}; only {raw_qty:.2f} available.")
                continue

            total_cost = price * quantity
            cash += total_cost
            sell_adj = quantity * factor

            holdings[stock]['adj_quantity'] -= sell_adj
            raw_holdings = holdings[stock]['adj_quantity'] / factor

            print(f"{date}, {date_dt.strftime('%Y-%m-%d')}, {stock.upper()}, {action}, {quantity}, {raw_holdings:.2f}, {raw_holdings:.2f}, {cash:.2f}, {total_input:.2f}, {holdings[stock]['adj_quantity']:.2f}")
        else:
            print(f"[ERROR] Invalid action '{action}' for {stock.upper()} on {date}.")

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
    print(f"\nTotal Cash: ${cash:.2f}")
    print(f"\nTotal Money: ${total_value + cash:.2f}")
    print(f"\nTotal Invested: ${total_input:.2f}")
    print(f"\nTotal Percentage gain: {((total_value + cash) / total_input)*100 - 100:.2f}%")


    # Restore stdout and return captured output
    sys.stdout = sys_stdout
    return buffer.getvalue()
