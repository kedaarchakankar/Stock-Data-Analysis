# transaction_plot.py
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

with open('transactions.json', 'r') as f:
    transactions = json.load(f)

holdings = {}
cash = 0.0
total_input = 0.0
plot_values = []

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
        transaction_log.append([date, date_dt.strftime('%Y-%m-%d'), stock.upper(), action, quantity, price, raw_holdings, raw_holdings * price, cash, total_input, holdings[stock]['adj_quantity']])
        plot_values.append([date, total_input, raw_holdings*price + cash])

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
        transaction_log.append([ date, date_dt.strftime('%Y-%m-%d'), stock.upper(), action, quantity, price, raw_holdings, raw_holdings * price, cash, total_input, holdings[stock]['adj_quantity'] ])
        plot_values.append([date, total_input, raw_holdings*price + cash])
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

#print(f"\nTotal Portfolio Value: ${total_value:.2f}")
#print(f"\nTotal Cash: ${cash:.2f}")
#print(f"\nTotal Money: ${total_value + cash:.2f}")
#print(f"\nTotal Invested: ${total_input:.2f}")
#print(f"\nTotal Percentage gain: {((total_value + cash) / total_input)*100 - 100:.2f}%")

df_transactions = pd.DataFrame(transaction_log, columns=[
    "Date",
    "Buy Date",
    "Stock",
    "Action",
    "Quantity",
    "Price",
    "Total Stocks",
    "Total Stock Value",
    "Total Cash",
    "Total Invested",
    "Unadjusted"
])

print(df_transactions)
df_transactions["Date"] = pd.to_datetime(df_transactions["Date"], utc=True)

# Determine the date range to evaluate daily values
start_date = df_transactions["Date"].min()
start_date = pd.to_datetime(start_date, utc=True)
# Use the latest date from all stock price data
end_date = max(data['df']["date"].max() for data in holdings.values())
end_date = pd.to_datetime(end_date, utc=True)
date_range = pd.date_range(start=start_date, end=end_date, freq="D")

daily_values = []

# Track running cash and holdings
cash_running = 0.0
total_input_running = 0.0
adj_quantities = {stock: 0.0 for stock in holdings}

# Sort transactions chronologically
tx_sorted = df_transactions.sort_values("Date").reset_index(drop=True)
tx_idx = 0

# Iterate over each day
for current_date in date_range:
    # Process any transactions on this date
    while tx_idx < len(tx_sorted) and tx_sorted.loc[tx_idx, "Date"].date() == current_date.date():
        row = tx_sorted.loc[tx_idx]
        stock = row["Stock"]
        action = row["Action"]
        quantity = row["Quantity"]
        price = row["Price"]
        unadjusted = row["Unadjusted"]

        # Find factor for this date from holdings data
        stock_df = holdings[stock]['df']
        factor = stock_df.loc[stock_df["date"] == pd.Timestamp(row["Buy Date"]), "cumulativeFactor"].values[0]

        if action == "buy":
            total_cost = price * quantity
            if total_cost > cash_running:
                total_input_running += total_cost - cash_running
                cash_running = 0
            else:
                cash_running -= total_cost
            adj_quantities[stock] += quantity * factor

        elif action == "sell":
            total_cost = price * quantity
            cash_running += total_cost
            adj_quantities[stock] -= quantity * factor

        tx_idx += 1

    # Compute portfolio value for this day
    portfolio_value = 0.0
    for stock, adj_qty in adj_quantities.items():
        stock_df = holdings[stock]['df']
        # Try to get price for current date; if not found, use last available before it
        price_row = stock_df[stock_df["date"] <= current_date].tail(1)
        if not price_row.empty:
            price_today = price_row.iloc[0]["adjustedOpen"]
            factor_today = price_row.iloc[0]["cumulativeFactor"]
            raw_qty_today = adj_qty / factor_today
            portfolio_value += raw_qty_today * price_today

    total_money = portfolio_value + cash_running
    daily_values.append([current_date, total_input_running, total_money])

# Convert to DataFrame
df_daily = pd.DataFrame(daily_values, columns=["Date", "Total Invested", "Total Money"])

# Plot
plt.figure(figsize=(12, 6))
plt.plot(df_daily["Date"], df_daily["Total Invested"], label="Total Invested", color="blue")
plt.plot(df_daily["Date"], df_daily["Total Money"], label="Total Money", color="green")
plt.xlabel("Date")
plt.ylabel("USD")
plt.title("Portfolio Value + Cash vs Total Invested (Daily)")
plt.legend()
plt.grid(True)

# Save to BytesIO
img_buffer = io.BytesIO()
plt.savefig(img_buffer, format="png")
img_buffer.seek(0)

# Upload to S3
plot_key = f"transactions/portfolio_plot.png"
s3_client.put_object(
    Bucket=S3_BUCKET,
    Key=plot_key,
    Body=img_buffer,
    ContentType="image/png"
)
