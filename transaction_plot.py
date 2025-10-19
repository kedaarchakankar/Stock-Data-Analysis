# transaction_plot.py
import pandas as pd
import json
import numpy as np
import boto3
import io
import base64
from datetime import timedelta
import matplotlib.pyplot as plt

S3_BUCKET = 'stonks-1'
S3_PREFIX = 'stock_data/'
s3_client = boto3.client('s3')

def generate_transaction_plot(transactions_key: str) -> str:
    # Load transactions file
    obj = s3_client.get_object(Bucket=S3_BUCKET, Key=transactions_key)
    transactions = json.loads(obj['Body'].read().decode('utf-8'))

    holdings = {}
    cash = 0.0
    total_input = 0.0
    transaction_log = []
    plot_values = []

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
        for i in range(11):
            date_parsed = date_dt.strftime('%Y-%m-%dT00:00:00.000Z')
            row = stock_df[stock_df['date'] == date_parsed]
            if not row.empty:
                break
            date_dt += timedelta(days=1)
        else:
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
            transaction_log.append([
                date, date_dt.strftime('%Y-%m-%d'), stock.upper(), action,
                quantity, price, raw_holdings, raw_holdings * price,
                cash, total_input, holdings[stock]['adj_quantity']
            ])
            plot_values.append([date, total_input, raw_holdings * price + cash])

        elif action == 'sell':
            if quantity > raw_qty:
                continue
            total_cost = price * quantity
            cash += total_cost
            sell_adj = quantity * factor
            holdings[stock]['adj_quantity'] -= sell_adj
            raw_holdings = holdings[stock]['adj_quantity'] / factor
            transaction_log.append([
                date, date_dt.strftime('%Y-%m-%d'), stock.upper(), action,
                quantity, price, raw_holdings, raw_holdings * price,
                cash, total_input, holdings[stock]['adj_quantity']
            ])
            plot_values.append([date, total_input, raw_holdings * price + cash])

    df_transactions = pd.DataFrame(transaction_log, columns=[
        "Date", "Buy Date", "Stock", "Action", "Quantity", "Price",
        "Total Stocks", "Total Stock Value", "Total Cash",
        "Total Invested", "Unadjusted"
    ])
    df_transactions["Date"] = pd.to_datetime(df_transactions["Date"], utc=True)

    # Daily portfolio value
    start_date = df_transactions["Date"].min()
    end_date = max(data['df']["date"].max() for data in holdings.values())
    date_range = pd.date_range(start=start_date, end=end_date, freq="D")

    daily_values = []
    cash_running = 0.0
    total_input_running = 0.0
    adj_quantities = {stock: 0.0 for stock in holdings}
    tx_sorted = df_transactions.sort_values("Date").reset_index(drop=True)
    tx_idx = 0

    for current_date in date_range:
        while tx_idx < len(tx_sorted) and tx_sorted.loc[tx_idx, "Date"].date() == current_date.date():
            row = tx_sorted.loc[tx_idx]
            stock = row["Stock"].lower()
            action = row["Action"]
            quantity = row["Quantity"]
            price = row["Price"]
            buy_date = pd.to_datetime(row["Buy Date"]).tz_localize(None)
            stock_df = holdings[stock]['df']
            valid_dates = stock_df[stock_df["date"].dt.tz_localize(None) <= buy_date]
            closest_row = valid_dates.iloc[-1]
            factor = closest_row["cumulativeFactor"]

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

        portfolio_value = 0.0
        for stock, adj_qty in adj_quantities.items():
            stock_df = holdings[stock]['df']
            price_row = stock_df[stock_df["date"] <= current_date].tail(1)
            if not price_row.empty:
                price_today = price_row.iloc[0]["adjustedOpen"]
                factor_today = price_row.iloc[0]["cumulativeFactor"]
                raw_qty_today = adj_qty / factor_today
                portfolio_value += raw_qty_today * price_today
        total_money = portfolio_value + cash_running
        daily_values.append([current_date, total_input_running, total_money])

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

    img_buffer = io.BytesIO()
    plt.savefig(img_buffer, format="png")
    plt.close()
    img_buffer.seek(0)

    # ✅ Encode image as base64 and return
    img_base64 = base64.b64encode(img_buffer.read()).decode("utf-8")
    return img_base64
