import pandas as pd
import json

transactions = [
    {"stock": "swim", "date": "2021-04-26", "action": "buy", "quantity": 10},
    {"stock": "swim", "date": "2021-04-27", "action": "sell", "quantity": 5},
    {"stock": "swim", "date": "2021-04-28", "action": "sell", "quantity": 10},
    {"stock": "aapl", "date": "2021-04-26", "action": "buy", "quantity": 20},
    {"stock": "aapl", "date": "2021-04-28", "action": "sell", "quantity": 5}
]

holdings = {}

for tx in transactions:
    stock = tx['stock']
    date = tx['date']
    action = tx['action'].lower()
    quantity = tx['quantity']

    # Load stock data if not already loaded
    if stock not in holdings:
        with open(f"{stock}_data.json", 'r') as f:
            stock_data = json.load(f)
        df = pd.DataFrame(stock_data)
        holdings[stock] = {'quantity': 0, 'df': df}

    stock_df = holdings[stock]['df']

    # Format date to match df
    formatted_date = pd.to_datetime(date).strftime('%Y-%m-%dT00:00:00.000Z')
    row = stock_df[stock_df['date'] == formatted_date]

    if row.empty:
        print(f"[ERROR] No data for {stock} on {formatted_date}")
        continue

    price = row.iloc[0]['open']
    current_holding = holdings[stock]['quantity']

    if action == 'buy':
        holdings[stock]['quantity'] += quantity
        print(f"[{stock.upper()}] Bought {quantity} shares on {date} at ${price:.2f}. Holdings: {holdings[stock]['quantity']}")
    elif action == 'sell':
        if quantity > current_holding:
            print(f"[ERROR] Cannot sell {quantity} shares of {stock.upper()} on {date}; only {current_holding} available.")
            continue
        holdings[stock]['quantity'] -= quantity
        print(f"[{stock.upper()}] Sold {quantity} shares on {date} at ${price:.2f}. Holdings: {holdings[stock]['quantity']}")
    else:
        print(f"[ERROR] Invalid action '{action}' for {stock.upper()} on {date}.")

# Summary
print("\nFinal Holdings Summary:")
total_value = 0.0

for stock, data in holdings.items():
    quantity = data['quantity']
    df = data['df']

    # Get latest price from last row of df
    last_price = df.iloc[-1]['open']
    holding_value = quantity * last_price
    total_value += holding_value

    print(f"{stock.upper()}: {quantity} shares x ${last_price:.2f} = ${holding_value:.2f}")

print(f"\nTotal Portfolio Value: ${total_value:.2f}")
