import json
import subprocess
import time

# Define all 8 test cases
test_cases = {
    "test_case_1_basic_buy_sell": [
        {"stock": "msft", "date": "2020-01-01", "action": "buy", "quantity": 10},
        {"stock": "msft", "date": "2020-01-02", "action": "sell", "quantity": 5}
    ],
    "test_case_2_oversell": [
        {"stock": "tsla", "date": "2022-06-01", "action": "buy", "quantity": 3},
        {"stock": "tsla", "date": "2022-06-02", "action": "sell", "quantity": 5}
    ],
    "test_case_3_invalid_action": [
        {"stock": "goog", "date": "2022-01-01", "action": "purchase", "quantity": 10}
    ],
    "test_case_4_missing_data": [
        {"stock": "aapl", "date": "1900-01-01", "action": "buy", "quantity": 10}
    ],
    "test_case_5_multiple_buys_sell": [
        {"stock": "nflx", "date": "2021-03-01", "action": "buy", "quantity": 10},
        {"stock": "nflx", "date": "2021-03-02", "action": "buy", "quantity": 20},
        {"stock": "nflx", "date": "2021-03-03", "action": "sell", "quantity": 25}
    ],
    "test_case_6_mixed_stocks": [
        {"stock": "aapl", "date": "2021-04-26", "action": "buy", "quantity": 10},
        {"stock": "tsla", "date": "2021-04-26", "action": "buy", "quantity": 5},
        {"stock": "aapl", "date": "2021-04-27", "action": "sell", "quantity": 5},
        {"stock": "tsla", "date": "2021-04-28", "action": "sell", "quantity": 5}
    ],
    "test_case_7_non_trading_day": [
        {"stock": "amzn", "date": "2021-07-02", "action": "buy", "quantity": 5},
        {"stock": "amzn", "date": "2021-07-04", "action": "sell", "quantity": 5}
    ],
    "test_case_8_all_sell_no_buy": [
        {"stock": "nvda", "date": "2023-05-10", "action": "sell", "quantity": 5}
    ]
}

for name, case in test_cases.items():
    print(f"\n============================")
    print(f"Running {name}")
    print(f"============================")

    # Write to transactions.json
    with open("transactions.json", "w") as f:
        json.dump(case, f, indent=4)

    # Run transactions.py
    try:
        subprocess.run(["python3", "transactions.py"], check=True)
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] Script failed: {e}")
    
    time.sleep(1)  # Optional pause between tests

