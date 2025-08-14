# transaction_logger.py
import json
import os

TRANSACTIONS_FILE = 'transactions.json'

def load_transactions():
    if os.path.exists(TRANSACTIONS_FILE):
        with open(TRANSACTIONS_FILE, 'r') as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                return []
    return []

def save_transaction(transaction):
    transactions = load_transactions()
    transactions.append(transaction)
    with open(TRANSACTIONS_FILE, 'w') as f:
        json.dump(transactions, f, indent=2)
