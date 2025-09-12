import json
import boto3

s3_client = boto3.client('s3')


def load_transactions(bucket_name, transactions_key):
    """
    Load transactions from S3.
    
    :param bucket_name: S3 bucket name
    :param transactions_key: Path/key to the transactions file in S3
    :return: list of transactions (empty list if file not found or error)
    """
    try:
        obj = s3_client.get_object(Bucket=bucket_name, Key=transactions_key)
        return json.loads(obj['Body'].read().decode('utf-8'))
    except s3_client.exceptions.NoSuchKey:
        return []  # no file yet
    except Exception as e:
        print(f"[ERROR] Could not load transactions: {e}")
        return []


def save_transaction(bucket_name, transactions_key, transaction):
    """
    Append a new transaction and save back to S3.
    
    :param bucket_name: S3 bucket name
    :param transactions_key: Path/key to the transactions file in S3
    :param transaction: dict representing the transaction to save
    :return: True if saved successfully, False otherwise
    """
    transactions = load_transactions(bucket_name, transactions_key)
    transactions.append(transaction)

    try:
        s3_client.put_object(
            Bucket=bucket_name,
            Key=transactions_key,
            Body=json.dumps(transactions, indent=2)
        )
        return True
    except Exception as e:
        print(f"[ERROR] Could not save transaction: {e}")
        return False
