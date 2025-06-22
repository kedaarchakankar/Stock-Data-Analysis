# token_manager.py
import boto3
import json
import uuid
from datetime import datetime, timezone

S3_BUCKET = 'stonk-api-storage'
TOKENS_KEY = 'tokens.json'

def load_tokens():
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=S3_BUCKET, Key=TOKENS_KEY)
    return json.loads(response['Body'].read())

def save_tokens(data):
    s3 = boto3.client('s3')
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=TOKENS_KEY,
        Body=json.dumps(data, indent=2).encode('utf-8')
    )

def add_token(username, token_type, valid_from, expires_at):
    now = datetime.now(timezone.utc).isoformat()
    new_token = str(uuid.uuid4())

    data = load_tokens()
    if "tokens" not in data:
        data["tokens"] = []

    data["tokens"].append({
        "token": new_token,
        "username": username,
        "type": token_type,
        "created_at": now,
        "valid_from": valid_from,
        "expires_at": expires_at
    })

    save_tokens(data)
    return new_token

def delete_token(token_to_remove):
    data = load_tokens()
    tokens = data.get("tokens", [])
    tokens = [t for t in tokens if t["token"] != token_to_remove]
    data["tokens"] = tokens
    save_tokens(data)
