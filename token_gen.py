import boto3
import json
import uuid

BUCKET_NAME = "stonk-api-storage"
OBJECT_KEY = "tokens.json"

def load_tokens(s3):
    """Load the current list of tokens from S3."""
    try:
        response = s3.get_object(Bucket=BUCKET_NAME, Key=OBJECT_KEY)
        data = json.loads(response['Body'].read())
        return data.get("valid_tokens", [])
    except s3.exceptions.NoSuchKey:
        print("[ℹ️] No existing tokens.json found, creating a new one.")
        return []
    except Exception as e:
        print(f"[❌] Failed to load tokens: {e}")
        return []

def save_tokens(s3, tokens):
    """Save the updated token list back to S3."""
    try:
        token_data = json.dumps({"valid_tokens": tokens}, indent=4)
        s3.put_object(Bucket=BUCKET_NAME, Key=OBJECT_KEY, Body=token_data, ContentType='application/json')
        print("[✅] tokens.json successfully updated in S3.")
    except Exception as e:
        print(f"[❌] Failed to save tokens: {e}")

def generate_token():
    """Generate a new UUID-based token."""
    return uuid.uuid4().hex

def main():
    s3 = boto3.client("s3")
    tokens = load_tokens(s3)
    
    new_token = generate_token()
    if new_token in tokens:
        print("[⚠️] Token collision detected (rare). Try again.")
        return

    tokens.append(new_token)
    save_tokens(s3, tokens)

    print(f"[🔐] New API token generated: {new_token}")

if __name__ == "__main__":
    main()

