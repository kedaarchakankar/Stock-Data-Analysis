# app.py
from flask import Flask, send_file, render_template_string, Response, jsonify, request, redirect, url_for

import matplotlib
matplotlib.use('Agg')  # Must come before pyplot import

import matplotlib.pyplot as plt
import os
import json
import matplotlib.pyplot as plt
from datetime import datetime, timezone
from flask import Response
import io
import boto3
import pandas as pd
from stock_correlations import find_top_correlations
import uuid
import threading
import time
from functools import wraps
from token_gen import add_token, delete_token
from transaction_logger import load_transactions, save_transaction
from datetime import timedelta



app = Flask(__name__)
S3_BUCKET = 'stonks-1'
S3_PREFIX = 'stock_data/'
job_statuses = {}
job_results = {}

def load_valid_tokens():
    try:
        s3 = boto3.client('s3')
        response = s3.get_object(Bucket='stonk-api-storage', Key='tokens.json')
        data = json.loads(response['Body'].read())
        return data.get("tokens", [])  # list of dicts now
    except Exception as e:
        print(f"Error loading API tokens from S3: {e}")
        return []

def require_admin_token(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        token_value = request.headers.get('X-API-TOKEN') or request.args.get('token')
        if not token_value:
            return jsonify({"error": "Missing API token"}), 401

        tokens = load_valid_tokens()
        now = datetime.now(timezone.utc)

        for token in tokens:
            if token.get("token") != token_value:
                continue

            try:
                valid_from = datetime.fromisoformat(token["valid_from"].replace("Z", "+00:00"))
                expires_at = datetime.fromisoformat(token["expires_at"].replace("Z", "+00:00"))
            except Exception:
                return jsonify({"error": "Invalid token time format"}), 500

            if not (valid_from <= now <= expires_at):
                return jsonify({"error": "Token expired or not yet valid"}), 403

            if token.get("type") != "admin":
                return jsonify({"error": "Admin privileges required"}), 403

            request.token_info = token
            return func(*args, **kwargs)

        return jsonify({"error": "Invalid API token"}), 403

    return wrapper

@app.route('/admin', methods=['GET'])
@require_admin_token
def admin_dashboard():
    s3 = boto3.client('s3')
    try:
        response = s3.get_object(Bucket='stonk-api-storage', Key='tokens.json')
        tokens_data = json.loads(response['Body'].read())
        tokens = tokens_data.get("tokens", [])
    except Exception as e:
        return f"<h3>Error loading tokens: {e}</h3>", 500

    token_rows = ""
    for t in tokens:
        token_rows += f"""
        <tr>
            <td>{t['token']}</td>
            <td>{t['username']}</td>
            <td>{t['type']}</td>
            <td>{t['created_at']}</td>
            <td>{t['valid_from']}</td>
            <td>{t['expires_at']}</td>
            <td>
                <form action="/admin/delete_token" method="POST">
                    <input type="hidden" name="token" value="{t['token']}">
                    <input type="hidden" name="admin_token" value="{request.token_info['token']}">
                    <button class="btn btn-sm btn-danger">Delete</button>
                </form>
            </td>
        </tr>
        """

    return render_template_string(f"""
    <html>
    <head>
        <title>Admin Dashboard</title>
        <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    </head>
    <body class="container mt-5">
        <h2>🔐 Admin Token Management</h2>

        <form action="/admin/add_token" method="POST" class="row g-3 my-4">
            <input type="hidden" name="admin_token" value="{request.token_info['token']}">
            <div class="col-md-2">
                <input type="text" name="username" class="form-control" placeholder="Username" required>
            </div>
            <div class="col-md-2">
                <select name="type" class="form-select">
                    <option value="user">user</option>
                    <option value="admin">admin</option>
                </select>
            </div>
            <div class="col-md-2">
                <input type="datetime-local" name="valid_from" class="form-control" required>
            </div>
            <div class="col-md-2">
                <input type="datetime-local" name="expires_at" class="form-control" required>
            </div>
            <div class="col-md-2">
                <button class="btn btn-success">➕ Add Token</button>
            </div>
        </form>

        <table class="table table-striped">
            <thead>
                <tr>
                    <th>Token</th>
                    <th>Username</th>
                    <th>Type</th>
                    <th>Created</th>
                    <th>Valid From</th>
                    <th>Expires At</th>
                    <th>Action</th>
                </tr>
            </thead>
            <tbody>
                {token_rows}
            </tbody>
        </table>
    </body>
    </html>
    """)

@app.route('/admin/add_token', methods=['POST'])
def admin_add_token():
    admin_token = request.form.get("admin_token")
    tokens = load_valid_tokens()
    admin = next((t for t in tokens if t['token'] == admin_token and t['type'] == 'admin'), None)
    if not admin:
        return "Unauthorized", 403

    username = request.form.get("username")
    token_type = request.form.get("type")
    valid_from = request.form.get("valid_from")
    expires_at = request.form.get("expires_at")

    new_token = add_token(username, token_type, valid_from, expires_at)
    return redirect(f"/admin?token={admin_token}")

@app.route('/admin/delete_token', methods=['POST'])
def admin_delete_token():
    token_to_delete = request.form.get("token")
    admin_token = request.form.get("admin_token")

    tokens = load_valid_tokens()
    admin = next((t for t in tokens if t['token'] == admin_token and t['type'] == 'admin'), None)
    if not admin:
        return "Unauthorized", 403

    delete_token(token_to_delete)
    return redirect(f"/admin?token={admin_token}")

# Parses datetime string into datetime object with UTC timezone. 
# This is necessary, since the user input from the browser is set to local timezone, 
# this function helps normalize the time zones between the token json data and browser timezones.
# Example testcase: 
# dt_str = "2023-10-27T10:00:00Z" # ISO 8601 with 'Z' for UTC
# expected_dt = datetime(2023, 10, 27, 10, 0, 0, tzinfo=timezone.utc)
# self.assertEqual(parse_iso_utc(dt_str), expected_dt)
def parse_iso_utc(dt_str):
    if dt_str.endswith('Z'):
        dt_str = dt_str[:-1] + '+00:00'
    dt = datetime.fromisoformat(dt_str)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def require_api_token(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        token_value = request.headers.get('X-API-TOKEN') or request.args.get('token')
        if not token_value:
            return jsonify({"error": "Missing API token"}), 401

        tokens = load_valid_tokens()
        now = datetime.now(timezone.utc)

        # Find matching token with validity checks
        for token in tokens:
            if token.get("token") != token_value:
                continue

            try:
                valid_from = parse_iso_utc(token["valid_from"]) 
                expires_at = parse_iso_utc(token["expires_at"])
            except Exception:
                return jsonify({"error": "Invalid timestamp format in token data"}), 500

            if not (valid_from <= now <= expires_at):
                return jsonify({"error": "Token expired or not yet valid"}), 403

            # Optional: attach user info to request context (Flask `g`)
            request.token_info = token
            return func(*args, **kwargs)

        return jsonify({"error": "Invalid API token"}), 403

    return wrapper

@app.route('/stock_input')
def stock_input():
    return render_template_string('''
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>Enter Stock Symbol</title>
        <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    </head>
    <body class="bg-light">
        <div class="container mt-5">
            <h2 class="text-center mb-4">📈 View Stock Data</h2>
            <form action="/redirect_to_stock" method="get" class="text-center">
                <div class="mb-3">
                    <input type="text" name="symbol" class="form-control form-control-lg" placeholder="Enter Stock Symbol (e.g., AAPL)" required>
                </div>
                <button type="submit" class="btn btn-primary btn-lg">Submit</button>
            </form>
        </div>
    </body>
    </html>
    ''')

@app.route('/redirect_to_stock')
def redirect_to_stock():
    symbol = request.args.get("symbol", "").strip().upper()
    if not symbol:
        return "<h3>❌ Invalid stock symbol</h3>", 400
    return redirect(f"/{symbol}_data")

@app.route('/correlation_input')
def correlation_input():
    return render_template_string('''
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>Check Stock Correlations</title>
        <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    </head>
    <body class="bg-light">
        <div class="container mt-5">
            <h2 class="text-center mb-4">🔗 Check Stock Correlations</h2>
            <form action="/correlation/submit" method="get" class="text-center">

                <div class="mb-3">
                    <input type="text" name="stock" class="form-control form-control-lg"
                        placeholder="Enter Stock Symbol (e.g., AAPL)" required>
                </div>

                <div class="mb-3">
                    <input type="number" name="top" class="form-control form-control-lg"
                        placeholder="Number of top correlated stocks (default: 5)">
                </div>

                <div class="mb-3">
                    <input type="date" name="min_date" class="form-control form-control-lg"
                        placeholder="Start Date (e.g., 2023-01-01)">
                </div>

                <div class="mb-3">
                    <input type="date" name="max_date" class="form-control form-control-lg"
                        placeholder="End Date (e.g., 2024-01-01)">
                </div>

                <button type="submit" class="btn btn-success btn-lg">Submit</button>
            </form>
        </div>
    </body>
    </html>
    ''')


@app.route('/')
def home():
    return render_template_string('''
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>Stonk Analysis Home</title>
        <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    </head>
    <body class="bg-light">
        <div class="container mt-5">
            <h1 class="text-center mb-4">📊 Stonk Analysis Dashboard</h1>

            <div class="d-grid gap-3 col-6 mx-auto">
                <a href="/stock_input" class="btn btn-primary btn-lg">📈 View Stock Data</a>
                <a href="/correlation_input" class="btn btn-success btn-lg">🔗 Check Stock Correlations</a>
            </div>

            <div class="text-center mt-5">
                <p class="text-muted">More features coming soon...</p>
            </div>
        </div>
    </body>
    </html>
    ''')

@app.route('/plot')
@require_api_token
def plot():
    # Generate plot
    x = [1, 2, 3, 4, 5]
    y = [i**2 for i in x]
    plt.figure()
    plt.plot(x, y, marker='o')
    plt.title('Simple Plot')
    plt.xlabel('X-axis')
    plt.ylabel('Y-axis')

    # Save to static folder
    path = '/home/ec2-user/plot_test.png'
    #path = '/Users/kedaarchakankar/Downloads/plot_test.png'
    plt.savefig(path)
    plt.close()

    return send_file(path, mimetype='image/png')

@app.route('/<stock_symbol>_data')
@require_api_token
def stock_data_plot(stock_symbol):
    # Define S3 parameters
    bucket_name = 'stonks-1'
    s3_key = f'stock_data/{stock_symbol.lower()}_data.json'  # Convert to lowercase

    try:
        # Connect to S3 and fetch JSON
        s3 = boto3.client('s3')
        response = s3.get_object(Bucket=bucket_name, Key=s3_key)
        json_data = json.loads(response['Body'].read().decode('utf-8'))

    except Exception as e:
        return f"Error reading JSON file from S3: {str(e)}", 500

    # Sort and adjust data
    sorted_data = sorted(json_data, key=lambda x: x['date'], reverse=True)

    cumulative_split = 1.0
    adjusted_dates = []
    adjusted_open_prices = []

    for item in sorted_data:
        date = datetime.strptime(item['date'], '%Y-%m-%dT%H:%M:%S.%fZ')
        split_factor = item.get('splitFactor', 1.0)
        adjusted_open = item['open'] * cumulative_split

        adjusted_dates.append(date)
        adjusted_open_prices.append(adjusted_open)

        cumulative_split /= split_factor

    adjusted_dates.reverse()
    adjusted_open_prices.reverse()

    # Plotting
    plt.figure(figsize=(10, 6))
    plt.plot(adjusted_dates, adjusted_open_prices, marker='o',
             label=f'{stock_symbol.upper()} Split-Adjusted Open Price', color='blue')
    plt.title(f'{stock_symbol.upper()} Stock (Split-Adjusted Open Price)', fontsize=16)
    plt.xlabel('Date', fontsize=14)
    plt.ylabel('Price (Adjusted)', fontsize=14)
    plt.xticks(rotation=45)
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.legend(fontsize=12)
    plt.tight_layout()

    # Save to buffer
    buf = io.BytesIO()
    plt.savefig(buf, format='png')
    buf.seek(0)
    plt.close()

    return Response(buf.getvalue(), mimetype='image/png')

@app.route("/correlations/<stock_symbol>", methods=["GET"])
@require_api_token
def stock_correlations(stock_symbol):
    try:
        num_stocks = int(request.args.get("top", 5))
        min_date = request.args.get("min_date", "2023-01-01")
        max_date = request.args.get("max_date", "2024-01-01")
        bucket_name = S3_BUCKET      # use your actual bucket variable
        data_prefix = S3_PREFIX.rstrip('/')  # remove trailing slash for consistency

        top_correlations = find_top_correlations(
            stock_to_compare=stock_symbol,
            num_stocks=num_stocks,
            min_date=min_date,
            max_date=max_date,
            bucket_name=bucket_name,
            data_prefix=data_prefix
        )

        return jsonify({"stock": stock_symbol.upper(), "correlations": top_correlations})

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Request Testing
def run_correlation_job(job_id, stock_symbol, num_stocks, min_date, max_date):
    try:
        bucket_name = S3_BUCKET
        data_prefix = S3_PREFIX.rstrip('/')

        top_correlations = find_top_correlations(
            stock_to_compare=stock_symbol,
            num_stocks=num_stocks,
            min_date=min_date,
            max_date=max_date,
            bucket_name=bucket_name,
            data_prefix=data_prefix
        )

        # Save results for retrieval after job finishes
        job_statuses[job_id] = "success"
        job_results[job_id] = {
            "stock": stock_symbol.upper(),
            "correlations": top_correlations
        }
        print(f"[✓] Job {job_id} completed for {stock_symbol}")
    except Exception as e:
        job_statuses[job_id] = "failed"
        job_results[job_id] = {"error": str(e)}

@app.route('/correlation/submit', methods=['GET', 'POST'])
@require_api_token
def submit_correlation_job():
    stock_symbol = request.args.get("stock")
    if not stock_symbol:
        return "<h3>❌ Missing required parameter: stock</h3>", 400

    num_stocks = int(request.args.get("top", 5))
    min_date = request.args.get("min_date", "2023-01-01")
    max_date = request.args.get("max_date", "2024-01-01")

    job_id = str(uuid.uuid4())
    job_statuses[job_id] = "processing"

    threading.Thread(target=run_correlation_job, args=(
        job_id, stock_symbol, num_stocks, min_date, max_date
    )).start()

    return render_template_string("""
        <html>
        <head>
            <title>Processing...</title>
            <script>
                async function pollStatus() {
                    const jobId = "{{ job_id }}";
                    const statusUrl = `/correlation/status/${jobId}`;

                    const interval = setInterval(async () => {
                        const res = await fetch(statusUrl, {
                            headers: { 'Accept': 'application/json' }
                        });
                        const data = await res.json();

                        if (data.status === "success" || data.status === "failed") {
                            clearInterval(interval);
                            window.location.href = statusUrl;
                        }
                    }, 2000);
                }
                window.onload = pollStatus;
            </script>
        </head>
        <body>
            <h2>🌀 Processing correlation for {{ stock }}</h2>
            <p>Please wait. Redirecting when complete...</p>
        </body>
        </html>
    """, job_id=job_id, stock=stock_symbol.upper())

@app.route('/correlation/status/<job_id>', methods=['GET'])
def get_job_status(job_id):
    status = job_statuses.get(job_id)

    if request.headers.get('Accept') == 'application/json':
        if status is None:
            return jsonify({"error": "Job ID not found"}), 404
        return jsonify({"job_id": job_id, "status": status}), 200
    else:
        result = job_results.get(job_id)
        if result is None:
            return "<h2>❌ No result found for this job.</h2>", 404

        if status == "failed":
            return f"<h2>❌ Job failed: {result.get('error')}</h2>", 500

        rows = "".join([
            f"<tr><td>{r[0]}</td><td>{r[1]:.4f}</td></tr>"
            for r in result['correlations']
        ])

        return f"""
            <h1>✅ Top Correlations for {result['stock']}</h1>
            <table border="1" cellpadding="5">
                <tr><th>Symbol</th><th>Correlation</th></tr>
                {rows}
            </table>
        """


@app.route('/routes')
def list_routes():
    return jsonify([str(rule) for rule in app.url_map.iter_rules()])

@app.route('/execute_transactions', methods=['GET', 'POST'])
def execute_transactions():
    if request.method == 'POST':
        stock = request.form.get('stock', '').lower().strip()
        date = request.form.get('date', '').strip()
        action = request.form.get('action', '').lower().strip()
        quantity = int(request.form.get('quantity', '0'))

        if stock and date and action in ['buy', 'sell'] and quantity > 0:
            new_tx = {
                "stock": stock,
                "date": date,
                "action": action,
                "quantity": quantity
            }
            save_transaction(new_tx)
            return redirect(url_for('transactions'))

    all_transactions = load_transactions()
    return render_template_string("""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Log Stock Transaction</title>
        <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    </head>
    <body class="container mt-5">
        <h2 class="mb-4">📋 Log a Stock Transaction</h2>
        <form method="post" class="mb-5">
            <div class="mb-3">
                <label>Stock Symbol:</label>
                <input type="text" name="stock" class="form-control" required>
            </div>
            <div class="mb-3">
                <label>Date (YYYY-MM-DD):</label>
                <input type="date" name="date" class="form-control" required>
            </div>
            <div class="mb-3">
                <label>Action:</label>
                <select name="action" class="form-select" required>
                    <option value="buy">Buy</option>
                    <option value="sell">Sell</option>
                </select>
            </div>
            <div class="mb-3">
                <label>Quantity:</label>
                <input type="number" name="quantity" min="1" class="form-control" required>
            </div>
            <button type="submit" class="btn btn-primary">Submit</button>
        </form>

        <h3>📦 Existing Transactions</h3>
        {% if transactions %}
        <table class="table table-bordered table-striped">
            <thead class="table-dark">
                <tr>
                    <th>#</th>
                    <th>Stock</th>
                    <th>Date</th>
                    <th>Action</th>
                    <th>Quantity</th>
                </tr>
            </thead>
            <tbody>
                {% for tx in transactions %}
                <tr>
                    <td>{{ loop.index }}</td>
                    <td>{{ tx.stock.upper() }}</td>
                    <td>{{ tx.date }}</td>
                    <td class="{{ 'text-success' if tx.action == 'buy' else 'text-danger' }}">
                        {{ tx.action.capitalize() }}
                    </td>
                    <td>{{ tx.quantity }}</td>
                </tr>
                {% endfor %}
            </tbody>
        </table>
        {% else %}
        <p>No transactions yet.</p>
        {% endif %}
    </body>
    </html>
    """, transactions=all_transactions)

@app.route('/dca_rule', methods=['GET', 'POST'])
def dca_rule():
    if request.method == 'POST':
        try:
            STOCK = request.form.get("stock", "aapl")
            FIXED_DOLLAR_AMOUNT = float(request.form.get("dollar_amount", 100))
            FREQUENCY = request.form.get("frequency", "weekly")
            START_DATE = pd.to_datetime(request.form.get("start_date", "2020-01-01"), utc=True)
            END_DATE   = pd.to_datetime(request.form.get("end_date", "2025-01-01"), utc=True)

            # Load stock data from S3
            S3_BUCKET = 'stonks-1'
            S3_PREFIX = 'stock_data/'
            s3_client = boto3.client('s3')
            s3_key = f"{S3_PREFIX}{STOCK}_data.json"
            obj = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
            stock_data = json.loads(obj['Body'].read().decode('utf-8'))
            df = pd.DataFrame(stock_data)
            df['date'] = pd.to_datetime(df['date'], utc=True)
            df = df.sort_values(by='date').reset_index(drop=True)

            # Load existing transactions
            with open('transactions.json', "r") as f:
                transactions = json.load(f)

            start_year = START_DATE.year
            end_year = END_DATE.year

            if FREQUENCY == 'monthly':
                for year in range(start_year, end_year + 1):
                    for month in range(1, 13):
                        target_date = pd.Timestamp(year=year, month=month, day=1, tz='UTC')
                        if target_date < df['date'].min() or target_date > df['date'].max():
                            continue
                        month_data = df[(df['date'].dt.year == year) & (df['date'].dt.month == month)]
                        if month_data.empty:
                            continue
                        date_lookup = month_data['date'].min()
                        close_price = df.loc[df['date'] == date_lookup, 'close'].iloc[0]
                        quantity = round(FIXED_DOLLAR_AMOUNT / close_price, 6)

                        transactions.append({
                            "stock": STOCK,
                            "date": date_lookup.strftime("%Y-%m-%d"),
                            "action": "buy",
                            "quantity": quantity
                        })

            elif FREQUENCY == 'weekly':
                first_sunday = START_DATE + pd.offsets.Week(weekday=6)
                current_date = first_sunday
                while current_date <= END_DATE:
                    if current_date > df['date'].max():
                        break
                    week_data = df[df['date'] >= current_date]
                    if week_data.empty:
                        break
                    date_lookup = week_data['date'].min()
                    close_price = df.loc[df['date'] == date_lookup, 'close'].iloc[0]
                    quantity = round(FIXED_DOLLAR_AMOUNT / close_price, 6)

                    transactions.append({
                        "stock": STOCK,
                        "date": date_lookup.strftime("%Y-%m-%d"),
                        "action": "buy",
                        "quantity": quantity
                    })

                    current_date += timedelta(weeks=1)

            # Save back
            with open('transactions.json', "w") as f:
                json.dump(transactions, f, indent=2)

            message = f"Added {FREQUENCY} buys of ${FIXED_DOLLAR_AMOUNT} for {STOCK}"
        except Exception as e:
            message = f"Error: {str(e)}"

        return render_template_string(UI_TEMPLATE, message=message)

    # GET request → show the form
    return render_template_string(UI_TEMPLATE, message=None)


# Inline HTML UI
UI_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
  <title>Add DCA Rule</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 2em; }
    form { display: flex; flex-direction: column; width: 300px; }
    label { margin-top: 10px; }
    button { margin-top: 20px; padding: 10px; background: #4CAF50; color: white; border: none; cursor: pointer; }
    button:hover { background: #45a049; }
    .msg { margin-top: 20px; font-weight: bold; }
  </style>
</head>
<body>
  <h2>Add DCA Investment Rule</h2>
  <form method="POST">
    <label>Stock Symbol:
      <input type="text" name="stock" value="aapl" required>
    </label>

    <label>Dollar Amount:
      <input type="number" name="dollar_amount" value="100" required>
    </label>

    <label>Frequency:
      <select name="frequency">
        <option value="weekly">Weekly</option>
        <option value="monthly">Monthly</option>
      </select>
    </label>

    <label>Start Date:
      <input type="date" name="start_date" value="2020-01-01" required>
    </label>

    <label>End Date:
      <input type="date" name="end_date" value="2025-01-01" required>
    </label>

    <button type="submit">Apply Rule</button>
  </form>

  {% if message %}
  <div class="msg">{{ message }}</div>
  {% endif %}
</body>
</html>
"""

if __name__ == '__main__':
    os.makedirs('static', exist_ok=True)
    app.run(host='0.0.0.0', port=5001, debug=True)

s3 = boto3.client("s3")

def load_stock_data_s3(bucket_name, object_key):
    """Load stock data directly from an S3 JSON object into a DataFrame."""
    try:
        response = s3.get_object(Bucket=bucket_name, Key=object_key)
        content = response["Body"].read()
        data = json.loads(content)

        if isinstance(data, dict):
            data = data.get("data", [])

        if not isinstance(data, list):
            print(f"Skipping {object_key}: Unexpected JSON format.")
            return None

        df = pd.DataFrame(data)

        if "date" not in df.columns:
            print(f"Skipping {object_key}: 'date' column missing.")
            return None

        return df
    except Exception as e:
        print(f"Failed to load {object_key} from S3: {e}")
        return None
### Request testing
