# app.py
from flask import Flask, send_file, render_template_string, Response, jsonify, request, redirect, url_for, g

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
from transactions import run_transactions
import statsmodels.api as sm
import base64
import re
from botocore.exceptions import ClientError



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
    token = request.args.get('token', '')

    return render_template_string("""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="utf-8">
        <title>Stonk Analysis Home</title>
        <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    </head>
    <body class="bg-light">
      <div class="container mt-5">
        <h1 class="text-center mb-4">📊 Stonk Analysis Dashboard</h1>

        <div class="row justify-content-center mb-4">
          <div class="col-md-8">
            <form method="get" action="/">
              <div class="input-group">
                <input type="text" name="token" class="form-control" placeholder="Enter API token" value="{{ token }}">
                <button class="btn btn-primary" type="submit">Use Token</button>
              </div>
              <div class="form-text mt-1">Protected endpoints will only appear after entering a valid token.</div>
            </form>
          </div>
        </div>

        {% if token %}
        <!-- Automated Rules & Transactions section -->
        <div class="row justify-content-center">
          <div class="col-md-10">
            <div class="card mb-4 shadow">
              <div class="card-body">
                <h3 class="card-title">Automated Rules & Transactions</h3>
                <p class="card-text">Access the rule creators and your transactions (requires a valid token).</p>
                <div class="d-grid gap-2">
                  <a class="btn btn-success btn-lg" href="{{ url_for('dca_rule', token=token) }}">💵 Add DCA Rule (dca_rule)</a>
                  <a class="btn btn-success btn-lg" href="{{ url_for('fqr_rule', token=token) }}">🔢 Add Fixed Quantity Rule (fqr_rule)</a>
                  <a class="btn btn-warning btn-lg" href="{{ url_for('transactions', token=token) }}">📋 Transactions (transactions)</a>
                </div>
              </div>
            </div>
          </div>
        </div>

        <!-- Data & Tools section -->
        <div class="row justify-content-center">
          <div class="col-md-10">
            <div class="card mb-4 shadow">
              <div class="card-body">
                <h3 class="card-title">Data & Tools</h3>
                <p class="card-text">Quick links to explore stock data and correlations.</p>
                <div class="d-grid gap-2">
                  <a class="btn btn-outline-primary btn-lg" href="{{ url_for('stock_input', token=token) }}">📈 View / Enter Stock (stock_input)</a>
                  <a class="btn btn-outline-secondary btn-lg" href="{{ url_for('correlation_input', token=token) }}">🔗 Check Correlations (correlation_input)</a>
                  <a class="btn btn-outline-info btn-lg" href="{{ url_for('plot', token=token) }}">📉 Simple Plot (plot)</a>
                  <a class="btn btn-outline-dark btn-lg" href="{{ url_for('stock_data_plot', stock_symbol='AAPL', token=token) }}">🔎 Example: AAPL Data (AAPL_data)</a>

                  <!-- ✅ NEW BUTTON ADDED HERE -->
                  <a class="btn btn-outline-success btn-lg" href="{{ url_for('pairs_trading', token=token) }}">📊 Pairs Trading </a>
                </div>
              </div>
            </div>
          </div>
        </div>

        <!-- Extra link -->
        <div class="row">
          <div class="col-md-12 text-center">
            <a href="{{ url_for('list_routes') }}" class="mt-3 d-inline-block">View app routes (routes)</a>
          </div>
        </div>
        {% endif %}

        <div class="text-center mt-4 text-muted">
          <small>Note: protected endpoints verify tokens via <code>request.args.get('token')</code> or <code>X-API-TOKEN</code> header.</small>
        </div>
      </div>
    </body>
    </html>
    """, token=token)

@app.route("/pairs_trading", methods=["GET", "POST"])
@require_api_token
def pairs_trading():
    if request.method == "GET":
        return render_template_string("""
        <h2>Pairs Trading Backtest</h2>
        <form method="post">
            Ticker 1: <input name="ticker1"><br><br>
            Ticker 2: <input name="ticker2"><br><br>
            Start Date (YYYY-MM-DD): <input name="start"><br><br>
            End Date (YYYY-MM-DD): <input name="end"><br><br>
            <button type="submit">Run Backtest</button>
        </form>
        """)

    # ----- Collect user input -----
    ticker1 = request.form["ticker1"].upper()
    ticker2 = request.form["ticker2"].upper()
    start = request.form["start"]
    end = request.form["end"]

    # ----- Load both tickers directly from S3 -----
    S3_BUCKET = "stonks-1"
    S3_PREFIX = "stock_data/"
    s3 = boto3.client("s3")

    df_list = {}
    for ticker in [ticker1, ticker2]:
        key = f"{S3_PREFIX}{ticker.lower()}_data.json"
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        data = json.loads(obj["Body"].read().decode("utf-8"))
        df_temp = pd.DataFrame(data)
        df_temp["date"] = pd.to_datetime(df_temp["date"])
        df_temp = df_temp.set_index("date")
        df_list[ticker] = df_temp["close"]

    # ----- Merge -----
    df = pd.concat([df_list[ticker1], df_list[ticker2]], axis=1)
    df.columns = [ticker1, ticker2]
    df = df.loc[start:end].dropna()

    # ----- Cointegration test -----
    coint_t, p_value, crit = sm.tsa.stattools.coint(df[ticker1], df[ticker2])

    coint_result = (
        f"Cointegration p-value: {p_value:.4f} — GOOD PAIR"
        if p_value < 0.05
        else f"Cointegration p-value: {p_value:.4f} — NOT COINTEGRATED"
    )

    # ----- Hedge ratio & spread -----
    X = sm.add_constant(df[ticker2])
    model = sm.OLS(df[ticker1], X).fit()
    hedge_ratio = model.params[ticker2]

    df["spread"] = df[ticker1] - hedge_ratio * df[ticker2]

    # ----- Rolling z-score -----
    window = 30
    df["spread_mean"] = df["spread"].rolling(window).mean()
    df["spread_std"] = df["spread"].rolling(window).std()
    df["zscore"] = (df["spread"] - df["spread_mean"]) / df["spread_std"]

    # ----- MA confirmation -----
    df["ma_short"] = df["spread"].rolling(10).mean()
    df["ma_long"] = df["spread"].rolling(50).mean()

    # ----- Signals -----
    entry, exit = 2.0, 0.5
    df["long_signal"] = ((df["zscore"] < -entry) & (df["ma_short"] < df["ma_long"])).astype(int)
    df["short_signal"] = ((df["zscore"] > entry) & (df["ma_short"] > df["ma_long"])).astype(int)
    df["exit_signal"] = (abs(df["zscore"]) < exit).astype(int)

    # ----- Position -----
    df["position"] = 0
    df.loc[df["long_signal"] == 1, "position"] = 1
    df.loc[df["short_signal"] == 1, "position"] = -1
    df.loc[df["exit_signal"] == 1, "position"] = 0
    df["position"] = df["position"].replace(to_replace=0, method="ffill")

    # ----- Strategy returns -----
    df["returns"] = df[ticker1].pct_change() - hedge_ratio * df[ticker2].pct_change()
    df["strategy"] = df["position"].shift(1) * df["returns"]
    cum_ret = (1 + df["strategy"].fillna(0)).cumprod().iloc[-1]

    # ========== PLOTS → BASE64 STRINGS (inline) ==========

    # --- Plot 1: Spread & MAs ---
    plt.figure(figsize=(10,5))
    plt.plot(df.index, df["spread"], label="Spread")
    plt.plot(df.index, df["ma_short"], label="10-day MA")
    plt.plot(df.index, df["ma_long"], label="50-day MA")
    plt.legend()
    buf = io.BytesIO()
    plt.savefig(buf, format="png", bbox_inches="tight")
    buf.seek(0)
    img1 = base64.b64encode(buf.read()).decode("utf-8")
    plt.close()

    # --- Plot 2: Z-score ---
    plt.figure(figsize=(10,5))
    plt.plot(df.index, df["zscore"], label="Z-score")
    plt.axhline(2, linestyle="--")
    plt.axhline(-2, linestyle="--")
    buf = io.BytesIO()
    plt.savefig(buf, format="png", bbox_inches="tight")
    buf.seek(0)
    img2 = base64.b64encode(buf.read()).decode("utf-8")
    plt.close()

    # --- Plot 3: Cumulative return ---
    plt.figure(figsize=(10,5))
    plt.plot((1 + df["strategy"].fillna(0)).cumprod(), label="Cumulative Return")
    plt.legend()
    buf = io.BytesIO()
    plt.savefig(buf, format="png", bbox_inches="tight")
    buf.seek(0)
    img3 = base64.b64encode(buf.read()).decode("utf-8")
    plt.close()

    # ----- Render HTML -----
    return render_template_string("""
        <h2>Pairs Trading Results: {{t1}} vs {{t2}}</h2>

        <p><b>{{coint}}</b></p>
        <p><b>Final Cumulative Return: {{ret}}</b></p>

        <h3>Spread & Moving Averages</h3>
        <img src="data:image/png;base64,{{img1}}">

        <h3>Z-score</h3>
        <img src="data:image/png;base64,{{img2}}">

        <h3>Strategy Performance</h3>
        <img src="data:image/png;base64,{{img3}}">

        <br><br>
        <a href="/pairs_trading">Run Another Backtest</a>
    """, 
    t1=ticker1, t2=ticker2,
    coint=coint_result,
    ret=f"{cum_ret:.2f}",
    img1=img1, img2=img2, img3=img3)


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


@app.route('/transactions', methods=['GET', 'POST'])
@require_api_token
def transactions():
    s3_client = boto3.client('s3')
    S3_BUCKET = "stonks-1"
    token = request.form.get('token') or request.args.get('token')
    calculation_output = None
    transaction_plot = None  # <--- now holds base64 image string

    # --- Identify user & directory ---
    user_id = request.token_info.get("username")
    user_prefix = f"user_data/{user_id}/tx/"

    # --- List available files ---
    existing_files = []
    resp = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=user_prefix)
    if "Contents" in resp:
        existing_files = [obj["Key"].split("/")[-1] for obj in resp["Contents"]]

    # --- Selected or new file ---
    selected_file = request.form.get("selected_file") or request.args.get("selected_file")
    new_file = request.form.get("new_file")

    if new_file:  # user creating new file
        selected_file = new_file if new_file.endswith(".json") else f"{new_file}.json"
        s3_client.put_object(Bucket=S3_BUCKET, Key=f"{user_prefix}{selected_file}", Body="[]")

    if not selected_file and existing_files:  # default: first file if exists
        selected_file = existing_files[0]

    transactions_key = f"{user_prefix}{selected_file}" if selected_file else None

    # --- Handle POST actions ---
    if request.method == 'POST' and transactions_key:
        if 'calculate' in request.form:
            calculation_output = run_transactions(transactions_key)

            # generate portfolio plot only when calculating
            from transaction_plot import generate_transaction_plot
            transaction_plot = generate_transaction_plot(transactions_key)
            print(transaction_plot[:100])

        else:
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
                save_transaction(S3_BUCKET, transactions_key, new_tx)
                return redirect(url_for('transactions', token=token, selected_file=selected_file))

    # --- Load transactions ---
    all_transactions = load_transactions(S3_BUCKET, transactions_key) if transactions_key else []

    # --- Render ---
    return render_template_string("""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Log Stock Transaction</title>
        <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    </head>
    <body class="container mt-5">
        <h2 class="mb-4">📋 Log a Stock Transaction</h2>

        <!-- File selection -->
        <form method="post" class="mb-4">
            <input type="hidden" name="token" value="{{ token }}">
            <div class="mb-3">
                <label>Select Transactions File:</label>
                <select name="selected_file" class="form-select" onchange="this.form.submit()">
                    {% for f in existing_files %}
                    <option value="{{ f }}" {% if f == selected_file %}selected{% endif %}>{{ f }}</option>
                    {% endfor %}
                </select>
            </div>
            <div class="mb-3">
                <label>Or Create New File:</label>
                <input type="text" name="new_file" placeholder="filename.json" class="form-control">
            </div>
            <button type="submit" class="btn btn-secondary">Open/Create File</button>
        </form>

        {% if selected_file %}
        <!-- Transaction Form -->
        <form method="post" class="mb-5">
            <input type="hidden" name="token" value="{{ token }}">
            <input type="hidden" name="selected_file" value="{{ selected_file }}">
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

        <!-- Transaction Table -->
        <h3>📦 Existing Transactions ({{ selected_file }})</h3>
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

        <!-- Calculate -->
        <form method="post" class="mb-3">
            <input type="hidden" name="token" value="{{ token }}">
            <input type="hidden" name="selected_file" value="{{ selected_file }}">
            <button type="submit" name="calculate" class="btn btn-success">Calculate</button>
        </form>

        {% if calculation_output %}
        <h3>📊 Calculation Output</h3>
        <textarea class="form-control" rows="15" readonly>{{ calculation_output }}</textarea>
        {% endif %}

        {% if transaction_plot %}
        <h3>📈 Portfolio Plot</h3>
        <img src="data:image/png;base64,{{ transaction_plot }}" alt="Portfolio Plot" class="img-fluid mt-3">
        {% endif %}

        {% else %}
        <p>Please select or create a transactions file first.</p>
        {% endif %}
    </body>
    </html>
    """, 
    transactions=all_transactions[::-1], 
    token=token, 
    calculation_output=calculation_output,
    existing_files=existing_files,
    selected_file=selected_file,
    transaction_plot=transaction_plot)



# -------- helpers (small, local) --------
def _sanitize_name(name: str) -> str:
    name = (name or "").strip()
    if not name:
        return ""
    name = re.sub(r"\s+", "_", name)
    name = re.sub(r"[^a-zA-Z0-9_-]", "", name)
    return name

def _s3_exists(s3_client, bucket: str, key: str) -> bool:
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
            return False
        raise

def _list_rule_files(s3_client, bucket: str, prefix: str):
    # Returns list of filenames (not full keys)
    resp = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    contents = resp.get("Contents", [])
    out = []
    for obj in contents:
        key = obj.get("Key", "")
        if not key or key.endswith("/"):
            continue
        out.append(key.split("/")[-1])
    out.sort()
    return out


# =========================
# DCA RULES
# =========================

@app.route('/dca_rule', methods=['GET', 'POST'])
@require_api_token
def dca_rule():
    S3_BUCKET = 'stonks-1'
    s3_client = boto3.client('s3')
    user_id = request.token_info.get("username")

    dca_prefix = f"user_data/{user_id}/dca/"
    tx_prefix  = f"user_data/{user_id}/tx/"

    message = None
    error = None

    # Which rule is selected (from query param or hidden form field)
    selected_rule = request.args.get("selected_rule") or request.form.get("selected_rule") or ""

    # Load list of rules
    rule_files = _list_rule_files(s3_client, S3_BUCKET, dca_prefix)

    # Load selected rule JSON (if any)
    selected_rule_json = None
    if selected_rule:
        selected_key = dca_prefix + selected_rule
        try:
            obj = s3_client.get_object(Bucket=S3_BUCKET, Key=selected_key)
            selected_rule_json = obj['Body'].read().decode('utf-8')
        except Exception as e:
            error = f"Error loading selected rule: {str(e)}"
            selected_rule = ""
            selected_rule_json = None

    # Handle Apply/Delete on POST
    if request.method == 'POST':
        action = request.form.get("action", "")

        if not selected_rule:
            error = "Please select a rule first."
            return render_template_string(
                UI_TEMPLATE_DCA_MAIN,
                message=message,
                error=error,
                rules=rule_files,
                selected_rule=selected_rule,
                selected_rule_json=selected_rule_json
            )

        selected_key = dca_prefix + selected_rule

        if action == "delete":
            try:
                s3_client.delete_object(Bucket=S3_BUCKET, Key=selected_key)
                message = f"Deleted rule: {selected_rule}"
                # refresh list and clear selection
                rule_files = _list_rule_files(s3_client, S3_BUCKET, dca_prefix)
                selected_rule = ""
                selected_rule_json = None
            except Exception as e:
                error = f"Error deleting rule: {str(e)}"

            return render_template_string(
                UI_TEMPLATE_DCA_MAIN,
                message=message,
                error=error,
                rules=rule_files,
                selected_rule=selected_rule,
                selected_rule_json=selected_rule_json
            )

        if action == "apply":
            try:
                # Load rule params from selected file (your rule is stored as [rule_obj])
                obj = s3_client.get_object(Bucket=S3_BUCKET, Key=selected_key)
                rule_data = json.loads(obj['Body'].read().decode('utf-8'))
                if isinstance(rule_data, list) and len(rule_data) > 0:
                    rule_dca = rule_data[0]
                elif isinstance(rule_data, dict):
                    rule_dca = rule_data
                else:
                    raise ValueError("Rule file is empty or invalid JSON.")

                STOCK = rule_dca.get("stock", "aapl")
                FIXED_DOLLAR_AMOUNT = float(rule_dca.get("fixed_dollar_amount", 100))
                FREQUENCY = rule_dca.get("frequency", "weekly")
                START_DATE = pd.to_datetime(rule_dca.get("start_date", "2020-01-01"), utc=True)
                END_DATE   = pd.to_datetime(rule_dca.get("end_date", "2025-01-01"), utc=True)

                # Load stock data from S3 (same as your existing code)
                S3_PREFIX = 'stock_data/'
                s3_key = f"{S3_PREFIX}{STOCK}_data.json"
                obj = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
                stock_data = json.loads(obj['Body'].read().decode('utf-8'))
                df = pd.DataFrame(stock_data)
                df['date'] = pd.to_datetime(df['date'], utc=True)
                df = df.sort_values(by='date').reset_index(drop=True)

                # Build transactions (same logic as before)
                transactions = []
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

                # Save tx file (UUID name; you can later add optional naming here too if you want)
                file_id = str(uuid.uuid4())
                file_id_tx = file_id + "_tx.json"
                tx_key = tx_prefix + file_id_tx

                s3_client.put_object(
                    Bucket=S3_BUCKET,
                    Key=tx_key,
                    Body=json.dumps(transactions, indent=2),
                    ContentType="application/json"
                )

                token = request.form.get("token") or request.args.get("token")
                return redirect(url_for("transactions", token=token, selected_file=file_id_tx))

            except Exception as e:
                error = f"Error applying rule: {str(e)}"

            return render_template_string(
                UI_TEMPLATE_DCA_MAIN,
                message=message,
                error=error,
                rules=rule_files,
                selected_rule=selected_rule,
                selected_rule_json=selected_rule_json
            )

    # GET → show main page
    return render_template_string(
        UI_TEMPLATE_DCA_MAIN,
        message=message,
        error=error,
        rules=rule_files,
        selected_rule=selected_rule,
        selected_rule_json=selected_rule_json
    )


@app.route('/dca_rule/new', methods=['GET', 'POST'])
@require_api_token
def dca_rule_new():
    if request.method == 'POST':
        try:
            STOCK = request.form.get("stock", "aapl")
            FIXED_DOLLAR_AMOUNT = float(request.form.get("dollar_amount", 100))
            FREQUENCY = request.form.get("frequency", "weekly")
            START_DATE = pd.to_datetime(request.form.get("start_date", "2020-01-01"), utc=True)
            END_DATE   = pd.to_datetime(request.form.get("end_date", "2025-01-01"), utc=True)

            # Save only the rule (no tx generation here)
            S3_BUCKET = 'stonks-1'
            s3_client = boto3.client('s3')
            user_id = request.token_info.get("username")

            desired_name_raw = request.form.get("file_name", "").strip()
            desired_name = _sanitize_name(desired_name_raw)

            if desired_name_raw and not desired_name:
                message = "Error: Invalid file name. Use only letters, numbers, underscores, or dashes."
                return render_template_string(UI_TEMPLATE_DCA_NEW, message=message)

            if desired_name:
                file_id = desired_name
            else:
                file_id = str(uuid.uuid4())

            file_id_dca = file_id + "_dca.json"
            dca_key = f"user_data/{user_id}/dca/{file_id_dca}"

            # collision check only if user typed a name
            if desired_name and _s3_exists(s3_client, S3_BUCKET, dca_key):
                message = "Error: A rule with that name already exists. Please choose a different name."
                return render_template_string(UI_TEMPLATE_DCA_NEW, message=message)

            rule_dca = {
                "stock": STOCK,
                "fixed_dollar_amount": FIXED_DOLLAR_AMOUNT,
                "frequency": FREQUENCY,
                "start_date": str(START_DATE),
                "end_date": str(END_DATE)
            }

            s3_client.put_object(
                Bucket=S3_BUCKET,
                Key=dca_key,
                Body=json.dumps([rule_dca], indent=2),
                ContentType="application/json"
            )

            token = request.form.get("token") or request.args.get("token")
            return redirect(url_for("dca_rule", token=token, selected_rule=file_id_dca))

        except Exception as e:
            message = f"Error: {str(e)}"
            return render_template_string(UI_TEMPLATE_DCA_NEW, message=message)

    return render_template_string(UI_TEMPLATE_DCA_NEW, message=None)


UI_TEMPLATE_DCA_MAIN = """
<!DOCTYPE html>
<html>
<head>
  <title>DCA Rules</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 2em; }
    .wrap { display: flex; gap: 40px; align-items: flex-start; }
    .left { width: 280px; }
    .right { flex: 1; }
    .rules { padding: 10px; border: 1px solid #ccc; }
    .rules a { display: block; padding: 4px 0; text-decoration: none; color: #000; }
    .rules a.selected { font-weight: bold; text-decoration: underline; }
    textarea { width: 100%; height: 220px; font-family: monospace; }
    .btnrow { display: flex; gap: 10px; margin-top: 16px; }
    .btn { padding: 12px 16px; border: none; cursor: pointer; font-weight: bold; }
    .btn-green { background: #93c47d; }
    .btn-red { background: #ff0000; color: white; }
    .btn:hover { opacity: 0.9; }
    .msg { margin-top: 18px; font-weight: bold; }
    .err { margin-top: 18px; font-weight: bold; color: red; }
    .add { margin-top: 14px; display: inline-block; padding: 10px 14px; background: #93c47d; font-weight: bold; text-decoration: none; color: black; }
  </style>
</head>
<body>
  <h2>Existing DCA Rules</h2>

  <div class="wrap">
    <div class="left">
      <div><b>Filter:</b></div>
      <div class="rules">
        {% for r in rules %}
          <a href="{{ url_for('dca_rule', token=request.args.get('token'), selected_rule=r) }}"
             class="{% if r == selected_rule %}selected{% endif %}">
             {{ r }}
          </a>
        {% endfor %}
        {% if rules|length == 0 %}
          <div style="color:#666;">No rules found.</div>
        {% endif %}
      </div>

      <a class="add" href="{{ url_for('dca_rule_new', token=request.args.get('token')) }}">Add New DCA Rule</a>
    </div>

    <div class="right">
      <h3>Rule params:</h3>

      {% if selected_rule_json %}
        <textarea readonly>{{ selected_rule_json }}</textarea>
      {% else %}
        <textarea readonly>[]</textarea>
      {% endif %}

      <form method="POST">
        <input type="hidden" name="selected_rule" value="{{ selected_rule }}">
        <input type="hidden" name="token" value="{{ request.args.get('token','') }}">

        <div class="btnrow">
          <button class="btn btn-green" type="submit" name="action" value="apply">Apply Rule</button>
          <button class="btn btn-red" type="submit" name="action" value="delete">Delete Rule</button>
        </div>
      </form>

      {% if error %}
        <div class="err">{{ error }}</div>
      {% endif %}
      {% if message %}
        <div class="msg">{{ message }}</div>
      {% endif %}
    </div>
  </div>
</body>
</html>
"""


UI_TEMPLATE_DCA_NEW = """
<!DOCTYPE html>
<html>
<head>
  <title>Add DCA Rule</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 2em; }
    form { display: flex; flex-direction: column; width: 320px; }
    label { margin-top: 10px; }
    button { margin-top: 20px; padding: 10px; background: #93c47d; font-weight: bold; border: none; cursor: pointer; }
    button:hover { opacity: 0.9; }
    .msg { margin-top: 20px; font-weight: bold; }
    a { display: inline-block; margin-top: 18px; }
  </style>
</head>
<body>
  <h2>Add DCA Investment Rule</h2>

  <form method="POST">
    <input type="hidden" name="token" value="{{ request.args.get('token','') }}">

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

    <label>Optional File Name:
      <input type="text" name="file_name" placeholder="Leave blank for auto-name">
    </label>

    <button type="submit">Add Rule</button>
  </form>

  {% if message %}
    <div class="msg">{{ message }}</div>
  {% endif %}

  <a href="{{ url_for('dca_rule', token=request.args.get('token')) }}">Back to DCA rules</a>
</body>
</html>
"""


# =========================
# FQR RULES
# =========================

@app.route('/fqr_rule', methods=['GET', 'POST'])
@require_api_token
def fqr_rule():
    S3_BUCKET = 'stonks-1'
    s3_client = boto3.client('s3')
    user_id = request.token_info.get("username")

    fqr_prefix = f"user_data/{user_id}/fqr/"
    tx_prefix  = f"user_data/{user_id}/tx/"

    message = None
    error = None

    selected_rule = request.args.get("selected_rule") or request.form.get("selected_rule") or ""

    rule_files = _list_rule_files(s3_client, S3_BUCKET, fqr_prefix)

    selected_rule_json = None
    if selected_rule:
        selected_key = fqr_prefix + selected_rule
        try:
            obj = s3_client.get_object(Bucket=S3_BUCKET, Key=selected_key)
            selected_rule_json = obj['Body'].read().decode('utf-8')
        except Exception as e:
            error = f"Error loading selected rule: {str(e)}"
            selected_rule = ""
            selected_rule_json = None

    if request.method == 'POST':
        action = request.form.get("action", "")

        if not selected_rule:
            error = "Please select a rule first."
            return render_template_string(
                UI_TEMPLATE_FQR_MAIN,
                message=message,
                error=error,
                rules=rule_files,
                selected_rule=selected_rule,
                selected_rule_json=selected_rule_json
            )

        selected_key = fqr_prefix + selected_rule

        if action == "delete":
            try:
                s3_client.delete_object(Bucket=S3_BUCKET, Key=selected_key)
                message = f"Deleted rule: {selected_rule}"
                rule_files = _list_rule_files(s3_client, S3_BUCKET, fqr_prefix)
                selected_rule = ""
                selected_rule_json = None
            except Exception as e:
                error = f"Error deleting rule: {str(e)}"

            return render_template_string(
                UI_TEMPLATE_FQR_MAIN,
                message=message,
                error=error,
                rules=rule_files,
                selected_rule=selected_rule,
                selected_rule_json=selected_rule_json
            )

        if action == "apply":
            try:
                obj = s3_client.get_object(Bucket=S3_BUCKET, Key=selected_key)
                rule_data = json.loads(obj['Body'].read().decode('utf-8'))
                if isinstance(rule_data, list) and len(rule_data) > 0:
                    rule_fqr = rule_data[0]
                elif isinstance(rule_data, dict):
                    rule_fqr = rule_data
                else:
                    raise ValueError("Rule file is empty or invalid JSON.")

                STOCK = rule_fqr.get("stock", "aapl")
                FIXED_QUANTITY = float(rule_fqr.get("fixed_quantity", 1))
                FREQUENCY = rule_fqr.get("frequency", "weekly")
                START_DATE = pd.to_datetime(rule_fqr.get("start_date", "2020-01-01"), utc=True)
                END_DATE   = pd.to_datetime(rule_fqr.get("end_date", "2025-01-01"), utc=True)

                # Load stock data from S3
                S3_PREFIX = 'stock_data/'
                s3_key = f"{S3_PREFIX}{STOCK}_data.json"
                obj = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
                stock_data = json.loads(obj['Body'].read().decode('utf-8'))
                df = pd.DataFrame(stock_data)
                df['date'] = pd.to_datetime(df['date'], utc=True)
                df = df.sort_values(by='date').reset_index(drop=True)

                transactions = []
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

                            transactions.append({
                                "stock": STOCK,
                                "date": date_lookup.strftime("%Y-%m-%d"),
                                "action": "buy",
                                "quantity": FIXED_QUANTITY,
                                "price": close_price,
                                "total_cost": round(FIXED_QUANTITY * close_price, 2)
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

                        transactions.append({
                            "stock": STOCK,
                            "date": date_lookup.strftime("%Y-%m-%d"),
                            "action": "buy",
                            "quantity": FIXED_QUANTITY,
                            "price": close_price,
                            "total_cost": round(FIXED_QUANTITY * close_price, 2)
                        })

                        current_date += timedelta(weeks=1)

                file_id = str(uuid.uuid4())
                file_id_tx = file_id + "_tx.json"
                tx_key = tx_prefix + file_id_tx

                s3_client.put_object(
                    Bucket=S3_BUCKET,
                    Key=tx_key,
                    Body=json.dumps(transactions, indent=2),
                    ContentType="application/json"
                )

                token = request.form.get("token") or request.args.get("token")
                return redirect(url_for("transactions", token=token, selected_file=file_id_tx))

            except Exception as e:
                error = f"Error applying rule: {str(e)}"

            return render_template_string(
                UI_TEMPLATE_FQR_MAIN,
                message=message,
                error=error,
                rules=rule_files,
                selected_rule=selected_rule,
                selected_rule_json=selected_rule_json
            )

    return render_template_string(
        UI_TEMPLATE_FQR_MAIN,
        message=message,
        error=error,
        rules=rule_files,
        selected_rule=selected_rule,
        selected_rule_json=selected_rule_json
    )


@app.route('/fqr_rule/new', methods=['GET', 'POST'])
@require_api_token
def fqr_rule_new():
    if request.method == 'POST':
        try:
            STOCK = request.form.get("stock", "aapl")
            FIXED_QUANTITY = float(request.form.get("quantity", 1))
            FREQUENCY = request.form.get("frequency", "weekly")
            START_DATE = pd.to_datetime(request.form.get("start_date", "2020-01-01"), utc=True)
            END_DATE   = pd.to_datetime(request.form.get("end_date", "2025-01-01"), utc=True)

            S3_BUCKET = 'stonks-1'
            s3_client = boto3.client('s3')
            user_id = request.token_info.get("username")

            desired_name_raw = request.form.get("file_name", "").strip()
            desired_name = _sanitize_name(desired_name_raw)

            if desired_name_raw and not desired_name:
                message = "Error: Invalid file name. Use only letters, numbers, underscores, or dashes."
                return render_template_string(UI_TEMPLATE_FQR_NEW, message=message)

            if desired_name:
                file_id = desired_name
            else:
                file_id = str(uuid.uuid4())

            file_id_fqr = file_id + "_fqr.json"
            fqr_key = f"user_data/{user_id}/fqr/{file_id_fqr}"

            if desired_name and _s3_exists(s3_client, S3_BUCKET, fqr_key):
                message = "Error: A rule with that name already exists. Please choose a different name."
                return render_template_string(UI_TEMPLATE_FQR_NEW, message=message)

            rule_fqr = {
                "stock": STOCK,
                "fixed_quantity": FIXED_QUANTITY,
                "frequency": FREQUENCY,
                "start_date": str(START_DATE),
                "end_date": str(END_DATE)
            }

            s3_client.put_object(
                Bucket=S3_BUCKET,
                Key=fqr_key,
                Body=json.dumps([rule_fqr], indent=2),
                ContentType="application/json"
            )

            token = request.form.get("token") or request.args.get("token")
            return redirect(url_for("fqr_rule", token=token, selected_rule=file_id_fqr))

        except Exception as e:
            message = f"Error: {str(e)}"
            return render_template_string(UI_TEMPLATE_FQR_NEW, message=message)

    return render_template_string(UI_TEMPLATE_FQR_NEW, message=None)


UI_TEMPLATE_FQR_MAIN = """
<!DOCTYPE html>
<html>
<head>
  <title>FQR Rules</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 2em; }
    .wrap { display: flex; gap: 40px; align-items: flex-start; }
    .left { width: 280px; }
    .right { flex: 1; }
    .rules { padding: 10px; border: 1px solid #ccc; }
    .rules a { display: block; padding: 4px 0; text-decoration: none; color: #000; }
    .rules a.selected { font-weight: bold; text-decoration: underline; }
    textarea { width: 100%; height: 220px; font-family: monospace; }
    .btnrow { display: flex; gap: 10px; margin-top: 16px; }
    .btn { padding: 12px 16px; border: none; cursor: pointer; font-weight: bold; }
    .btn-green { background: #93c47d; }
    .btn-red { background: #ff0000; color: white; }
    .btn:hover { opacity: 0.9; }
    .msg { margin-top: 18px; font-weight: bold; }
    .err { margin-top: 18px; font-weight: bold; color: red; }
    .add { margin-top: 14px; display: inline-block; padding: 10px 14px; background: #93c47d; font-weight: bold; text-decoration: none; color: black; }
  </style>
</head>
<body>
  <h2>Existing FQR Rules</h2>

  <div class="wrap">
    <div class="left">
      <div><b>Filter:</b></div>
      <div class="rules">
        {% for r in rules %}
          <a href="{{ url_for('fqr_rule', token=request.args.get('token'), selected_rule=r) }}"
             class="{% if r == selected_rule %}selected{% endif %}">
             {{ r }}
          </a>
        {% endfor %}
        {% if rules|length == 0 %}
          <div style="color:#666;">No rules found.</div>
        {% endif %}
      </div>

      <a class="add" href="{{ url_for('fqr_rule_new', token=request.args.get('token')) }}">Add New FQR Rule</a>
    </div>

    <div class="right">
      <h3>Rule params:</h3>

      {% if selected_rule_json %}
        <textarea readonly>{{ selected_rule_json }}</textarea>
      {% else %}
        <textarea readonly>[]</textarea>
      {% endif %}

      <form method="POST">
        <input type="hidden" name="selected_rule" value="{{ selected_rule }}">
        <input type="hidden" name="token" value="{{ request.args.get('token','') }}">

        <div class="btnrow">
          <button class="btn btn-green" type="submit" name="action" value="apply">Apply Rule</button>
          <button class="btn btn-red" type="submit" name="action" value="delete">Delete Rule</button>
        </div>
      </form>

      {% if error %}
        <div class="err">{{ error }}</div>
      {% endif %}
      {% if message %}
        <div class="msg">{{ message }}</div>
      {% endif %}
    </div>
  </div>
</body>
</html>
"""


UI_TEMPLATE_FQR_NEW = """
<!DOCTYPE html>
<html>
<head>
  <title>Add FQR Rule</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 2em; }
    form { display: flex; flex-direction: column; width: 320px; }
    label { margin-top: 10px; }
    button { margin-top: 20px; padding: 10px; background: #93c47d; font-weight: bold; border: none; cursor: pointer; }
    button:hover { opacity: 0.9; }
    .msg { margin-top: 20px; font-weight: bold; }
    a { display: inline-block; margin-top: 18px; }
  </style>
</head>
<body>
  <h2>Add Fixed Quantity Rule</h2>

  <form method="POST">
    <input type="hidden" name="token" value="{{ request.args.get('token','') }}">

    <label>Stock Symbol:
      <input type="text" name="stock" value="aapl" required>
    </label>

    <label>Quantity:
      <input type="number" name="quantity" value="1" required>
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

    <label>Optional File Name:
      <input type="text" name="file_name" placeholder="Leave blank for auto-name">
    </label>

    <button type="submit">Add Rule</button>
  </form>

  {% if message %}
    <div class="msg">{{ message }}</div>
  {% endif %}

  <a href="{{ url_for('fqr_rule', token=request.args.get('token')) }}">Back to FQR rules</a>
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
