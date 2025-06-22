# 📊 Stonk Analysis API

A Flask-based API and web dashboard for visualizing stock data, computing correlations, and managing secure API tokens using AWS S3.

---

## 🚀 Features

- Plot split-adjusted stock price charts from S3-stored JSON data
- Compute top N correlated stocks over a date range
- View all routes via `/routes`
- Admin dashboard for managing tokens (`/admin`)
- API token validation for protected routes
- Background job threading for async correlation processing

---

## 📁 Project Structure
├── app.py # Main Flask application

├── token_gen.py # Token generation and management utility

├── stock_correlations.py # Correlation logic (assumed separately implemented)

---

## 🛠️ Setup Instructions

### 1. 📦 Install Requirements

Create a virtual environment (optional but recommended), then install:

pip install flask matplotlib boto3 pandas

### 2. 🔐 AWS Credentials
Make sure your environment has valid AWS credentials to access S3. You can do this using:
export AWS_ACCESS_KEY_ID=your-access-key
export AWS_SECRET_ACCESS_KEY=your-secret-key
export AWS_DEFAULT_REGION=us-east-1

### 3. 📂 Ensure S3 Buckets Are Accessible

Before running the app, verify that the following S3 buckets exist and are accessible with your credentials:

| Bucket Name          | Purpose                        | Example Key                             |
|----------------------|--------------------------------|------------------------------------------|
| `stonks-1`           | Stores stock price JSON files  | `stock_data/AAPL_data.json`              |
| `stonk-api-storage`  | Stores API token metadata      | `tokens.json`                            |

These must be readable (and writable for admin routes) by your IAM user, role, or access keys.

### 4. 🧪 Run the App

Start the Flask app locally via:

python app.py

You can also use nohup to keep it running in the background.
