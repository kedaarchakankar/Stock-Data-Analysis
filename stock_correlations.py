import os
import json
import argparse
import pandas as pd

def load_stock_data(file_path):
    """Load stock data from a JSON file and return a DataFrame."""
    with open(file_path, "r") as f:
        data = json.load(f)

    if isinstance(data, dict):
        data = data.get("data", [])

    if not isinstance(data, list):
        print(f"Skipping {file_path}: Unexpected JSON format.")
        return None

    df = pd.DataFrame(data)

    if "date" not in df.columns:
        print(f"Skipping {file_path}: 'date' column missing.")
        return None

    return df

def preprocess_stock_data(df, min_date, max_date):
    """Preprocess stock data by adjusting for splits and computing daily percent change."""
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df.dropna(subset=["date"], inplace=True)
    df.set_index("date", inplace=True)

    required_columns = {"adjClose", "splitFactor"}
    if not required_columns.issubset(df.columns):
        print("Skipping: Required columns missing.")
        return None

    df["splitFactor"] = df["splitFactor"].fillna(1)
    df["cumulative_split"] = df["splitFactor"][::-1].cumprod()[::-1]

    for col in ["adjClose", "adjOpen", "adjHigh", "adjLow"]:
        if col in df.columns:
            df[col] = df[col] / df["cumulative_split"]

    df.drop(columns=["cumulative_split"], inplace=True)

    df = df.loc[(df.index >= min_date) & (df.index <= max_date)].copy()
    df["pct_change"] = df["adjClose"].pct_change()
    df.dropna(subset=["pct_change"], inplace=True)

    return df

def find_top_correlations(stock_to_compare, num_stocks, min_date, max_date, data_folder):
    """Find top correlated stocks with the target stock."""
    correlations = {}

    min_date = pd.to_datetime(min_date).tz_localize("UTC")
    max_date = pd.to_datetime(max_date).tz_localize("UTC")

    comparison_file = os.path.join(data_folder, f"{stock_to_compare.lower()}_data.json")
    compared_stock_df = load_stock_data(comparison_file)

    if compared_stock_df is None:
        raise ValueError(f"Error: {stock_to_compare} data could not be loaded.")

    compared_stock_df = preprocess_stock_data(compared_stock_df, min_date, max_date)
    if compared_stock_df is None:
        raise ValueError(f"Error: {stock_to_compare} data missing required columns.")

    for file in os.listdir(data_folder):
        if not file.endswith("_data.json") or file.lower() == f"{stock_to_compare.lower()}_data.json":
            continue

        stock_name = file.split("_")[0].upper()
        stock_file = os.path.join(data_folder, file)

        stock_df = load_stock_data(stock_file)
        if stock_df is None:
            continue

        stock_df = preprocess_stock_data(stock_df, min_date, max_date)
        if stock_df is None or stock_df.empty:
            continue

        if stock_df.index.min() > (min_date + pd.Timedelta(days=5)):
            continue

        combined_df = compared_stock_df.join(stock_df, how="inner", lsuffix="_aapl", rsuffix="_stock")

        if combined_df.empty or "pct_change_stock" not in combined_df.columns:
            continue

        correlation = combined_df["pct_change_aapl"].corr(combined_df["pct_change_stock"])
        if pd.notna(correlation):
            correlations[stock_name] = correlation

    top_correlated_stocks = sorted(correlations.items(), key=lambda x: x[1], reverse=True)[:num_stocks]

    print(f"Top {num_stocks} correlated stocks with {stock_to_compare.upper()}:")
    for stock, corr in top_correlated_stocks:
        print(f"{stock}: {corr:.4f}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Find top correlated stocks.")
    parser.add_argument("--stock_to_compare", type=str, required=True, help="Ticker of stock to compare against others")
    parser.add_argument("--num_stocks", type=int, required=True, help="Number of top correlated stocks to output")
    parser.add_argument("--min_date", type=str, required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--max_date", type=str, required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--data_folder", type=str, required=True, help="Folder containing stock data JSON files")

    args = parser.parse_args()

    find_top_correlations(
        stock_to_compare=args.stock_to_compare,
        num_stocks=args.num_stocks,
        min_date=args.min_date,
        max_date=args.max_date,
        data_folder=args.data_folder
    )

