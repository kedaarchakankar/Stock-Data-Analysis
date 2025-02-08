import argparse
import os
import json
from datetime import datetime
from dateutil.relativedelta import relativedelta

def summarize_data(directory):
    results = []

    # Ensure the directory exists
    if not os.path.exists(directory):
        print(f"Error: Directory '{directory}' does not exist.")
        return

    for filename in os.listdir(directory):
        if not filename.endswith("_data.json"):
            continue  # Skip non-JSON files

        ticker = filename.replace("_data.json", "").lower()
        file_path = os.path.join(directory, filename)

        with open(file_path, 'r') as f:
            data = json.load(f)

            if not isinstance(data, list) or len(data) == 0:
                print(f"Warning: {file_path} is empty or invalid. Skipping {ticker}.")
                continue

            first_close = data[0].get('close')
            first_date = data[0].get('date')
            last_close = data[-1].get('close')
            last_date = data[-1].get('date')

            split_divider = 1.0
            for row in data:
                if row['splitFactor'] != 1.0:
                    split_divider *= row['splitFactor']

            first_date_obj = datetime.strptime(first_date, '%Y-%m-%dT%H:%M:%S.%fZ')
            last_date_obj = datetime.strptime(last_date, '%Y-%m-%dT%H:%M:%S.%fZ')

            first_close_adjusted = first_close / split_divider if split_divider != 0 else first_close
            mult_growth = last_close / first_close_adjusted if first_close_adjusted != 0 else 0

            difference = relativedelta(last_date_obj, first_date_obj)
            exp_growth = mult_growth ** (1 / difference.years) if difference.years != 0 else 1

            results.append({
                "ticker": ticker,
                "first_close": first_close,
                "first_date": first_date,
                "first_close_split_adjusted": first_close_adjusted,
                "last_close": last_close,
                "last_date": last_date,
                "growth": mult_growth,
                "exponential_growth": exp_growth
            })

    # Save results as JSON
    summary_file_path = os.path.join(directory, "summary.json")
    with open(summary_file_path, "w") as json_file:
        json.dump(results, json_file, indent=4)

    print(f"Summary saved to {summary_file_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Summarize stock data with split-adjusted growth calculations.")
    parser.add_argument("-dir", type=str, required=True, help="Directory containing stock JSON files")

    args = parser.parse_args()
    summarize_data(args.dir)
