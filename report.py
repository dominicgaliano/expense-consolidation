# app.py
from flask import Flask, render_template, jsonify
import pandas as pd
import logging

logging.basicConfig(
    level=logging.WARNING, format="%(asctime)s [%(levelname)s] %(message)s"
)

app = Flask(__name__)

# Load CSV
CSV_FILE = "output/combined_expenses.csv"
df = pd.read_csv(CSV_FILE, parse_dates=["Date"])

df["Date"] = pd.to_datetime(df["Date"], format="%m/%d/%Y", errors="coerce")

invalid_rows = df[df["Date"].isna()]
if not invalid_rows.empty:
    logging.warning(
        "The following rows have invalid dates and will be dropped:\n%s", invalid_rows
    )

# keep only valid dates
df = df.dropna(subset=["Date"])

# precompute metrics
total_spent = df["amountCents"].sum() / 100.0

# spend by month (format YYYY-MM)
spending_by_month = (
    df.groupby(df["Date"].dt.to_period("M").astype(str))["amountCents"]
    .sum()
    .div(100.00)
    .to_dict()
)

available_months = sorted(df["Date"].dt.to_period("M").astype(str).unique().tolist())


@app.route("/")
def dashboard():
    return render_template(
        "dashboard.html",
        spending_by_month=spending_by_month,
        total_spent=total_spent,
        available_months=available_months,
    )


@app.route("/api/expenses/<month>")
def get_expenses_for_month(month):
    """Return list of expenses for a given month (format YYYY-MM)."""
    try:
        month_period = pd.Period(month)
    except ValueError:
        return jsonify({"error": "Invalid month format. Expected YYYY-MM"}), 400

    filtered = df[df["Date"].dt.to_period("M") == month_period]
    expenses = filtered.sort_values("Date").to_dict(orient="records")

    # convert cents → dollars
    for e in expenses:
        e["amountDollars"] = e["amountCents"] / 100.0
        e["Date"] = e["Date"].strftime("%Y-%m-%d")

    return jsonify(expenses)

if __name__ == "__main__":
    app.run(debug=True)
