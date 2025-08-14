# app.py
from flask import Flask, render_template
import pandas as pd

app = Flask(__name__)

# Load CSV
CSV_FILE = 'combined_expenses.csv'
df = pd.read_csv(CSV_FILE, parse_dates=['Date'])

# Precompute metrics
df['amount'] = df['amountCents'] / 100.0  # convert cents to dollars

# Total spending
total_spent = df['amount'].sum()

# Average daily spending
avg_daily = df.groupby('Date')['amount'].sum().mean()

# Spending by description
spending_by_desc = df.groupby('Description')['amount'].sum().sort_values(ascending=False)

# Spending by day
spending_by_day = df.groupby('Date')['amount'].sum()

@app.route('/')
def dashboard():
    return render_template('dashboard.html',
                           total_spent=total_spent,
                           avg_daily=avg_daily,
                           spending_by_desc=spending_by_desc.to_dict(),
                           spending_by_day=spending_by_day.to_dict())

if __name__ == '__main__':
    app.run(debug=True)

