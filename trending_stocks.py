import yfinance as yf
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime, timedelta
import time
import re

# Database connection setup
db_path = '/home/ubuntu/stock_analysis.db'

# Connect to the database and create the table if it doesn't exist
def setup_database():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS trending_stocks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT,
            beta REAL,
            alpha REAL,
            price_change REAL,
            rsi REAL,
            volatility REAL,
            trend_type TEXT,
            analysis_date TEXT
        )
    ''')
    conn.commit()
    conn.close()

# Known unsupported symbols
unsupported_symbols = ["BRK.B", "BF.B"]

# Fetch S&P 500 stock list
def get_sp500_tickers():
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    sp500_table = pd.read_html(url, header=0)[0]
    symbols = sp500_table['Symbol'].tolist()
    symbols = [symbol.replace(".", "-") for symbol in symbols if re.match(r'^[A-Za-z0-9]+$', symbol) and symbol not in unsupported_symbols]
    return symbols

# Insert trend data into the database
def insert_trend_data(data, trend_type):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    analysis_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for row in data.itertuples(index=False):
        cursor.execute('''
            INSERT INTO trending_stocks (ticker, beta, alpha, price_change, rsi, volatility, trend_type, analysis_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (row.Ticker, row.Beta, row.Alpha, row._4, row.RSI, row.Volatility, trend_type, analysis_date))
    conn.commit()
    conn.close()

# Clear existing data for a specific trend type
def clear_old_data(trend_type):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('DELETE FROM trending_stocks WHERE trend_type = ?', (trend_type,))
    conn.commit()
    conn.close()

# Define indicators and calculations for stock trends
def calculate_beta_alpha(stock_returns, market_returns):
    cov_matrix = np.cov(stock_returns, market_returns)
    beta = cov_matrix[0, 1] / cov_matrix[1, 1]
    alpha = np.mean(stock_returns) - beta * np.mean(market_returns)
    return beta, alpha

def calculate_indicators(data, short_window=10, long_window=50):
    data['Short_MA'] = data['Close'].rolling(window=short_window).mean()
    data['Long_MA'] = data['Close'].rolling(window=long_window).mean()
    delta = data['Close'].diff()
    gain = delta.where(delta > 0, 0).rolling(window=14).mean()
    loss = -delta.where(delta < 0, 0).rolling(window=14).mean()
    rs = gain / loss
    data['RSI'] = 100 - (100 / (1 + rs))
    data['Volatility'] = data['Close'].pct_change().rolling(window=short_window).std()
    return data

# Retry function for stock data download
def download_stock_data(ticker, start, end, retries=3, delay=5):
    for i in range(retries):
        try:
            data = yf.download(ticker, start=start, end=end, timeout=20)
            if 'Close' in data.columns:
                return data
            print(f"Missing 'Close' data for {ticker}, retrying... ({i + 1}/{retries})")
        except Exception as e:
            print(f"Error downloading data for {ticker}: {e}, retrying... ({i + 1}/{retries})")
        time.sleep(delay)
    print(f"Failed to retrieve data for {ticker} after {retries} attempts.")
    return None

# Main analysis function for a given period
def analyze_trends(period_days, trend_type):
    end_date = datetime.today()
    start_date = end_date - timedelta(days=period_days)
    failed_tickers = []

    # Download S&P 500 market data
    try:
        market_data = yf.download('^GSPC', start=start_date, end=end_date, timeout=20)
        market_data['Market_Returns'] = market_data['Close'].pct_change()
        if market_data.index.nlevels > 1:
            market_data.index = market_data.index.droplevel(0)
    except Exception as e:
        print(f"Failed to download market data: {e}")
        return None, None

    trending_stocks = []
    for ticker in sp500_tickers:
        stock_data = download_stock_data(ticker, start_date, end_date)
        if stock_data is None or 'Close' not in stock_data.columns:
            failed_tickers.append(ticker)
            continue

        # Flatten index if necessary
        if isinstance(stock_data.columns, pd.MultiIndex):
            stock_data.columns = stock_data.columns.get_level_values(0)

        stock_data['Stock_Returns'] = stock_data['Close'].pct_change()
        stock_data = calculate_indicators(stock_data)

        # Reset index if necessary
        if 'Date' not in stock_data.columns:
            stock_data.reset_index(inplace=True)
        if 'Date' not in market_data.columns:
            market_data.reset_index(inplace=True)

        # Merge with market data
        aligned_data = stock_data.set_index('Date').join(market_data.set_index('Date')['Market_Returns'], how='inner').dropna()
        if aligned_data.empty:
            print(f"No overlapping data for {ticker}")
            failed_tickers.append(ticker)
            continue

        # Calculate beta and alpha
        beta, alpha = calculate_beta_alpha(aligned_data['Stock_Returns'], aligned_data['Market_Returns'])
        price_change = (aligned_data['Close'].iloc[-1] - aligned_data['Close'].iloc[0]) / aligned_data['Close'].iloc[0] * 100

        trending_stocks.append({
            'Ticker': ticker,
            'Beta': beta,
            'Alpha': alpha,
            'Price Change (%)': price_change,
            'RSI': aligned_data['RSI'].iloc[-1],
            'Volatility': aligned_data['Volatility'].iloc[-1]
        })

    # Convert results to DataFrame
    trending_df = pd.DataFrame(trending_stocks)
    if not trending_df.empty:
        trending_df = trending_df.sort_values(by="Price Change (%)", ascending=False).reset_index(drop=True)
        top_uptrend = trending_df.head(10)
        top_downtrend = trending_df.tail(10).sort_values(by="Price Change (%)").reset_index(drop=True)
        clear_old_data(trend_type)  # Clear old data for this trend type
        insert_trend_data(top_uptrend, f"{trend_type}_uptrend")
        insert_trend_data(top_downtrend, f"{trend_type}_downtrend")
    else:
        top_uptrend, top_downtrend = None, None

    if failed_tickers:
        print(f"Failed to process data for the following tickers: {failed_tickers}")

    return top_uptrend, top_downtrend

# Setup the database and create tables if necessary
setup_database()

# Retrieve the S&P 500 tickers
sp500_tickers = get_sp500_tickers()

# Run analyses for both 365 days and 90 days
top_uptrend_365, top_downtrend_365 = analyze_trends(365, '365_days')
top_uptrend_90, top_downtrend_90 = analyze_trends(90, '90_days')

# Display results
if top_uptrend_365 is not None and top_downtrend_365 is not None:
    print("Top 10 Uptrend Stocks (Last 365 Days):")
    print(top_uptrend_365)
    print("\nTop 10 Downtrend Stocks (Last 365 Days):")
    print(top_downtrend_365)

if top_uptrend_90 is not None and top_downtrend_90 is not None:
    print("\nTop 10 Uptrend Stocks (Last 90 Days):")
    print(top_uptrend_90)
    print("\nTop 10 Downtrend Stocks (Last 90 Days):")
    print(top_downtrend_90)
else:
    print("Unable to retrieve trending stocks for the selected periods.")
