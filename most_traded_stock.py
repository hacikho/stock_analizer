import yfinance as yf
import pandas as pd
import sqlite3
from concurrent.futures import ThreadPoolExecutor

# Database connection and table setup
def initialize_database():
    conn = sqlite3.connect('stock_analysis.db')
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS most_traded_stocks (
            stock_ticker TEXT PRIMARY KEY,
            total_volume INTEGER,
            buy_volume INTEGER,
            sell_volume INTEGER
        )
    ''')
    conn.commit()
    return conn, cursor

def clear_table(cursor):
    """
    Clears the most_traded_stocks table to remove old data.
    """
    cursor.execute('DELETE FROM most_traded_stocks')

def insert_stock_data(cursor, stock_ticker, total_volume, buy_volume, sell_volume):
    """
    Inserts stock data into the most_traded_stocks table.
    """
    cursor.execute('''
        INSERT INTO most_traded_stocks (stock_ticker, total_volume, buy_volume, sell_volume)
        VALUES (?, ?, ?, ?)
    ''', (stock_ticker, total_volume, buy_volume, sell_volume))

def get_sp500_tickers():
    """
    Fetches the S&P 500 stock tickers.
    """
    url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    table = pd.read_html(url)
    sp500_df = table[0]
    return sp500_df['Symbol'].tolist()

def get_stock_data(stock_symbol):
    """
    Fetches 1-minute interval data for the past 24 hours, including pre- and post-market data.
    """
    stock = yf.Ticker(stock_symbol)
    data = stock.history(period="1d", interval="1m", prepost=True)
    return data

def calculate_buy_sell_volume(data):
    """
    Estimates buy and sell volumes based on intraday price movements.
    """
    buy_volume = data[data['Close'] > data['Open']]['Volume'].sum()
    sell_volume = data[data['Close'] < data['Open']]['Volume'].sum()
    total_volume = data['Volume'].sum()

    buy_percentage = (buy_volume / total_volume) * 100 if total_volume else 0
    sell_percentage = (sell_volume / total_volume) * 100 if total_volume else 0

    return buy_volume, sell_volume, buy_percentage, sell_percentage

def find_top_traded_stocks(tickers, top_n=10):
    """
    Identifies the top N most traded stocks by total volume over the last 24 hours, including pre- and post-market.
    """
    total_volumes = {}
    
    def fetch_and_calculate_volume(ticker):
        try:
            data = get_stock_data(ticker)
            return ticker, data['Volume'].sum(), data
        except Exception as e:
            print(f"Error processing {ticker}: {e}")
            return ticker, 0, None

    with ThreadPoolExecutor() as executor:
        results = executor.map(fetch_and_calculate_volume, tickers)

    for ticker, total_volume, data in results:
        if data is not None:
            total_volumes[ticker] = (total_volume, data)
    
    # Sort stocks by total volume and select the top N
    sorted_stocks = sorted(total_volumes.items(), key=lambda x: x[1][0], reverse=True)[:top_n]
    
    # Create a list of top traded stocks with data
    top_traded_stocks = [(ticker, data) for ticker, (_, data) in sorted_stocks]
    
    return top_traded_stocks

# Main Execution
conn, cursor = initialize_database()
clear_table(cursor)  # Clear the table before inserting new data

tickers = get_sp500_tickers()
top_traded_stocks = find_top_traded_stocks(tickers, top_n=10)

# Insert the top 10 most traded stocks into the database
for ticker, data in top_traded_stocks:
    total_volume = data['Volume'].sum()
    buy_volume, sell_volume, buy_percentage, sell_percentage = calculate_buy_sell_volume(data)
    insert_stock_data(cursor, ticker, total_volume, buy_volume, sell_volume)
    print(f"\nStock: {ticker}")
    print(f"Total Volume: {total_volume} shares")
    print(f"Buy Volume: {buy_volume} shares ({buy_percentage:.2f}%)")
    print(f"Sell Volume: {sell_volume} shares ({sell_percentage:.2f}%)")

# Commit changes and close the connection
conn.commit()
conn.close()
