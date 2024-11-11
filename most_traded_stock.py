import sqlite3
import yfinance as yf

# Database connection
db_path = '/home/ubuntu/stock_analysis.db'  # Absolute path to your SQLite database
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

def create_table():
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS most_traded_stocks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            stock_ticker TEXT,
            total_volume INTEGER,
            buy_percentage REAL,
            sell_percentage REAL
        )
    ''')
    conn.commit()

def clear_table():
    # Delete all existing records from the table before inserting new data
    cursor.execute('DELETE FROM most_traded_stocks')
    conn.commit()

def insert_stock_data(cursor, stock_ticker, total_volume, buy_percentage, sell_percentage):
    try:
        cursor.execute('''
            INSERT INTO most_traded_stocks (stock_ticker, total_volume, buy_percentage, sell_percentage)
            VALUES (?, ?, ?, ?)
        ''', (stock_ticker, total_volume, buy_percentage, sell_percentage))
    except sqlite3.IntegrityError as e:
        print(f"Integrity Error for {stock_ticker}: {e}")
    except Exception as e:
        print(f"Error inserting data for {stock_ticker}: {e}")

def get_sp500_tickers():
    # Placeholder for actual list of S&P 500 tickers
    return ['AAPL', 'MSFT', 'GOOGL', 'AMZN']  # Example tickers

def find_top_traded_stocks(tickers, top_n=10):
    top_traded = []
    for ticker in tickers:
        stock_data = yf.Ticker(ticker).history(period="1d", interval="1m")
        if not stock_data.empty:
            total_volume = stock_data['Volume'].sum()
            if total_volume and total_volume > 0:  # Ensure volume data is valid
                top_traded.append((ticker, {'Volume': stock_data['Volume']}))
    top_traded.sort(key=lambda x: x[1]['Volume'].sum(), reverse=True)
    return top_traded[:top_n]

def calculate_buy_sell_percentage(data):
    total_volume = data['Volume'].sum()
    if total_volume > 0:
        buy_percentage = 60.0  # Placeholder for buy percentage (e.g., 60%)
        sell_percentage = 40.0  # Placeholder for sell percentage (e.g., 40%)
    else:
        buy_percentage, sell_percentage = 0.0, 0.0
    return buy_percentage, sell_percentage

# Ensure table exists
create_table()

# Clear table before inserting new data
clear_table()

# Main logic
tickers = get_sp500_tickers()
print("Tickers retrieved:", tickers)

top_traded_stocks = find_top_traded_stocks(tickers, top_n=10)
print("Top traded stocks:", [ticker for ticker, _ in top_traded_stocks])

for ticker, data in top_traded_stocks:
    total_volume = data['Volume'].sum()
    buy_percentage, sell_percentage = calculate_buy_sell_percentage(data)
    # Print debug information before insertion
    print(f"Inserting data for {ticker}: Total Volume={total_volume}, Buy Percentage={buy_percentage}%, Sell Percentage={sell_percentage}%")
    insert_stock_data(cursor, ticker, total_volume, buy_percentage, sell_percentage)

# Commit all changes and close the database connection
conn.commit()
conn.close()
