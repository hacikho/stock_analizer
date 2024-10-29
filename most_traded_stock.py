import sqlite3
import yfinance as yf

# Database connection
db_path = '/home/ubunt/stock_analysis.db'  # Replace with the absolute path to your SQLite database
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

def create_table():
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS most_traded_stocks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            stock_ticker TEXT,
            total_volume INTEGER,
            buy_volume INTEGER,
            sell_volume INTEGER
        )
    ''')
    conn.commit()

def insert_stock_data(cursor, stock_ticker, total_volume, buy_volume, sell_volume):
    try:
        cursor.execute('''
            INSERT INTO most_traded_stocks (stock_ticker, total_volume, buy_volume, sell_volume)
            VALUES (?, ?, ?, ?)
        ''', (stock_ticker, total_volume, buy_volume, sell_volume))
    except sqlite3.IntegrityError as e:
        print(f"Integrity Error for {stock_ticker}: {e}")
    except Exception as e:
        print(f"Error inserting data for {stock_ticker}: {e}")

def get_sp500_tickers():
    # This is a placeholder; implement actual method to get tickers
    return ['AAPL', 'MSFT', 'GOOGL', 'AMZN']  # Example tickers

def find_top_traded_stocks(tickers, top_n=10):
    top_traded = []
    for ticker in tickers:
        stock_data = yf.Ticker(ticker).history(period="1d", interval="1m")
        if not stock_data.empty:
            total_volume = stock_data['Volume'].sum()
            top_traded.append((ticker, {'Volume': stock_data['Volume']}))
    top_traded.sort(key=lambda x: x[1]['Volume'].sum(), reverse=True)
    return top_traded[:top_n]

def calculate_buy_sell_volume(data):
    total_volume = data['Volume'].sum()
    buy_volume = int(total_volume * 0.6)  # Placeholder calculation
    sell_volume = int(total_volume * 0.4)  # Placeholder calculation
    return buy_volume, sell_volume, 60, 40

# Ensure table exists
create_table()

# Main logic
tickers = get_sp500_tickers()
print("Tickers retrieved:", tickers)

top_traded_stocks = find_top_traded_stocks(tickers, top_n=10)
print("Top traded stocks:", [ticker for ticker, _ in top_traded_stocks])

for ticker, data in top_traded_stocks:
    total_volume = data['Volume'].sum()
    buy_volume, sell_volume, buy_percentage, sell_percentage = calculate_buy_sell_volume(data)
    print(f"Inserting data for {ticker}: Total Volume={total_volume}, Buy Volume={buy_volume}, Sell Volume={sell_volume}")
    insert_stock_data(cursor, ticker, total_volume, buy_volume, sell_volume)
    conn.commit()  # Commit after each insertion

# Close the database connection
conn.close()
