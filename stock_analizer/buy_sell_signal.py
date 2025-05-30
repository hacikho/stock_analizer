import pandas as pd
import yfinance as yf
from ta.momentum import RSIIndicator
from ta.trend import PSARIndicator
import warnings
import re
import sqlite3
from datetime import datetime, timedelta, timezone
import pytz

# Suppress FutureWarnings
warnings.filterwarnings("ignore", category=FutureWarning)

# Database connection setup
db_path = '/home/ubuntu/stock_analysis.db'

# Connect to SQLite and create table if not exists
def create_database():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS buy_sell_signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            close_price REAL,
            signal TEXT,
            timestamp_utc TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()

# Known unsupported symbols
unsupported_symbols = ["BRK.B", "BF.B"]

# Fetch S&P 500 stock list
def get_sp500_stocks():
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    sp500_table = pd.read_html(url, header=0)[0]
    symbols = sp500_table['Symbol'].tolist()
    symbols = [symbol.replace(".", "-") for symbol in symbols if re.match(r'^[A-Za-z0-9]+$', symbol) and symbol not in unsupported_symbols]
    return symbols

# Fetch recent stock data
def get_latest_stock_data(symbol):
    ticker = yf.Ticker(symbol)
    for period in ["6mo", "3mo", "1mo"]:  
        try:
            data = ticker.history(period=period)
            if not data.empty:
                return data
            print(f"Skipping {symbol}: no price data for period '{period}'")
        except Exception as e:
            print(f"Error fetching data for {symbol} (period={period}): {e}")
    return None

# Calculate Bollinger Bands
def bollinger_bands(data, window=20, no_of_std=2):
    rolling_mean = data['Close'].rolling(window).mean()
    rolling_std = data['Close'].rolling(window).std()
    data['Bollinger Upper'] = rolling_mean + (rolling_std * no_of_std)
    data['Bollinger Lower'] = rolling_mean - (rolling_std * no_of_std)
    data['Rolling Mean'] = rolling_mean
    return data

# Calculate RSI
def calculate_rsi(data, window=14):
    rsi = RSIIndicator(data['Close'], window=window)
    data['RSI'] = rsi.rsi()
    return data

# Calculate Moving Averages
def calculate_moving_average(data, windows=[50, 100, 200]):
    for window in windows:
        data[f'Moving Average {window}'] = data['Close'].rolling(window=window).mean()
    return data

# Calculate MACD and Signal Line
def calculate_macd(data, fast_period=12, slow_period=26, signal_period=9):
    data['EMA_12'] = data['Close'].ewm(span=fast_period, adjust=False).mean()
    data['EMA_26'] = data['Close'].ewm(span=slow_period, adjust=False).mean()
    data['MACD'] = data['EMA_12'] - data['EMA_26']
    data['Signal Line'] = data['MACD'].ewm(span=signal_period, adjust=False).mean()
    return data

# Calculate Stochastic Oscillator
def calculate_stochastic(data, k_period=14, d_period=3):
    data['Low_14'] = data['Low'].rolling(window=k_period).min()
    data['High_14'] = data['High'].rolling(window=k_period).max()
    data['%K'] = 100 * ((data['Close'] - data['Low_14']) / (data['High_14'] - data['Low_14']))
    data['%D'] = data['%K'].rolling(window=d_period).mean()
    return data

# Calculate Parabolic SAR
def calculate_parabolic_sar(data):
    psar_indicator = PSARIndicator(data['High'], data['Low'], data['Close'], step=0.02, max_step=0.2)
    data['PSAR'] = psar_indicator.psar()
    return data

# Buy/Sell Signal Strategy
def signal_strategy(data):
    proximity_percentage = min(0.02, data['Close'].pct_change().rolling(window=10).std().iloc[-1] * 1.5)

    def check_signal(row):
        price = row['Close']
        lower_band = row['Bollinger Lower']
        upper_band = row['Bollinger Upper']
        rsi = row['RSI']
        macd = row['MACD']
        signal_line = row['Signal Line']
        stochastic_k = row['%K']
        psar = row['PSAR']
        moving_avg_200 = row['Moving Average 200']
        avg_volume = row['Avg Volume']
        volume = row['Volume']

        if moving_avg_200 and price < moving_avg_200:
            return 'Hold'  

        if pd.notna(avg_volume) and volume < avg_volume * 1.2:
            return 'Hold' 

        if pd.notna(price) and pd.notna(lower_band) and pd.notna(rsi):
            if price <= lower_band * (1 + proximity_percentage) and rsi > 30 and stochastic_k < 20 and price > psar:
                return 'Buy'

        if pd.notna(price) and pd.notna(upper_band) and pd.notna(rsi) and pd.notna(macd) and pd.notna(signal_line):
            if price >= upper_band * (1 - proximity_percentage) and rsi < 70:
                if (macd < signal_line) or (stochastic_k > 80 and price < psar):
                    return 'Sell'
        
        return 'Hold'

    data['Signal'] = data.apply(lambda row: check_signal(row), axis=1)
    return data

# Insert buy/sell signal into the SQLite database
def insert_signal_to_db(symbol, close_price, signal):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    timestamp_utc = datetime.now(timezone.utc)
    cursor.execute('''
        INSERT INTO buy_sell_signals (symbol, close_price, signal, timestamp_utc)
        VALUES (?, ?, ?, ?)
    ''', (symbol, close_price, signal, timestamp_utc))
    conn.commit()
    conn.close()

# Function to delete data older than a month
def delete_old_data():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    one_month_ago = datetime.now(timezone.utc) - timedelta(days=30)
    cursor.execute('DELETE FROM buy_sell_signals WHERE timestamp_utc < ?', (one_month_ago,))
    conn.commit()
    conn.close()

# Main function to process buy/sell signals
if __name__ == "__main__":
    # Setup database and create table
    create_database()
    delete_old_data()

    sp500_symbols = get_sp500_stocks()

    for symbol in sp500_symbols:
        try:
            stock_data = get_latest_stock_data(symbol)
            if stock_data is None or len(stock_data) < 50:
                continue

            # Calculate indicators
            stock_data = bollinger_bands(stock_data)
            stock_data = calculate_rsi(stock_data)
            stock_data = calculate_moving_average(stock_data)
            stock_data = calculate_macd(stock_data)
            stock_data = calculate_stochastic(stock_data)
            stock_data = calculate_parabolic_sar(stock_data)
            stock_data['Avg Volume'] = stock_data['Volume'].rolling(window=20).mean()
            stock_data = signal_strategy(stock_data)

            # Insert latest signals into the database
            latest_data = stock_data.iloc[-1]
            if latest_data['Signal'] in ['Buy', 'Sell']:
                insert_signal_to_db(
                    symbol=symbol,
                    close_price=latest_data['Close'],
                    signal=latest_data['Signal']
                )

        except Exception as e:
            print(f"Error processing {symbol}: {e}")
            continue
