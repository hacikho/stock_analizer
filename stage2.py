import requests
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()  # Load environment variables from .env
API_KEY = os.getenv('API_KEY')

# Fetch the list of S&P 500 tickers (same as before)
def get_sp500_tickers():
    url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    table = pd.read_html(url, header=0)
    df = table[0]
    tickers = df['Symbol'].tolist()  
    return tickers

import datetime

def get_dynamic_url(ticker):
    # Get today's date and the date one year ago
    today = datetime.datetime.today().date()
    one_year_ago = today - datetime.timedelta(days=365)

    # Format the dates as strings in 'YYYY-MM-DD' format
    today_str = today.strftime('%Y-%m-%d')
    one_year_ago_str = one_year_ago.strftime('%Y-%m-%d')

    # Use the formatted dates to construct the URL
    url = f'https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{one_year_ago_str}/{today_str}'
    return url
    

# Function to fetch historical data from Polygon.io
def get_polygon_data(ticker):
    url = get_dynamic_url(ticker)
    params = {
        'apiKey': API_KEY
    }
    response = requests.get(url, params=params)
    
    if response.status_code == 200:
        data = response.json()
        if data.get('results'):
            # Convert to a pandas DataFrame
            df = pd.DataFrame(data['results'])
            df['timestamp'] = pd.to_datetime(df['t'], unit='ms')
            df.set_index('timestamp', inplace=True)
            df['close'] = df['c']  # Close price
            return df[['close']]  # Only return the close prices
    else:
        print(f"Error fetching data for {ticker}: {response.status_code}")
        return None

# Calculate RSI
def calculate_rsi(data, window=14):
    delta = data.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

# Trend filter function
def check_trend_template(ticker):
    try:
        # Fetch historical data from Polygon.io
        hist = get_polygon_data(ticker)
        if hist is None or hist.empty:
            print(f"Skipping {ticker}: No historical data")
            return False

        # Calculate the moving averages and RSI
        hist['50_MA'] = hist['close'].rolling(window=50).mean()
        hist['150_MA'] = hist['close'].rolling(window=150).mean()
        hist['200_MA'] = hist['close'].rolling(window=200).mean()
        hist['RSI'] = calculate_rsi(hist['close'])

        latest = hist.iloc[-1] 

        if any(pd.isna([latest['50_MA'], latest['150_MA'], latest['200_MA'], latest['RSI']])):
            print(f"Skipping {ticker}: Missing moving average or RSI data")
            return False

        # Step 1: Current price > 150-day MA and > 200-day MA
        if not (latest['close'] > latest['150_MA'] and latest['close'] > latest['200_MA']):
            print(f"Skipping {ticker}: Price is not above 150-day & 200-day MA")
            return False

        # Step 2: 150-day MA > 200-day MA
        if not (latest['150_MA'] > latest['200_MA']):
            print(f"Skipping {ticker}: 150-day MA is not above 200-day MA")
            return False

        # Step 3: 200-day MA is rising (check last 5 days)
        if not (hist['200_MA'].iloc[-1] > hist['200_MA'].iloc[-5]):
            print(f"Skipping {ticker}: 200-day MA is not trending upward")
            return False

        # Step 4: 50-day MA > 150-day MA and 50-day MA > 200-day MA
        if not (latest['50_MA'] > latest['150_MA'] and latest['50_MA'] > latest['200_MA']):
            print(f"Skipping {ticker}: 50-day MA is not above both 150-day and 200-day MA")
            return False

        # Step 5: Current price > 50-day MA
        if not (latest['close'] > latest['50_MA']):
            print(f"Skipping {ticker}: Price is not above 50-day MA")
            return False

        # Step 6: Current price is 30% above 52-week low
        min_52_week = hist['close'].min()
        if not (latest['close'] >= 1.3 * min_52_week):
            print(f"Skipping {ticker}: Price is not at least 30% above 52-week low")
            return False

        # Step 7: Current price is within 25% of 52-week high
        max_52_week = hist['close'].max()
        if not (latest['close'] >= 0.75 * max_52_week):
            print(f"Skipping {ticker}: Price is not within 25% of 52-week high")
            return False

        # Step 8: RSI is at least 70
        if latest['RSI'] < 70:
            print(f"Skipping {ticker}: RSI ({latest['RSI']:.2f}) is below 70")
            return False

        print(f"{ticker} meets all trend template criteria!")
        return True

    except Exception as e:
        print(f"Error processing {ticker}: {e}")
        return False

# Get tickers from S&P 500 (or other indices like NASDAQ, Dow Jones, etc.)
tickers = get_sp500_tickers()

# Apply the 8-step filtering to each ticker
qualified_stocks = [ticker for ticker in tickers if check_trend_template(ticker)]

# Print the stocks that meet the trend template criteria
print("Stocks meeting trend template criteria:")
print(qualified_stocks)
