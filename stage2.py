stage2.py
import yfinance as yf
import pandas as pd
import requests
from bs4 import BeautifulSoup

def get_sp500_tickers():
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    resp = requests.get(url)
    soup = BeautifulSoup(resp.text, "html.parser")
    table = soup.find("table", {"id": "constituents"})
    tickers = [row.findAll("td")[0].text.strip() for row in table.findAll("tr")[1:]]
    return tickers

def get_nasdaq_tickers():
    url = "https://en.wikipedia.org/wiki/NASDAQ-100"
    resp = requests.get(url)
    soup = BeautifulSoup(resp.text, "html.parser")
    table = soup.find("table", {"class": "wikitable"})
    tickers = [row.findAll("td")[1].text.strip() for row in table.findAll("tr")[1:]]
    return tickers

def get_dow_tickers():
    url = "https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average"
    resp = requests.get(url)
    soup = BeautifulSoup(resp.text, "html.parser")
    table = soup.find("table", {"class": "wikitable"})
    tickers = [row.findAll("td")[1].text.strip() for row in table.findAll("tr")[1:]]
    return tickers

def get_russell_2000_tickers():
    # Russell 2000 tickers are not freely available; a static list or API subscription may be required
    return []

def calculate_rsi(data, window=14):
    """Calculate RSI based on closing prices."""
    delta = data['Close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

def check_trend_template(ticker):
    try:
        stock = yf.Ticker(ticker)
        hist = stock.history(period="1y", interval="1d", auto_adjust=True)

        if hist.empty:
            print(f"Skipping {ticker}: No historical data")
            return False

        # Compute moving averages
        hist['50_MA'] = hist['Close'].rolling(window=50).mean()
        hist['150_MA'] = hist['Close'].rolling(window=150).mean()
        hist['200_MA'] = hist['Close'].rolling(window=200).mean()

        # Compute RSI
        hist['RSI'] = calculate_rsi(hist)

        latest = hist.iloc[-1]  # Latest data point

        if any(pd.isna([latest['50_MA'], latest['150_MA'], latest['200_MA'], latest['RSI']])):
            print(f"Skipping {ticker}: Missing moving average or RSI data")
            return False

        # **Step 1: Current price > 150-day MA and > 200-day MA**
        if not (latest['Close'] > latest['150_MA'] and latest['Close'] > latest['200_MA']):
            print(f"Skipping {ticker}: Price is not above 150-day & 200-day MA")
            return False

        # **Step 2: 150-day MA > 200-day MA**
        if not (latest['150_MA'] > latest['200_MA']):
            print(f"Skipping {ticker}: 150-day MA is not above 200-day MA")
            return False

        # **Step 3: 200-day MA is rising (check last 5 days)**
        if not (hist['200_MA'].iloc[-1] > hist['200_MA'].iloc[-5]):
            print(f"Skipping {ticker}: 200-day MA is not trending upward")
            return False

        # **Step 4: 50-day MA > 150-day MA and 50-day MA > 200-day MA**
        if not (latest['50_MA'] > latest['150_MA'] and latest['50_MA'] > latest['200_MA']):
            print(f"Skipping {ticker}: 50-day MA is not above both 150-day and 200-day MA")
            return False

        # **Step 5: Current price > 50-day MA**
        if not (latest['Close'] > latest['50_MA']):
            print(f"Skipping {ticker}: Price is not above 50-day MA")
            return False

        # **Step 6: Current price is 30% above 52-week low**
        min_52_week = hist['Close'].min()
        if not (latest['Close'] >= 1.3 * min_52_week):
            print(f"Skipping {ticker}: Price is not at least 30% above 52-week low")
            return False

        # **Step 7: Current price is within 25% of 52-week high**
        max_52_week = hist['Close'].max()
        if not (latest['Close'] >= 0.75 * max_52_week):
            print(f"Skipping {ticker}: Price is not within 25% of 52-week high")
            return False

        # **Step 8: RSI is at least 70**
        if latest['RSI'] < 70:
            print(f"Skipping {ticker}: RSI ({latest['RSI']:.2f}) is below 70")
            return False

        print(f"{ticker} meets all trend template criteria!")
        return True

    except Exception as e:
        print(f"Error processing {ticker}: {e}")
        return False



# Get tickers from indices
tickers = set(get_sp500_tickers() + get_nasdaq_tickers() + get_dow_tickers() + get_russell_2000_tickers())

# Filter stocks meeting criteria
qualified_stocks = [ticker for ticker in tickers if check_trend_template(ticker)]

print("Stocks meeting trend template criteria:", qualified_stocks)
