import yfinance as yf
import pandas as pd


def get_sp500_tickers():
    """
    Fetch the stock tickers for the S&P 500 index from Wikipedia.
    
    :return: List of S&P 500 stock tickers
    """
    # Fetch S&P 500 stock tickers from Wikipedia
    url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    table = pd.read_html(url)
    
    # The first table on the page contains the S&P 500 tickers
    sp500_df = table[0]
    
    # Get the 'Symbol' column which contains the tickers
    sp500_tickers = sp500_df['Symbol'].tolist()
    
    return sp500_tickers

def get_moving_averages(stock_symbol, period="1y"):
    """
    Fetch stock data and calculate moving averages.

    :param stock_symbol: Stock ticker symbol (e.g., AAPL for Apple)
    :param period: Period for historical data (default is 1 year)
    :return: DataFrame with stock data and moving averages
    """
    # Download stock data
    stock_data = yf.download(stock_symbol, period=period)
    
    # Calculate 50-day and 200-day moving averages
    stock_data['50_MA'] = stock_data['Close'].rolling(window=50).mean()
    stock_data['200_MA'] = stock_data['Close'].rolling(window=200).mean()
    
    return stock_data

def is_stock_in_strong_uptrend(stock_data):
    """
    Check if a stock is in a strong uptrend based on multiple criteria:
    
    - The 50-day moving average must be well above the 200-day moving average
    - The stock's price must have risen significantly in the last 10 days
    - The RSI must be in a moderate range (e.g., 30 < RSI < 70)
    - The volume must show a surge compared to its 10-day average
    
    :param stock_data: DataFrame containing stock data with moving averages
    :return: Boolean indicating whether the stock passes all strict uptrend criteria
    """
    # Calculate the RSI (Relative Strength Index)
    delta = stock_data['Close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    stock_data['RSI'] = 100 - (100 / (1 + rs))
    
    # Calculate 10-day price change percentage
    stock_data['10_day_change'] = stock_data['Close'].pct_change(periods=10) * 100
    
    # Calculate the 10-day average volume
    stock_data['Avg_Volume_10'] = stock_data['Volume'].rolling(window=10).mean()
    
    # Ensure the last day of data is available for calculations
    if stock_data['50_MA'].isna().any() or stock_data['200_MA'].isna().any():
        return False

    # Criteria for a strong uptrend:
    last_price = stock_data['Close'].iloc[-1]
    last_50_ma = stock_data['50_MA'].iloc[-1]
    last_200_ma = stock_data['200_MA'].iloc[-1]
    last_rsi = stock_data['RSI'].iloc[-1]
    last_10_day_change = stock_data['10_day_change'].iloc[-1]
    last_avg_volume = stock_data['Avg_Volume_10'].iloc[-1]
    current_volume = stock_data['Volume'].iloc[-1]
    
    # Criteria 1: 50-MA must be significantly higher than 200-MA
    if not last_50_ma > last_200_ma * 1.05:
        return False
    
    # Criteria 2: Price should have increased significantly in the last 10 days (e.g., > 5%)
    if last_10_day_change < 2:
        return False
    
    # # Criteria 3: RSI should be moderate (between 30 and 70)
    # if not (30 < last_rsi < 70):
    #     return False
    
    # Criteria 4: Current volume should be higher than the average volume (volume surge)
    if current_volume < last_avg_volume * 1.2:
        return False

    return True

def find_uptrend_stocks(stock_symbols):
    """
    Find stocks that are in a strong uptrend from a list of stock symbols.

    :param stock_symbols: List of stock ticker symbols
    :return: List of stocks that pass the strict uptrend criteria
    """
    uptrend_stocks = []
    
    for symbol in stock_symbols:
        try:
            stock_data = get_moving_averages(symbol)
            if is_stock_in_strong_uptrend(stock_data):
                uptrend_stocks.append(symbol)
        except Exception as e:
            print(f"Error processing {symbol}: {e}")
    
    return uptrend_stocks




# List of stock tickers to track (you can update this list with more tickers)
#tickers = ["AAPL", "MSFT", "TSLA", "AMZN", "GOOGL", "NVDA", "META"]
tickers = get_sp500_tickers()
# Dictionary to store volume data
volume_data = {}

# Fetch data for each ticker in the list
for ticker in tickers:
    stock = yf.Ticker(ticker)
    # Get data for the last 24 hours (this fetches the last day's trading data)
    hist = stock.history(period="1d")
    
    # Store the volume data (trading volume)
    if not hist.empty:
        volume_data[ticker] = hist["Volume"].iloc[0]

# Convert the volume data to a DataFrame for easy sorting
volume_df = pd.DataFrame(volume_data.items(), columns=["Ticker", "Volume"])

# Sort the stocks by volume in descending order to find the most traded
volume_df = volume_df.sort_values(by="Volume", ascending=False)

# Display the most traded stock
most_traded_stock = volume_df.iloc[0]
print(f"Most traded stock in the last 24 hours: {most_traded_stock['Ticker']} with volume {most_traded_stock['Volume']}")

# Show the entire DataFrame
print(volume_df)
print("testing build in trigger")


# Find stocks in an uptrend
uptrend_stocks = find_uptrend_stocks(tickers)

print("Stocks in an uptrend:")
for stock in uptrend_stocks:
    print(stock)