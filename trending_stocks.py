import yfinance as yf
import pandas as pd

# Parameters to define a "trending" stock
volume_threshold = 1_000_000  # Minimum volume to be considered trending
price_change_threshold = 2  # Minimum percentage price change

# Fetch S&P 500 stock tickers
sp500_tickers = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")[0]["Symbol"].tolist()

# Fetch historical data for all tickers at once
data = yf.download(sp500_tickers, period="5d", interval="1d", group_by='ticker', auto_adjust=True, threads=True)

# Initialize a list to store trending stocks
trending_stocks = []

# Process each stock's data
for ticker in sp500_tickers:
    try:
        hist = data[ticker]
        
        # Skip if data is missing or insufficient
        if len(hist) < 2:
            continue
        
        # Calculate volume and price change
        volume = hist["Volume"].iloc[-1]
        price_change = ((hist["Close"].iloc[-1] - hist["Close"].iloc[-2]) / hist["Close"].iloc[-2]) * 100
        
        # Check if stock meets criteria
        if volume > volume_threshold and abs(price_change) > price_change_threshold:
            trending_stocks.append({
                "Ticker": ticker,
                "Volume": volume,
                "Price Change (%)": round(price_change, 2)
            })
    except Exception as e:
        print(f"Error processing data for {ticker}: {e}")

# Convert the list of trending stocks to a DataFrame
trending_df = pd.DataFrame(trending_stocks)

if not trending_df.empty:
    # Sort by 'Price Change (%)' in descending order
    trending_df = trending_df.sort_values(by="Price Change (%)", ascending=False).reset_index(drop=True)
    print("Trending Stocks (Sorted by Price Change %):")
    print(trending_df)
else:
    print("No trending stocks found based on the criteria.")
