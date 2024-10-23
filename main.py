import yfinance as yf
import pandas as pd

# List of stock tickers to track (you can update this list with more tickers)
tickers = ["AAPL", "MSFT", "TSLA", "AMZN", "GOOGL", "NVDA", "META"]

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
