import pandas as pd
import yfinance as yf
from ta.momentum import RSIIndicator
from ta.trend import PSARIndicator
import warnings


# Suppress FutureWarnings
warnings.filterwarnings("ignore", category=FutureWarning)

# Function to fetch the S&P 500 stock list from Wikipedia
def get_sp500_stocks():
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    sp500_table = pd.read_html(url, header=0)[0]
    return sp500_table['Symbol'].tolist()

# Function to fetch recent stock data (skip delisted stocks)
def get_latest_stock_data(symbol):
    ticker = yf.Ticker(symbol)
    try:
        # Fetch recent data for the last 6 months
        data = ticker.history(period="6mo")
        if data.empty:  # Check if the data is empty (delisted or no data)
            print(f"Skipping {symbol}: possibly delisted; no price data found")
            return None  # Return None if no data found
        return data
    except Exception as e:
        print(f"Error fetching data for {symbol}: {e}")
        return None  # Return None on any error

# Calculate Bollinger Bands
def bollinger_bands(data, window=20, no_of_std=2):
    rolling_mean = data['Close'].rolling(window).mean()
    rolling_std = data['Close'].rolling(window).std()

    data['Bollinger Upper'] = rolling_mean + (rolling_std * no_of_std)
    data['Bollinger Lower'] = rolling_mean - (rolling_std * no_of_std)
    data['Rolling Mean'] = rolling_mean
    return data

# Calculate RSI (needs a series of data, not just one point)
def calculate_rsi(data, window=14):
    rsi = RSIIndicator(data['Close'], window=window)
    data['RSI'] = rsi.rsi()
    return data

# Calculate Moving Averages (used to detect uptrend)
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
    data['%D'] = data['%K'].rolling(window=d_period).mean()  # 3-day moving average of %K
    return data

# Calculate Parabolic SAR
def calculate_parabolic_sar(data):
    psar_indicator = PSARIndicator(data['High'], data['Low'], data['Close'], step=0.02, max_step=0.2)
    data['PSAR'] = psar_indicator.psar()
    return data

# Signal strategy that generates buy/sell signals
def signal_strategy(data, proximity_percentage=0.02, rsi_buy_threshold=30, rsi_sell_threshold=65):
    def check_signal(row):
        price = row['Close']
        lower_band = row['Bollinger Lower']
        upper_band = row['Bollinger Upper']
        rsi = row['RSI']
        macd = row['MACD']
        signal_line = row['Signal Line']
        stochastic_k = row['%K']
        psar = row['PSAR']

        # Buy Condition
        if pd.notna(price) and pd.notna(lower_band) and pd.notna(rsi):
            if price <= lower_band * (1 + proximity_percentage) and rsi > rsi_buy_threshold and stochastic_k < 20:
                return 'Buy'

        # Relaxed Sell Condition
        if pd.notna(price) and pd.notna(upper_band) and pd.notna(rsi) and pd.notna(macd) and pd.notna(signal_line):
            if price >= upper_band * (1 - proximity_percentage) and rsi < (rsi_sell_threshold + 5):  # Slightly relaxed RSI
                if (macd < signal_line) or (stochastic_k > 80 and price < psar):
                    return 'Sell'

        return 'Hold'


    data['Signal'] = data.apply(lambda row: check_signal(row), axis=1)

    return data

# Main function to check the latest stock price and generate buy/sell signals
if __name__ == "__main__":
    # Get the list of S&P 500 stocks
    sp500_symbols = get_sp500_stocks()

    # Create list to store stock signals and indicator data
    stock_signal_data = []

    # Loop through each stock symbol in the S&P 500
    for symbol in sp500_symbols:
        try:
            # Get the recent stock data (last 6 months for more robust calculation)
            stock_data = get_latest_stock_data(symbol)

            # Skip if stock is delisted or there's no data
            if stock_data is None:
                continue

            # Skip if there's not enough data for indicators
            if len(stock_data) < 50:  # Need at least 50 days for moving average
                continue

            # Calculate Bollinger Bands
            stock_data = bollinger_bands(stock_data)

            # Calculate RSI
            stock_data = calculate_rsi(stock_data)

            # Calculate Moving Averages for trend detection
            stock_data = calculate_moving_average(stock_data)

            # Calculate MACD and Signal Line
            stock_data = calculate_macd(stock_data)

            # Calculate Stochastic Oscillator
            stock_data = calculate_stochastic(stock_data)

            # Calculate Parabolic SAR
            stock_data = calculate_parabolic_sar(stock_data)

            # Generate buy/sell signals for the latest data
            stock_data = signal_strategy(stock_data)

            # Get the latest entry for signals
            latest_data = stock_data.iloc[-1]

            # Store all relevant data into a list
            stock_signal_data.append({
                'Stock': symbol,
                'Close': latest_data['Close'],
                'Bollinger Lower': latest_data['Bollinger Lower'],
                'Bollinger Upper': latest_data['Bollinger Upper'],
                'RSI': latest_data['RSI'],
                '%K': latest_data['%K'],
                'MACD': latest_data['MACD'],
                'Signal Line': latest_data['Signal Line'],
                'PSAR': latest_data['PSAR'],
                'Moving Average 50': latest_data['Moving Average 50'],
                'Moving Average 200': latest_data['Moving Average 200'],
                'Signal': latest_data['Signal']
            })

        except Exception as e:
            print(f"Error processing {symbol}: {e}")
            continue


    # Convert the list to a DataFrame
    df_signals = pd.DataFrame(stock_signal_data)

    # Filter out only 'Buy' and 'Sell' signals
    buy_sell_df = df_signals[df_signals['Signal'].isin(['Buy', 'Sell'])]

    # Filter for Buy and Sell signals
    buy_list = buy_sell_df[buy_sell_df['Signal'] == 'Buy']['Stock'].tolist()
    sell_list = buy_sell_df[buy_sell_df['Signal'] == 'Sell']['Stock'].tolist()

    # Print the lists
    print("Buy List:", buy_list)
    print("Sell List:", sell_list)