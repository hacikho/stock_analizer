import numpy as np
import tensorflow as tf
import os
import yfinance as yf
import pandas as pd
from tensorflow.keras.models import Sequential, load_model
from tensorflow.keras.layers import LSTM, Dense, Dropout
from sklearn.preprocessing import MinMaxScaler
from prophet import Prophet
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime

# Set random seeds for reproducibility
np.random.seed(42)
tf.random.set_seed(42)

# Disable GPU for full determinism
os.environ["CUDA_VISIBLE_DEVICES"] = "-1"

# Download data from Yahoo Finance
def download_stock_data(ticker, period="10y"):
    data = yf.download(ticker, period=period, interval="1mo")
    data = data['Close'].resample('Q').last().dropna()
    return data

# Prepare data for LSTM
def prepare_lstm_data(data):
    scaler = MinMaxScaler(feature_range=(0, 1))
    scaled_data = scaler.fit_transform(data.values.reshape(-1, 1))
    
    X, y = [], []
    for i in range(4, len(scaled_data)):
        X.append(scaled_data[i-4:i, 0])
        y.append(scaled_data[i, 0])
    
    X, y = np.array(X), np.array(y)
    X = np.reshape(X, (X.shape[0], X.shape[1], 1))
    return X, y, scaler

# Build and train LSTM model, then save it
def build_train_lstm_model(X_train, y_train, model_path="lstm_model.h5"):
    model = Sequential()
    model.add(LSTM(units=50, return_sequences=True, input_shape=(X_train.shape[1], 1)))
    model.add(Dropout(0.2))
    model.add(LSTM(units=50, return_sequences=False))
    model.add(Dropout(0.2))
    model.add(Dense(units=1))
    
    model.compile(optimizer='adam', loss='mean_squared_error')
    model.fit(X_train, y_train, epochs=50, batch_size=32)
    
    # Save the trained model
    model.save(model_path)
    return model

# Load LSTM model if it exists
def load_lstm_model(model_path="lstm_model.h5"):
    return load_model(model_path)

# Predict using LSTM
def lstm_predict(model, data, scaler):
    last_4_quarters = data[-4:].values.reshape(-1, 1)
    last_4_quarters_scaled = scaler.transform(last_4_quarters)
    X_test = np.reshape(last_4_quarters_scaled, (1, 4, 1))
    
    predicted_price_scaled = model.predict(X_test)
    predicted_price = scaler.inverse_transform(predicted_price_scaled)
    return predicted_price[0, 0]

# Determine the current quarter and its end date
def get_current_quarter_end():
    now = datetime.now()
    year = now.year
    month = now.month
    if month in [1, 2, 3]:
        quarter_end_date = datetime(year, 3, 31)
    elif month in [4, 5, 6]:
        quarter_end_date = datetime(year, 6, 30)
    elif month in [7, 8, 9]:
        quarter_end_date = datetime(year, 9, 30)
    else:
        quarter_end_date = datetime(year, 12, 31)
    return quarter_end_date

# Prophet Forecast for End of Current Quarter
def prophet_forecast_end_of_current_quarter(data, quarter_end_date):
    prophet_data = data.reset_index()
    prophet_data.columns = ['ds', 'y']
    prophet_data['ds'] = prophet_data['ds'].dt.tz_localize(None)
    
    model = Prophet(yearly_seasonality=True, seasonality_mode='multiplicative')
    model.fit(prophet_data)
    
    # Forecast to the end of the current quarter
    future = model.make_future_dataframe(periods=1, freq='Q')
    forecast = model.predict(future)
    
    # Find prediction specifically for the end of the current quarter
    prediction = forecast[forecast['ds'] == quarter_end_date]['yhat'].values[0]
    return prediction

# Main function
def main():
    ticker = "NVDA"  # NVIDIA ticker for earnings prediction
    data = download_stock_data(ticker)

    # Determine end of the current quarter
    quarter_end_date = get_current_quarter_end()

    # Prepare data for LSTM
    X, y, scaler = prepare_lstm_data(data)

    # Load the LSTM model if it exists, otherwise train a new one and save it
    model_path = "lstm_model.h5"
    if os.path.exists(model_path):
        model = load_lstm_model(model_path)
        print("Loaded LSTM model from file.")
    else:
        model = build_train_lstm_model(X, y, model_path=model_path)
        print("Trained and saved new LSTM model.")

    # Make LSTM prediction
    lstm_prediction = lstm_predict(model, data, scaler)
    print(f"LSTM Prediction for end of current quarter: {lstm_prediction:.2f}")
    
    # Predict with Prophet for end of current quarter
    prophet_prediction = prophet_forecast_end_of_current_quarter(data, quarter_end_date)
    print(f"Prophet Prediction for end of current quarter: {prophet_prediction:.2f}")
    
    # Visualize
    plt.figure(figsize=(14, 7))
    plt.plot(data, label="Historical Data", color="blue")

    # Annotate historical data values
    for date, price in zip(data.index, data.iloc[:, 0]):
        plt.text(date, price, f"{price:.2f}", ha="center", va="bottom", fontsize=8, color="blue")
    
    # Add horizontal lines for LSTM and Prophet predictions
    plt.axhline(y=lstm_prediction, color="orange", linestyle="--", label="LSTM Prediction (End of Current Quarter)")
    plt.axhline(y=prophet_prediction, color="green", linestyle="--", label="Prophet Prediction (End of Current Quarter)")

    # Annotate prediction values on the right side
    plt.text(data.index[-1], lstm_prediction, f"{lstm_prediction:.2f}", color="orange", ha="left", va="center", fontsize=10, fontweight="bold")
    plt.text(data.index[-1], prophet_prediction, f"{prophet_prediction:.2f}", color="green", ha="left", va="center", fontsize=10, fontweight="bold")
    
    plt.legend()
    
    # Add quarter format to x-axis
    ax = plt.gca()
    ax.xaxis.set_major_locator(mdates.YearLocator())               # Set major ticks to each year
    ax.xaxis.set_minor_locator(mdates.MonthLocator([1, 4, 7, 10])) # Set minor ticks to each quarter (Jan, Apr, Jul, Oct)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))       # Show year on major ticks
    
    # Custom function for quarter labels
    def format_quarters(x, pos):
        month = mdates.num2date(x).month
        quarter = (month - 1) // 3 + 1
        return f"Q{quarter}"
    
    # Set minor tick formatter to quarters
    ax.xaxis.set_minor_formatter(plt.FuncFormatter(format_quarters))
    
    # Rotate quarter labels for readability
    plt.setp(ax.get_xticklabels(minor=True), rotation=45)

    # Add vertical lines at each quarter
    for quarter_date in data.index:
        plt.axvline(x=quarter_date, color="grey", linestyle="--", linewidth=0.5)
    
    plt.title(f"Predictions for {ticker} - End of Current Quarter")
    plt.show()

if __name__ == "__main__":
    main()
