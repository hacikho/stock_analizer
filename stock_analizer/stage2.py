import os
import asyncio
import aiohttp
import pandas as pd
import datetime
from fastapi import FastAPI, HTTPException
from dotenv import load_dotenv
from cachetools import TTLCache


# Use this code snippet in your app.
# If you need more information about configurations
# or implementing the sample code, visit the AWS docs:
# https://aws.amazon.com/developer/language/python/

import boto3
from botocore.exceptions import ClientError


def get_secret():

    secret_name = "polygon_api"
    region_name = "us-east-2"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    secret = get_secret_value_response['SecretString']

    # Your code goes here.

env = os.getenv("ENVIRONMENT", "local").lower()

if env == "aws":
    api_key = get_secret()
else:
    # Load .env variables for local development
    load_dotenv()
    API_KEY = os.getenv('API_KEY')


app = FastAPI()

# Cache S&P 500 tickers for 1 hour (3600 seconds)
sp500_cache = TTLCache(maxsize=1, ttl=3600)

# Function to fetch S&P 500 tickers
async def get_sp500_tickers():
    if "tickers" in sp500_cache:
        return sp500_cache["tickers"]
    
    url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    dfs = pd.read_html(url, header=0)
    tickers = dfs[0]['Symbol'].tolist()
    sp500_cache["tickers"] = tickers
    return tickers

# Generate dynamic URL
def get_dynamic_url(ticker):
    today = datetime.date.today()
    one_year_ago = today - datetime.timedelta(days=365)
    return f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{one_year_ago}/{today}"

# Function to fetch historical data from Polygon.io
async def get_polygon_data(ticker, session):
    url = get_dynamic_url(ticker)
    params = {"apiKey": API_KEY}
    
    try:
        timeout = aiohttp.ClientTimeout(total=10)  # ⏰ 10 seconds timeout
        async with session.get(url, params=params, timeout=timeout) as response:
            if response.status != 200:
                print(f"Error fetching {ticker}: Status {response.status}")
                return None
            data = await response.json()
            if data.get('results'):
                df = pd.DataFrame(data['results'])
                df['timestamp'] = pd.to_datetime(df['t'], unit='ms')
                df.set_index('timestamp', inplace=True)
                df['close'] = df['c']
                return df[['close']]
            else:
                return None
    except asyncio.TimeoutError:
        print(f"Timeout fetching {ticker}")
        return None
    except Exception as e:
        print(f"Exception fetching {ticker}: {e}")
        return None

# Calculate RSI
def calculate_rsi(data, window=14):
    delta = data.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

# Check trend template
async def check_trend_template(ticker):
    async with aiohttp.ClientSession() as session:
        hist = await get_polygon_data(ticker, session)
        
        if hist is None or hist.empty:
            return False

        hist['50_MA'] = hist['close'].rolling(window=50).mean()
        hist['150_MA'] = hist['close'].rolling(window=150).mean()
        hist['200_MA'] = hist['close'].rolling(window=200).mean()
        hist['RSI'] = calculate_rsi(hist['close'])
        
        latest = hist.iloc[-1]
        
        if any(pd.isna([latest['50_MA'], latest['150_MA'], latest['200_MA'], latest['RSI']])):
            return False

        if not (latest['close'] > latest['150_MA'] and latest['close'] > latest['200_MA']):
            return False
        if not (latest['150_MA'] > latest['200_MA']):
            return False
        if not (hist['200_MA'].iloc[-1] > hist['200_MA'].iloc[-5]):
            return False
        if not (latest['50_MA'] > latest['150_MA'] and latest['50_MA'] > latest['200_MA']):
            return False
        if not (latest['close'] > latest['50_MA']):
            return False
        min_52_week = hist['close'].min()
        if not (latest['close'] >= 1.3 * min_52_week):
            return False
        max_52_week = hist['close'].max()
        if not (latest['close'] >= 0.75 * max_52_week):
            return False
        if latest['RSI'] < 70:
            return False

        return True

# Endpoint to get trending stocks
@app.get("/trending_stocks")
async def trending_stocks():
    tickers = await get_sp500_tickers()

    # 🔥 Parallel execution
    tasks = [check_trend_template(ticker) for ticker in tickers]
    results = await asyncio.gather(*tasks)

    qualified_stocks = [ticker for ticker, passed in zip(tickers, results) if passed]
    
    return {"qualified_stocks": qualified_stocks}

# Manual refresh endpoint
@app.get("/refresh_cache")
async def refresh_cache():
    sp500_cache.clear()
    return {"message": "S&P 500 tickers cache cleared."}
