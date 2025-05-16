import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
import boto3
from botocore.exceptions import ClientError
from cachetools import TTLCache

# -----------------------------------------------------------------------------
# 1) Secret‐manager helper now returns the secret string
# -----------------------------------------------------------------------------
def get_secret():
    secret_name = "polygon_api"
    region_name = "us-east-2"

    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)

    try:
        resp = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        raise e

    return resp['SecretString']

# -----------------------------------------------------------------------------
# 2) Load API_KEY exactly once, from Secrets Manager if on AWS, else from .env
# -----------------------------------------------------------------------------
env = os.getenv("ENVIRONMENT", "local").lower()
if env == "aws":
    API_KEY = get_secret()
else:
    load_dotenv()  # reads .env into os.environ
    API_KEY = os.getenv("API_KEY")

if not API_KEY:
    raise RuntimeError("Polygon API key not found in environment or Secrets Manager")

# -----------------------------------------------------------------------------
# 3) S&P 500 ticker fetcher as a normal (synchronous) function
# -----------------------------------------------------------------------------
sp500_cache = TTLCache(maxsize=1, ttl=3600)

def get_sp500_tickers():
    if "tickers" in sp500_cache:
        return sp500_cache["tickers"]
    url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    dfs = pd.read_html(url, header=0)
    tickers = dfs[0]['Symbol'].tolist()
    sp500_cache["tickers"] = tickers
    return tickers

# -----------------------------------------------------------------------------
# 4) Polygon bars fetcher unchanged
# -----------------------------------------------------------------------------
def fetch_polygon_bars(ticker: str, from_date: str, to_date: str, api_key: str):
    url = (
        f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/"
        f"{from_date}/{to_date}"
    )
    params = {
        "adjusted": "true",
        "sort": "asc",
        "limit": 50000,
        "apiKey": api_key
    }
    resp = requests.get(url, params=params)
    resp.raise_for_status()
    data = resp.json().get("results", [])
    if not data:
        raise ValueError(f"No data returned for {ticker}")
    df = pd.DataFrame(data)
    df["date"] = pd.to_datetime(df["t"], unit="ms")
    df.set_index("date", inplace=True)
    df.rename(columns={"o":"Open","h":"High","l":"Low","c":"Close","v":"Volume"}, inplace=True)
    return df[["Open","High","Low","Close","Volume"]]

# -----------------------------------------------------------------------------
# 5) Golden‐cross checker uses our global API_KEY
# -----------------------------------------------------------------------------
def has_golden_cross_polygon(ticker: str) -> bool:
    end_dt = datetime.now()
    start_dt = end_dt - timedelta(days=365)
    from_str, to_str = start_dt.strftime("%Y-%m-%d"), end_dt.strftime("%Y-%m-%d")

    df = fetch_polygon_bars(ticker, from_str, to_str, API_KEY)
    df["MA50"]  = df["Close"].rolling(window=50).mean()
    df["MA200"] = df["Close"].rolling(window=200).mean()
    df.dropna(subset=["MA50","MA200"], inplace=True)

    df["Signal"] = (df["MA50"] > df["MA200"]).astype(int)
    df["Cross"]  = df["Signal"].diff()

    cutoff = end_dt - timedelta(days=14)
    recent = df[(df.index >= cutoff) & (df["Cross"] == 1)]

    if not recent.empty:
        dates = recent.index.strftime("%Y-%m-%d").tolist()
        print(f"✅ {ticker}: golden cross on {', '.join(dates)}")
        return True
    else:
        print(f"❌ {ticker}: no golden cross in last 2 weeks")
        return False

# -----------------------------------------------------------------------------
# 6) Main block now just calls the synchronous get_sp500_tickers()
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    tickers = get_sp500_tickers()
    for sym in tickers:
        try:
            has_golden_cross_polygon(sym)
        except Exception as e:
            print(f"Error for {sym}: {e}")
