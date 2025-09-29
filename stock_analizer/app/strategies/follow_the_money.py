
# Major SPDR sector ETFs
sector_etfs = {
    "XLC": "Communication Services",
    "XLY": "Consumer Discretionary",
    "XLP": "Consumer Staples",
    "XLE": "Energy",
    "XLF": "Financials",
    "XLV": "Healthcare",
    "XLI": "Industrials",
    "XLK": "Technology",
    "XLRE": "Real Estate",
    "XLU": "Utilities",
    "XLB": "Materials"
}

# Add S&P 500 ETF for comparison
benchmark = "SPY"

# --- Options Activity Analysis (Polygon.io) ---
def extract_call_put_counts(option_data):
    # This function expects the Polygon options contracts API response for a ticker
    # and returns (call_count, put_count)
    if not option_data or 'results' not in option_data:
        return 0, 0
    call_count = 0
    put_count = 0
    for contract in option_data['results']:
        if contract.get('type') == 'call':
            call_count += 1
        elif contract.get('type') == 'put':
            put_count += 1
    return call_count, put_count


import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))
import pandas as pd
import matplotlib.pyplot as plt
from stock_analizer.app.services.sp500 import get_sp500_tickers, get_sector_map
from stock_analizer.app.services.polygon import fetch_polygon_close_async
import asyncio
import datetime
import aiohttp
try:
    from app.services.polygon import get_polygon_data
    from app.services.polygon_options import get_sector_options_activity
except ModuleNotFoundError:
    from stock_analizer.app.services.polygon import get_polygon_data
    from stock_analizer.app.services.polygon_options import get_sector_options_activity


async def fetch_all_polygon_data(tickers, start_date, end_date):
    async with aiohttp.ClientSession() as session:
        dfs = {}
        for ticker in tickers:
            df = await get_polygon_data(ticker, session)
            if df is not None:
                # Filter to last 3 months
                df = df[(df.index >= pd.Timestamp(start_date)) & (df.index <= pd.Timestamp(end_date))]
                dfs[ticker] = df
        return dfs

async def main():
    tickers = list(sector_etfs.keys()) + [benchmark]
    end_date = datetime.date.today()
    start_date = end_date - datetime.timedelta(days=90)
    dfs = await fetch_all_polygon_data(tickers, start_date, end_date)
    # Build close and volume DataFrames
    close = pd.DataFrame({k: v['close'] for k, v in dfs.items() if 'close' in v})
    volume = pd.DataFrame({k: v['volume'] for k, v in dfs.items() if 'volume' in v})
    
    # --- Sector Breadth & Volume Analysis ---
    sector_breadth = await get_sector_breadth()
    # Calculate total volume for each sector (sum of all S&P 500 stocks in that sector, latest day)
    sector_map = await get_sector_map()
    # Fetch latest close and volume for all S&P 500 tickers
    all_sp500_tickers = [ticker for tickers in sector_map.values() for ticker in tickers]
    # Use the same async batch fetcher for volume (reuse fetch_polygon_close_async, but get last close and volume)
    # We'll fetch the last row for each ticker's DataFrame
    import aiohttp
    from stock_analizer.app.services.polygon import get_polygon_data
    async def fetch_latest_volume(ticker):
        async with aiohttp.ClientSession() as session:
            df = await get_polygon_data(ticker, session)
            if df is not None and not df.empty:
                return ticker, df['volume'].iloc[-1]
            return ticker, 0
    # Batch fetch volumes
    sector_volumes = {sector: 0 for sector in sector_map}
    batch_size = 10
    for sector, tickers in sector_map.items():
        vols = []
        for i in range(0, len(tickers), batch_size):
            batch = tickers[i:i+batch_size]
            tasks = [fetch_latest_volume(t) for t in batch]
            results = await asyncio.gather(*tasks)
            vols.extend([v for _, v in results])
        sector_volumes[sector] = sum(vols)
    print("\n📊 Sector Breadth & Volume Analysis:")
    sector_stats = []
    for sector, data in sector_breadth.items():
        vol = sector_volumes.get(sector, 0)
        adv = data['advancing']
        dec = data['declining']
        total = data['total']
        adv_pct = adv / total if total else 0
        sector_stats.append({
            'sector': sector,
            'advancing': adv,
            'declining': dec,
            'total': total,
            'adv_pct': adv_pct,
            'volume': vol
        })
        print(f"{sector}: Advancing={adv}, Declining={dec}, Unchanged={data['unchanged']}, Total={total}, Volume={vol:,.0f}")

    # --- Automated Interpretation ---
    # Rank by breadth (advancing %) and volume (relative to median)
    import numpy as np
    median_vol = np.median([s['volume'] for s in sector_stats])
    # Score: high breadth (>60%) and above-median volume
    leaders = [s for s in sector_stats if s['adv_pct'] > 0.6 and s['volume'] > median_vol]
    # Also show strong breadth but lower volume
    strong_breadth = [s for s in sector_stats if s['adv_pct'] > 0.6 and s['volume'] <= median_vol]
    # Weak sectors: more decliners than advancers
    laggards = [s for s in sector_stats if s['advancing'] < s['declining']]

    print("\n🔎 Summary Interpretation:")
    if leaders:
        print("Money is clearly moving into the following sectors (strong breadth and high volume):")
        for s in sorted(leaders, key=lambda x: (-x['adv_pct'], -x['volume'])):
            print(f"  - {s['sector']} (Advancing: {s['advancing']}/{s['total']} | Volume: {s['volume']:,})")
    else:
        print("No sector shows both strong breadth and high volume today.")
    if strong_breadth:
        print("\nOther sectors with strong breadth but lower volume:")
        for s in sorted(strong_breadth, key=lambda x: -x['adv_pct']):
            print(f"  - {s['sector']} (Advancing: {s['advancing']}/{s['total']} | Volume: {s['volume']:,})")
    if laggards:
        print("\nSectors showing weakness (more decliners than advancers):")
        for s in laggards:
            print(f"  - {s['sector']} (Advancing: {s['advancing']}, Declining: {s['declining']})")

    # --- Actionable Insight ---
    print("\n💡 Actionable Insight:")
    if leaders:
        print("Focus on the leading sectors above for long opportunities, as these are where institutional money is most active right now.")
    elif strong_breadth:
        print("Watch sectors with strong breadth for potential breakouts if volume increases.")
    else:
        print("No clear sector leadership today. Consider waiting for stronger signals or look for rotation.")

    # Normalize prices to start at 100 for easy comparison
async def get_sector_breadth(date=None):
    """
    Calculate sector breadth (advance/decline) for each sector in the S&P 500.
    Returns a dict: {sector: {'advancing': int, 'declining': int, 'unchanged': int, 'total': int}}
    """
    sector_map = await get_sector_map()
    all_tickers = [ticker for tickers in sector_map.values() for ticker in tickers]
    # Fetch previous close and current/target close for all tickers
    # If date is None, use last close; else use close for that date and previous trading day
    # fetch_polygon_close_async returns {ticker: close}
    if date is None:
        closes = await fetch_polygon_close_async(all_tickers, days=2)  # {ticker: [prev_close, last_close]}
    else:
        closes = await fetch_polygon_close_async(all_tickers, days=2, end_date=date)
    # Calculate advance/decline/unchanged for each sector
    sector_breadth = {}
    for sector, tickers in sector_map.items():
        adv = dec = unch = 0
        for ticker in tickers:
            vals = closes.get(ticker)
            if not vals or len(vals) < 2:
                continue
            prev, curr = vals[-2], vals[-1]
            if pd.isna(prev) or pd.isna(curr):
                continue
            if curr > prev:
                adv += 1
            elif curr < prev:
                dec += 1
            else:
                unch += 1
        sector_breadth[sector] = {
            'advancing': adv,
            'declining': dec,
            'unchanged': unch,
            'total': adv + dec + unch
        }
    return sector_breadth
    normalized = close / close.iloc[0] * 100

    # Compute relative strength vs S&P 500
    relative_strength = normalized.div(normalized[benchmark], axis=0)

    # Find last value (most recent RS)
    latest_rs = relative_strength.iloc[-1].sort_values(ascending=False)

    print("\n📊 Sector Rotation Ranking (Relative Strength vs SPY):")
    for etf, score in latest_rs.items():
        if etf != benchmark:
            print(f"{sector_etfs.get(etf, etf):<20} ({etf}): {score:.2f}")

    # --- Volume Analysis ---
    volume_avg = volume.rolling(window=20).mean()  # 20-day rolling average
    latest_volume = volume.iloc[-1]
    latest_volume_avg = volume_avg.iloc[-1]
    volume_surge = (latest_volume / latest_volume_avg * 100).sort_values(ascending=False)

    print("\n🔊 Sector Volume Surge (Latest vs 20-day Avg, %):")
    for etf, surge in volume_surge.items():
        if etf != benchmark:
            print(f"{sector_etfs.get(etf, etf):<20} ({etf}): {surge:.1f}%")

    # Optional: Plot rolling average volume for each sector
    plt.figure(figsize=(12,6))
    for etf in sector_etfs.keys():
        plt.plot(volume_avg.index, volume_avg[etf], label=etf)
    plt.title("Sector 20-Day Rolling Average Volume")
    plt.legend()
    plt.show()

    # Plot relative strength
    plt.figure(figsize=(12,6))
    for etf in sector_etfs.keys():
        plt.plot(relative_strength.index, relative_strength[etf], label=etf)

    plt.title("Sector Relative Strength vs SPY")
    plt.legend()
    plt.show()


    # --- Options Activity Analysis (Polygon.io) ---
    await analyze_options_activity()

# --- Options Activity Analysis (Polygon.io) ---
async def analyze_options_activity():
    print("\n🟢 Options Activity (Call/Put Ratio, Polygon.io):")
    tickers = list(sector_etfs.keys())
    today = datetime.date.today().isoformat()
    options_data = await get_sector_options_activity(tickers, today)
    ratios = {}
    for etf, data in options_data.items():
        call_count, put_count = extract_call_put_counts(data)
        total = call_count + put_count
        ratio = (call_count / put_count) if put_count > 0 else float('inf')
        ratios[etf] = ratio
        print(f"{sector_etfs.get(etf, etf):<20} ({etf}): Calls={call_count}, Puts={put_count}, Call/Put Ratio={ratio:.2f}")
    # Optionally, print the most call-heavy and put-heavy sectors
    sorted_ratios = sorted(ratios.items(), key=lambda x: x[1], reverse=True)
    print("\nTop Sectors by Call/Put Ratio:")
    for etf, ratio in sorted_ratios:
        print(f"{sector_etfs.get(etf, etf):<20} ({etf}): {ratio:.2f}")

    tickers = list(sector_etfs.keys())
    today = datetime.date.today().isoformat()
    options_data = await get_sector_options_activity(tickers, today)
    ratios = {}
    for etf, data in options_data.items():
        call_count, put_count = extract_call_put_counts(data)
        total = call_count + put_count
        ratio = (call_count / put_count) if put_count > 0 else float('inf')
        ratios[etf] = ratio
        print(f"{sector_etfs.get(etf, etf):<20} ({etf}): Calls={call_count}, Puts={put_count}, Call/Put Ratio={ratio:.2f}")
    # Optionally, print the most call-heavy and put-heavy sectors
    sorted_ratios = sorted(ratios.items(), key=lambda x: x[1], reverse=True)
    print("\nTop Sectors by Call/Put Ratio:")
    for etf, ratio in sorted_ratios:
        print(f"{sector_etfs.get(etf, etf):<20} ({etf}): {ratio:.2f}")


# Run the async main at the end of the script
if __name__ == "__main__":
    asyncio.run(main())
