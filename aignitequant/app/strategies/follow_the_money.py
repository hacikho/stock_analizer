
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
import numpy as np
import json
from aignitequant.app.services.sp500 import get_sp500_tickers, get_sector_map
from aignitequant.app.services.polygon import fetch_polygon_close_async
import asyncio
import datetime
import aiohttp
from aignitequant.app.services.market_data import get_dataframe_from_db, get_multiple_dataframes_from_db
try:
    from aignitequant.app.services.polygon import get_polygon_data
    from aignitequant.app.services.polygon_options import get_sector_options_activity
except ModuleNotFoundError:
    from aignitequant.app.services.polygon import get_polygon_data
    from aignitequant.app.services.polygon_options import get_sector_options_activity


async def fetch_all_polygon_data(tickers, start_date, end_date):
    """Fetch OHLCV data for tickers — DB first, API fallback."""
    import time
    t0 = time.time()
    dfs_raw = get_multiple_dataframes_from_db(tickers)
    dfs = {}
    api_fallback = []
    for ticker in tickers:
        df = dfs_raw.get(ticker)
        if df is not None and not df.empty:
            df = df[(df.index >= pd.Timestamp(start_date)) & (df.index <= pd.Timestamp(end_date))]
            if not df.empty:
                dfs[ticker] = df
                continue
        api_fallback.append(ticker)
    
    db_hits = len(dfs)
    print(f"DB batch read (sector ETFs): {db_hits}/{len(tickers)} in {time.time()-t0:.2f}s")
    
    # API fallback
    if api_fallback:
        async with aiohttp.ClientSession() as session:
            for ticker in api_fallback:
                df = await get_polygon_data(ticker, session)
                if df is not None:
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
    # Batch read all S&P 500 tickers from DB
    import time as _time
    t_vol = _time.time()
    all_sp500_dfs = get_multiple_dataframes_from_db(all_sp500_tickers)
    db_vol_hits = sum(1 for v in all_sp500_dfs.values() if v is not None and not v.empty)
    print(f"📂 DB batch read (all S&P 500): {db_vol_hits}/{len(all_sp500_tickers)} in {_time.time()-t_vol:.2f}s")

    def fetch_latest_volume_from_df(ticker):
        df = all_sp500_dfs.get(ticker)
        if df is not None and not df.empty:
            return ticker, df['volume'].iloc[-1]
        return ticker, 0

    # Compute volumes from DB
    sector_volumes = {sector: 0 for sector in sector_map}
    for sector, tickers in sector_map.items():
        vols = []
        for t in tickers:
            _, vol = fetch_latest_volume_from_df(t)
            vols.append(vol)
        sector_volumes[sector] = sum(vols)
    # Get individual stock performance for each sector
    print("\n🔍 Computing stock performance for sector leaders...")
    sector_stock_leaders = {}

    def fetch_stock_performance_from_df(ticker):
        """Get latest price change from pre-loaded DB data"""
        try:
            df = all_sp500_dfs.get(ticker)
            if df is not None and len(df) >= 2:
                latest_close = df.iloc[-1]['close']
                previous_close = df.iloc[-2]['close']
                price_change_pct = ((latest_close - previous_close) / previous_close) * 100
                volume = df.iloc[-1].get('volume', 0)
                return {
                    'symbol': ticker,
                    'price_change_pct': price_change_pct,
                    'volume': volume,
                    'price': latest_close
                }
        except Exception as e:
            print(f"⚠️ Error processing {ticker}: {e}")
        return None

    # Process all stocks in each sector from DB data
    for sector, tickers in sector_map.items():
        print(f"  📊 Processing {sector} ({len(tickers)} stocks)...")

        all_stocks = []
        for ticker in tickers:
            result = fetch_stock_performance_from_df(ticker)
            if isinstance(result, dict) and result is not None:
                all_stocks.append(result)
        
        # Separate gainers and decliners
        gainers = [stock for stock in all_stocks if stock['price_change_pct'] > 0]
        decliners = [stock for stock in all_stocks if stock['price_change_pct'] < 0]
        
        # Sort by performance
        gainers.sort(key=lambda x: x['price_change_pct'], reverse=True)
        decliners.sort(key=lambda x: x['price_change_pct'])
        
        sector_stock_leaders[sector] = {
            'top_gainers': gainers[:5],
            'top_decliners': decliners[:5]
        }

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
        
        # Show top 5 gainers and decliners for this sector
        if sector in sector_stock_leaders:
            gainers = sector_stock_leaders[sector]['top_gainers']
            decliners = sector_stock_leaders[sector]['top_decliners']
            
            if gainers:
                gainer_names = [f"{stock['symbol']} (+{stock['price_change_pct']:.1f}%)" for stock in gainers]
                print(f"  📈 Top Gainers: {', '.join(gainer_names)}")
            
            if decliners:
                decliner_names = [f"{stock['symbol']} ({stock['price_change_pct']:.1f}%)" for stock in decliners]
                print(f"  📉 Top Decliners: {', '.join(decliner_names)}")
            print()  # Add spacing between sectors

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

    # --- COMPREHENSIVE SECTOR RANKING SYSTEM ---
    print("\n🏆 COMPREHENSIVE SECTOR RANKING")
    print("=" * 80)
    
    # Calculate composite scores for ranking
    sector_rankings = []
    
    for stats in sector_stats:
        sector = stats['sector']
        
        # 1. Breadth Score (0-25 points): % advancing stocks
        breadth_score = min(stats['adv_pct'] * 25, 25)
        
        # 2. Volume Score (0-25 points): relative to median volume
        volume_ratio = stats['volume'] / median_vol if median_vol > 0 else 1
        volume_score = min(np.log(volume_ratio + 1) / np.log(3) * 25, 25)  # Log scale, cap at 25
        
        # 3. Market Leadership Score (0-25 points): combination of breadth strength and volume
        leadership_score = 0
        if stats['adv_pct'] > 0.7:  # Very strong breadth
            leadership_score += 15
        elif stats['adv_pct'] > 0.6:  # Strong breadth
            leadership_score += 10
        elif stats['adv_pct'] > 0.5:  # Moderate breadth
            leadership_score += 5
        
        if volume_ratio > 2:  # High volume
            leadership_score += 10
        elif volume_ratio > 1.5:  # Above average volume
            leadership_score += 5
        
        # 4. Consistency Score (0-25 points): based on advance/decline ratio consistency
        if stats['total'] > 0:
            consistency_ratio = abs(stats['advancing'] - stats['declining']) / stats['total']
            consistency_score = min(consistency_ratio * 25, 25)
        else:
            consistency_score = 0
        
        # Total Composite Score (0-100)
        total_score = breadth_score + volume_score + leadership_score + consistency_score
        
        sector_rankings.append({
            'sector': sector,
            'total_score': total_score,
            'breadth_score': breadth_score,
            'volume_score': volume_score,
            'leadership_score': leadership_score,
            'consistency_score': consistency_score,
            'advancing': stats['advancing'],
            'declining': stats['declining'],
            'total_stocks': stats['total'],
            'breadth_pct': stats['adv_pct'],
            'volume': stats['volume'],
            'volume_ratio': volume_ratio
        })
    
    # Sort by total score (highest first)
    sector_rankings.sort(key=lambda x: x['total_score'], reverse=True)
    
    # Display rankings
    print(f"{'Rank':<4} {'Sector':<25} {'Score':<6} {'Breadth':<8} {'Volume':<8} {'Leadership':<10} {'Consistency':<10}")
    print("-" * 80)
    
    for i, ranking in enumerate(sector_rankings, 1):
        print(f"{i:<4} {ranking['sector']:<25} {ranking['total_score']:<6.1f} "
              f"{ranking['breadth_score']:<8.1f} {ranking['volume_score']:<8.1f} "
              f"{ranking['leadership_score']:<10.1f} {ranking['consistency_score']:<10.1f}")
    
    # Sector Classification
    print(f"\n📊 SECTOR CLASSIFICATION:")
    
    # Strong sectors (score >= 70)
    strong_sectors = [r for r in sector_rankings if r['total_score'] >= 70]
    if strong_sectors:
        print(f"\n💪 STRONG SECTORS (Score ≥70):")
        for sector in strong_sectors:
            print(f"  🟢 {sector['sector']}: {sector['total_score']:.1f} points")
            print(f"     - Breadth: {sector['advancing']}/{sector['total_stocks']} ({sector['breadth_pct']:.1%})")
            print(f"     - Volume: {sector['volume']:,} ({sector['volume_ratio']:.1f}x median)")
            
            # Show top stock performers
            if sector['sector'] in sector_stock_leaders:
                gainers = sector_stock_leaders[sector['sector']]['top_gainers']
                if gainers:
                    gainer_names = [f"{stock['symbol']} (+{stock['price_change_pct']:.1f}%)" for stock in gainers[:3]]
                    print(f"     - Top Gainers: {', '.join(gainer_names)}")
    
    # Moderate sectors (score 50-69)
    moderate_sectors = [r for r in sector_rankings if 50 <= r['total_score'] < 70]
    if moderate_sectors:
        print(f"\n🔶 MODERATE SECTORS (Score 50-69):")
        for sector in moderate_sectors:
            print(f"  🟡 {sector['sector']}: {sector['total_score']:.1f} points")
            
            # Show top stock performers
            if sector['sector'] in sector_stock_leaders:
                gainers = sector_stock_leaders[sector['sector']]['top_gainers']
                if gainers:
                    gainer_names = [f"{stock['symbol']} (+{stock['price_change_pct']:.1f}%)" for stock in gainers[:3]]
                    print(f"     - Top Gainers: {', '.join(gainer_names)}")
    
    # Weak sectors (score < 50)
    weak_sectors = [r for r in sector_rankings if r['total_score'] < 50]
    if weak_sectors:
        print(f"\n📉 WEAK SECTORS (Score <50):")
        for sector in weak_sectors:
            print(f"  🔴 {sector['sector']}: {sector['total_score']:.1f} points")
            
            # Show top decliners for context
            if sector['sector'] in sector_stock_leaders:
                decliners = sector_stock_leaders[sector['sector']]['top_decliners']
                if decliners:
                    decliner_names = [f"{stock['symbol']} ({stock['price_change_pct']:.1f}%)" for stock in decliners[:3]]
                    print(f"     - Top Decliners: {', '.join(decliner_names)}")
    
    # Trading Recommendations
    print(f"\n🎯 TRADING RECOMMENDATIONS:")
    if strong_sectors:
        print(f"\n✅ BULLISH PLAYS:")
        for i, sector in enumerate(strong_sectors[:3], 1):  # Top 3
            print(f"  {i}. {sector['sector']} - Strong institutional activity detected")
        
        print(f"\n📈 SECTOR ROTATION OPPORTUNITIES:")
        if len(strong_sectors) >= 2:
            print(f"  • Rotate FROM weak sectors TO: {', '.join([s['sector'] for s in strong_sectors[:2]])}")
        
        if weak_sectors:
            print(f"\n⚠️  SECTORS TO AVOID:")
            for sector in weak_sectors[-2:]:  # Bottom 2
                print(f"  • {sector['sector']} - Weak breadth and institutional interest")
    
    else:
        print("  🔄 MIXED MARKET - Consider defensive positioning or wait for clearer sector leadership")
    
    # --- COMPREHENSIVE MARKET INSIGHTS & TRADING IDEAS ---
    print(f"\n🎯 KEY MARKET INSIGHTS & ACTIONABLE INTELLIGENCE:")
    print("=" * 80)
    
    # Analyze cross-sector themes
    all_gainers = []
    all_decliners = []
    
    for sector_name, leaders in sector_stock_leaders.items():
        if leaders['top_gainers']:
            for stock in leaders['top_gainers'][:3]:  # Top 3 from each sector
                all_gainers.append((stock, sector_name))
        if leaders['top_decliners']:
            for stock in leaders['top_decliners'][:3]:  # Top 3 decliners from each sector
                all_decliners.append((stock, sector_name))
    
    # Sort by performance
    all_gainers.sort(key=lambda x: x[0]['price_change_pct'], reverse=True)
    all_decliners.sort(key=lambda x: x[0]['price_change_pct'])
    
    # Market themes analysis
    print(f"\n💡 MARKET THEMES & SECTOR ROTATION:")
    
    # Leading sectors analysis
    print(f"\n📈 STRONGEST SECTORS TODAY:")
    for i, ranking in enumerate(sector_rankings[:3], 1):
        sector = ranking['sector']
        print(f"  {i}. {sector} (Score: {ranking['total_score']:.1f})")
        if sector in sector_stock_leaders and sector_stock_leaders[sector]['top_gainers']:
            top_stocks = sector_stock_leaders[sector]['top_gainers'][:3]
            stock_names = [f"{s['symbol']} (+{s['price_change_pct']:.1f}%)" for s in top_stocks]
            print(f"     Leaders: {', '.join(stock_names)}")
    
    print(f"\n📉 WEAKEST SECTORS TODAY:")
    for i, ranking in enumerate(sector_rankings[-3:], 1):
        sector = ranking['sector']
        print(f"  {i}. {sector} (Score: {ranking['total_score']:.1f})")
        if sector in sector_stock_leaders and sector_stock_leaders[sector]['top_decliners']:
            worst_stocks = sector_stock_leaders[sector]['top_decliners'][:3]
            stock_names = [f"{s['symbol']} ({s['price_change_pct']:.1f}%)" for s in worst_stocks]
            print(f"     Laggards: {', '.join(stock_names)}")
    
    # Cross-sector stock analysis
    print(f"\n🏆 TOP INDIVIDUAL STOCK PERFORMERS (All Sectors):")
    for i, (stock, sector) in enumerate(all_gainers[:10], 1):
        print(f"  {i:2d}. {stock['symbol']:5s} +{stock['price_change_pct']:5.1f}% ({sector})")
    
    print(f"\n📊 WORST INDIVIDUAL STOCK PERFORMERS (All Sectors):")
    for i, (stock, sector) in enumerate(all_decliners[:10], 1):
        print(f"  {i:2d}. {stock['symbol']:5s} {stock['price_change_pct']:6.1f}% ({sector})")
    
    # Thematic analysis
    print(f"\n🔍 THEMATIC ANALYSIS:")
    
    # Defensive vs Growth rotation analysis
    defensive_sectors = ['Consumer Staples', 'Utilities', 'Health Care']
    growth_sectors = ['Information Technology', 'Communication Services', 'Consumer Discretionary']
    
    defensive_scores = [r['total_score'] for r in sector_rankings if r['sector'] in defensive_sectors]
    growth_scores = [r['total_score'] for r in sector_rankings if r['sector'] in growth_sectors]
    
    avg_defensive = sum(defensive_scores) / len(defensive_scores) if defensive_scores else 0
    avg_growth = sum(growth_scores) / len(growth_scores) if growth_scores else 0
    
    if avg_defensive > avg_growth:
        print(f"  🛡️  DEFENSIVE ROTATION: Defensive sectors (avg: {avg_defensive:.1f}) outperforming Growth (avg: {avg_growth:.1f})")
        print(f"      → Risk-off sentiment, investors seeking safety")
    else:
        print(f"  🚀 GROWTH LEADERSHIP: Growth sectors (avg: {avg_growth:.1f}) outperforming Defensive (avg: {avg_defensive:.1f})")
        print(f"      → Risk-on sentiment, growth optimism")
    
    # Generate specific trading ideas
    print(f"\n💰 SPECIFIC TRADING IDEAS:")
    
    # Bullish ideas (top gainers from strong sectors)
    bullish_ideas = []
    for sector_ranking in sector_rankings[:3]:  # Top 3 sectors
        sector = sector_ranking['sector']
        if sector in sector_stock_leaders and sector_stock_leaders[sector]['top_gainers']:
            for stock in sector_stock_leaders[sector]['top_gainers'][:2]:  # Top 2 from each
                if stock['price_change_pct'] > 1.0:  # Only strong gainers
                    bullish_ideas.append((stock, sector))
    
    if bullish_ideas:
        print(f"\n  📈 BULLISH OPPORTUNITIES:")
        for stock, sector in bullish_ideas[:6]:  # Top 6 ideas
            print(f"    • {stock['symbol']} (+{stock['price_change_pct']:.1f}%) - {sector} sector leader")
    
    # Bearish ideas (worst performers from weak sectors)
    bearish_ideas = []
    for sector_ranking in sector_rankings[-3:]:  # Bottom 3 sectors
        sector = sector_ranking['sector']
        if sector in sector_stock_leaders and sector_stock_leaders[sector]['top_decliners']:
            for stock in sector_stock_leaders[sector]['top_decliners'][:2]:  # Worst 2 from each
                if stock['price_change_pct'] < -1.0:  # Only significant decliners
                    bearish_ideas.append((stock, sector))
    
    if bearish_ideas:
        print(f"\n  📉 BEARISH/AVOID OPPORTUNITIES:")
        for stock, sector in bearish_ideas[:6]:  # Top 6 ideas
            print(f"    • {stock['symbol']} ({stock['price_change_pct']:.1f}%) - {sector} sector weakness")
    
    # Sector pair trades
    print(f"\n🔄 SECTOR PAIR TRADE IDEAS:")
    if len(sector_rankings) >= 2:
        strongest = sector_rankings[0]
        weakest = sector_rankings[-1]
        print(f"  • LONG {strongest['sector']} / SHORT {weakest['sector']}")
        print(f"    Rationale: {strongest['total_score']:.1f} vs {weakest['total_score']:.1f} score differential")
        
        # Suggest specific stocks for the pair trade
        if (strongest['sector'] in sector_stock_leaders and 
            weakest['sector'] in sector_stock_leaders):
            
            strong_stock = sector_stock_leaders[strongest['sector']]['top_gainers']
            weak_stock = sector_stock_leaders[weakest['sector']]['top_decliners']
            
            if strong_stock and weak_stock:
                print(f"    Example: LONG {strong_stock[0]['symbol']} / SHORT {weak_stock[0]['symbol']}")
    
    # Market regime assessment
    print(f"\n🌡️  MARKET REGIME ASSESSMENT:")
    
    # Count advancing vs declining sectors
    advancing_sectors = len([r for r in sector_rankings if r['breadth_pct'] > 0.5])
    total_sectors = len(sector_rankings)
    market_breadth = advancing_sectors / total_sectors
    
    if market_breadth > 0.6:
        regime = "🟢 BROAD BULL MARKET"
        advice = "High conviction long positions, sector rotation opportunities"
    elif market_breadth < 0.4:
        regime = "🔴 BROAD BEAR MARKET" 
        advice = "Defensive positioning, short opportunities, cash preservation"
    else:
        regime = "🟡 MIXED/CHURNING MARKET"
        advice = "Stock picking environment, focus on relative strength"
    
    print(f"  Regime: {regime}")
    print(f"  Breadth: {advancing_sectors}/{total_sectors} sectors advancing ({market_breadth:.1%})")
    print(f"  Strategy: {advice}")
    
    # Save detailed ranking to JSON
    ranking_report = {
        'timestamp': datetime.datetime.now().isoformat(),
        'analysis_type': 'comprehensive_sector_ranking',
        'market_summary': {
            'total_sectors_analyzed': len(sector_rankings),
            'strong_sectors_count': len(strong_sectors),
            'moderate_sectors_count': len(moderate_sectors),
            'weak_sectors_count': len(weak_sectors),
            'median_volume': float(median_vol)
        },
        'sector_rankings': [
            {
                **ranking,
                'rank': i + 1,
                'classification': ('Strong' if ranking['total_score'] >= 70 
                                 else 'Moderate' if ranking['total_score'] >= 50 
                                 else 'Weak'),
                'top_gainers': sector_stock_leaders.get(ranking['sector'], {}).get('top_gainers', []),
                'top_decliners': sector_stock_leaders.get(ranking['sector'], {}).get('top_decliners', [])
            }
            for i, ranking in enumerate(sector_rankings)
        ],
        'individual_stock_leaders': sector_stock_leaders,
        'trading_recommendations': {
            'bullish_sectors': [s['sector'] for s in strong_sectors[:3]],
            'sectors_to_avoid': [s['sector'] for s in weak_sectors[-2:]],
            'rotation_opportunity': len(strong_sectors) >= 2 and len(weak_sectors) >= 1,
            'top_stocks_to_watch': {
                sector_name: [stock['symbol'] for stock in leaders['top_gainers'][:3]]
                for sector_name, leaders in sector_stock_leaders.items()
                if leaders['top_gainers']
            }
        }
    }
    
    # Save reports
    import os
    os.makedirs('reports', exist_ok=True)
    
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f'reports/sector_ranking_{timestamp}.json'
    
    with open(filename, 'w') as f:
        json.dump(ranking_report, f, indent=2, default=str)
    
    with open('reports/latest_sector_ranking.json', 'w') as f:
        json.dump(ranking_report, f, indent=2, default=str)
    
    print(f"\n💾 Detailed ranking saved to: {filename}")
    print(f"💾 Latest ranking saved to: reports/latest_sector_ranking.json")

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
    
    # --- Save Results to JSON ---
    await save_analysis_results(relative_strength, volume_surge, sector_breadth, sector_volumes, sector_stock_leaders)

async def save_analysis_results(relative_strength, volume_surge, sector_breadth, sector_volumes, sector_stock_leaders):
    """Save follow-the-money analysis results to JSON file"""
    import os
    
    # Ensure reports directory exists
    os.makedirs("reports", exist_ok=True)
    
    # Get latest values
    latest_rs = relative_strength.iloc[-1].to_dict()
    latest_volume_surge = volume_surge.to_dict()
    
    # Prepare data for JSON serialization
    analysis_data = {
        "timestamp": datetime.datetime.now().isoformat(),
        "date": datetime.date.today().isoformat(),
        "sector_relative_strength": {
            etf: round(float(score), 2) 
            for etf, score in latest_rs.items() 
            if etf != benchmark
        },
        "sector_volume_surge": {
            etf: round(float(surge), 2) 
            for etf, surge in latest_volume_surge.items() 
            if etf != benchmark
        },
        "sector_breadth": {
            sector: {
                "advancing": int(data["advancing"]),
                "declining": int(data["declining"]),
                "breadth_ratio": round(float(data["breadth_ratio"]), 2)
            }
            for sector, data in sector_breadth.items()
        },
        "sector_total_volume": {
            sector: int(vol) 
            for sector, vol in sector_volumes.items()
        },
        "sector_stock_leaders": {
            sector: {
                "top_gainers": [
                    {
                        "symbol": stock["symbol"],
                        "price_change_pct": round(float(stock["price_change_pct"]), 2),
                        "price": round(float(stock["price"]), 2),
                        "volume": int(stock["volume"])
                    }
                    for stock in leaders.get("top_gainers", [])[:5]
                ],
                "top_decliners": [
                    {
                        "symbol": stock["symbol"],
                        "price_change_pct": round(float(stock["price_change_pct"]), 2),
                        "price": round(float(stock["price"]), 2),
                        "volume": int(stock["volume"])
                    }
                    for stock in leaders.get("top_decliners", [])[:5]
                ]
            }
            for sector, leaders in sector_stock_leaders.items()
        },
        "sector_etf_mapping": sector_etfs
    }
    
    # Save to latest file
    latest_file = "reports/latest_sector_analysis.json"
    with open(latest_file, 'w') as f:
        json.dump(analysis_data, f, indent=2)
    
    # Also save timestamped version
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    timestamped_file = f"reports/sector_analysis_{timestamp}.json"
    with open(timestamped_file, 'w') as f:
        json.dump(analysis_data, f, indent=2)
    
    print(f"\n💾 Analysis saved to: {latest_file}")
    print(f"💾 Timestamped backup: {timestamped_file}")
    
    return analysis_data

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
