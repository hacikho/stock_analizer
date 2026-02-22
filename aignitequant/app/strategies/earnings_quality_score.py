"""
Earnings Quality Score Calculator
---------------------------------
Analyzes S&P 500 stocks with recent earnings (last 5 days) and calculates
an Earnings Quality Score to determine optimal buy timing after earnings.

The score considers multiple factors:
1. Earnings Beat/Miss (Revenue & EPS)
2. Guidance Updates (Forward PE changes)
3. Post-Earnings Price Action
4. Volume Analysis
5. Analyst Sentiment Changes
6. Financial Health Metrics

Score Range: 0-100
- 80-100: BUY IMMEDIATELY (High quality earnings)
- 60-79:  BUY IN 1-2 DAYS (Good earnings, minor concerns)
- 40-59:  WAIT 3-5 DAYS (Mixed signals, let dust settle)
- 20-39:  WAIT 1-2 WEEKS (Concerning signals)
- 0-19:   AVOID (Poor earnings quality)
"""

import pandas as pd
import numpy as np
import requests
import json
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import asyncio
import aiohttp
import warnings
import os
from dotenv import load_dotenv
from aignitequant.app.db import SessionLocal, EarningsQualityData
from sqlalchemy import and_

# Import your existing Polygon services
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from app.services.polygon import get_polygon_data
from aignitequant.app.services.market_data import get_dataframe_from_db

warnings.filterwarnings('ignore')
load_dotenv()
API_KEY = os.getenv("API_KEY")
print(f"✅ Polygon API key loaded: {API_KEY is not None}")

def is_trading_day(date):
    """Check if a date is a trading day (not weekend)"""
    # 0-4 = Monday-Friday, 5-6 = Saturday-Sunday
    return date.weekday() < 5

def get_last_n_trading_days(n=3):
    """Get the last N trading days (excluding weekends)"""
    trading_days = []
    current_date = datetime.now()
    days_back = 0
    
    while len(trading_days) < n:
        check_date = current_date - timedelta(days=days_back)
        if is_trading_day(check_date):
            trading_days.append(check_date)
        days_back += 1
        
        # Safety limit to prevent infinite loop
        if days_back > n * 3:
            break
    
    return trading_days

def get_earnings_tickers(date):
    """Get earnings tickers directly from Yahoo Finance earnings calendar with better headers"""
    import time
    import random
    import requests
    from urllib.parse import urlencode
    
    try:
        # Add a longer delay to be more respectful
        time.sleep(random.uniform(1, 3))
        
        date_str = date.strftime("%Y-%m-%d")
        url = f"https://finance.yahoo.com/calendar/earnings?day={date_str}"
        
        # Use requests with proper headers to avoid being blocked
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
        
        session = requests.Session()
        session.headers.update(headers)
        
        max_retries = 2
        for attempt in range(max_retries):
            try:
                if attempt > 0:
                    delay = 10 + random.uniform(5, 10)  # Longer delays
                    print(f"⏳ Waiting {delay:.1f} seconds before retry {attempt + 1}/{max_retries}...")
                    time.sleep(delay)
                
                response = session.get(url, timeout=30)
                
                if response.status_code == 200:
                    # Use pandas to read HTML from the response content
                    tables = pd.read_html(response.content)
                    if tables and len(tables) > 0:
                        df = tables[0]
                        if "Symbol" in df.columns:
                            tickers = df["Symbol"].dropna().tolist()
                            # Clean up tickers
                            tickers = [ticker.strip() for ticker in tickers if ticker and len(ticker.strip()) <= 5]
                            print(f"✅ Found {len(tickers)} earnings for {date_str}")
                            return tickers
                        else:
                            print(f"No Symbol column found in earnings table for {date_str}")
                            return []
                    else:
                        print(f"No earnings table found for {date_str}")
                        return []
                
                elif response.status_code == 429:
                    print(f"🚫 Rate limit hit (attempt {attempt + 1}/{max_retries})")
                    if attempt == max_retries - 1:
                        print(f"❌ Max retries exceeded for {date_str}")
                        return []
                    continue
                else:
                    print(f"HTTP {response.status_code} error for {date_str}")
                    return []
            
            except Exception as e:
                print(f"Error getting earnings tickers for {date} (attempt {attempt + 1}): {e}")
                if attempt == max_retries - 1:
                    return []
                continue
        
        return []
        
    except Exception as e:
        print(f"Error getting earnings tickers for {date}: {e}")
        return []

class EarningsQualityAnalyzer:
    def __init__(self):
        pass  # No need to load S&P 500 since we get tickers directly from Yahoo Finance
    
    async def _calculate_earnings_beat_score(self, ticker: str, session: aiohttp.ClientSession) -> Tuple[float, dict]:
        """Calculate score based on earnings beat/miss using Polygon financials (0-25 points)"""
        try:
            earnings_data = {}
            score = 12.5  # Base score
            
            # Get latest financial data from Polygon
            url = f"https://api.polygon.io/vX/reference/financials"
            params = {
                'ticker': ticker,
                'limit': 4,  # Last 4 quarters
                'order': 'desc',
                'sort': 'filing_date',
                'apikey': API_KEY
            }
            
            async with session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get('results') and len(data['results']) >= 2:
                        results = data['results']
                        latest = results[0]
                        previous = results[1]
                        
                        # Calculate revenue growth (quarter over quarter)
                        latest_rev = latest.get('financials', {}).get('income_statement', {}).get('revenues', {}).get('value', 0)
                        prev_rev = previous.get('financials', {}).get('income_statement', {}).get('revenues', {}).get('value', 0)
                        
                        if latest_rev and prev_rev and prev_rev > 0:
                            revenue_growth = (latest_rev - prev_rev) / prev_rev
                            
                            if revenue_growth > 0.15:  # >15% growth
                                score += 10
                                earnings_data['revenue_growth'] = f"+{revenue_growth:.1%}"
                            elif revenue_growth > 0.05:  # 5-15% growth
                                score += 8
                                earnings_data['revenue_growth'] = f"+{revenue_growth:.1%}"
                            elif revenue_growth > 0:  # Positive growth
                                score += 5
                                earnings_data['revenue_growth'] = f"+{revenue_growth:.1%}"
                            elif revenue_growth > -0.05:  # Small decline
                                score += 3
                                earnings_data['revenue_growth'] = f"{revenue_growth:.1%}"
                            else:  # Large decline
                                score += 0
                                earnings_data['revenue_growth'] = f"{revenue_growth:.1%}"
                        
                        # Calculate earnings quality metrics
                        net_income = latest.get('financials', {}).get('income_statement', {}).get('net_income_loss', {}).get('value', 0)
                        prev_net_income = previous.get('financials', {}).get('income_statement', {}).get('net_income_loss', {}).get('value', 0)
                        
                        if net_income and prev_net_income and prev_net_income != 0:
                            earnings_growth = (net_income - prev_net_income) / abs(prev_net_income)
                            
                            if earnings_growth > 0.20:  # >20% earnings growth
                                score += 15
                                earnings_data['earnings_growth'] = f"+{earnings_growth:.1%}"
                            elif earnings_growth > 0.10:  # 10-20% growth
                                score += 12
                                earnings_data['earnings_growth'] = f"+{earnings_growth:.1%}"
                            elif earnings_growth > 0:  # Positive growth
                                score += 8
                                earnings_data['earnings_growth'] = f"+{earnings_growth:.1%}"
                            elif earnings_growth > -0.10:  # Small decline
                                score += 3
                                earnings_data['earnings_growth'] = f"{earnings_growth:.1%}"
                            else:  # Large decline
                                score += 0
                                earnings_data['earnings_growth'] = f"{earnings_growth:.1%}"
                        
                        # Add filing recency bonus
                        filing_date = latest.get('filing_date')
                        if filing_date:
                            filing_dt = datetime.strptime(filing_date, '%Y-%m-%d')
                            days_ago = (datetime.now() - filing_dt).days
                            if days_ago <= 3:
                                score += 2  # Bonus for very recent filings
                                earnings_data['filing_recency'] = f"{days_ago} days ago"
            
            return min(score, 25), earnings_data
        except Exception as e:
            print(f"Error calculating earnings score for {ticker}: {e}")
            return 12.5, {'earnings_data': 'Error fetching data'}
    
    async def _calculate_guidance_score(self, ticker: str, session: aiohttp.ClientSession) -> Tuple[float, dict]:
        """Calculate score based on forward guidance using Polygon data (0-20 points)"""
        try:
            guidance_data = {}
            score = 10  # Base score
            
            # Get recent news for guidance updates
            url = f"https://api.polygon.io/v2/reference/news"
            params = {
                'ticker': ticker,
                'published_utc.gte': (datetime.now() - timedelta(days=10)).strftime('%Y-%m-%d'),
                'limit': 100,
                'apikey': API_KEY
            }
            
            guidance_signals = []
            async with session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get('results'):
                        for article in data['results']:
                            title = article.get('title', '').lower()
                            description = article.get('description', '').lower() if article.get('description') else ''
                            content = f"{title} {description}"
                            
                            # Look for guidance signals in news
                            if any(word in content for word in ['guidance', 'outlook', 'forecast', 'expects', 'projects']):
                                if any(word in content for word in ['raises', 'increases', 'boosts', 'lifts', 'upgrades']):
                                    guidance_signals.append('raise')
                                elif any(word in content for word in ['cuts', 'lowers', 'reduces', 'downgrades', 'slashes']):
                                    guidance_signals.append('cut')
                                elif any(word in content for word in ['maintains', 'reaffirms', 'confirms']):
                                    guidance_signals.append('maintain')
                                elif any(word in content for word in ['beats', 'exceeds', 'above']):
                                    guidance_signals.append('beat')
            
            # Score based on guidance signals
            if guidance_signals:
                raises = guidance_signals.count('raise')
                cuts = guidance_signals.count('cut')
                beats = guidance_signals.count('beat')
                maintains = guidance_signals.count('maintain')
                
                if raises > cuts and raises >= 1:
                    score += 20
                    guidance_data['guidance_signal'] = "Guidance Raised"
                elif beats > 0 and cuts == 0:
                    score += 15
                    guidance_data['guidance_signal'] = "Beat Expectations"
                elif maintains > cuts:
                    score += 12
                    guidance_data['guidance_signal'] = "Guidance Maintained"
                elif cuts > raises:
                    score += 2
                    guidance_data['guidance_signal'] = "Guidance Cut"
                else:
                    score += 8
                    guidance_data['guidance_signal'] = "Mixed Signals"
                
                guidance_data['signal_count'] = f"Raises: {raises}, Cuts: {cuts}, Beats: {beats}"
            
            # Fallback: Analyze financial trends for implied guidance
            financial_url = f"https://api.polygon.io/vX/reference/financials"
            financial_params = {
                'ticker': ticker,
                'limit': 8,  # 2 years of quarterly data
                'order': 'desc',
                'sort': 'filing_date',
                'apikey': API_KEY
            }
            
            async with session.get(financial_url, params=financial_params) as fin_response:
                if fin_response.status == 200:
                    fin_data = await fin_response.json()
                    if fin_data.get('results') and len(fin_data['results']) >= 4:
                        results = fin_data['results']
                        
                        # Calculate revenue trend (last 4 quarters)
                        revenues = []
                        for result in results[:4]:
                            rev = result.get('financials', {}).get('income_statement', {}).get('revenues', {}).get('value')
                            if rev:
                                revenues.append(rev)
                        
                        if len(revenues) >= 3:
                            # Calculate growth trend
                            recent_growth = (revenues[0] - revenues[1]) / revenues[1] if revenues[1] else 0
                            older_growth = (revenues[1] - revenues[2]) / revenues[2] if revenues[2] else 0
                            
                            if recent_growth > older_growth and recent_growth > 0.05:
                                score += 5
                                guidance_data['trend_signal'] = "Accelerating Growth"
                            elif recent_growth > 0 and abs(recent_growth - older_growth) < 0.02:
                                score += 3
                                guidance_data['trend_signal'] = "Consistent Growth"
                            elif recent_growth < older_growth:
                                score -= 2
                                guidance_data['trend_signal'] = "Decelerating Growth"
            
            return min(max(score, 0), 20), guidance_data
        except Exception as e:
            print(f"Error calculating guidance score for {ticker}: {e}")
            return 10, {'guidance_signal': 'Error fetching data'}
    
    async def _calculate_price_action_score(self, ticker: str, earnings_date: datetime, session: aiohttp.ClientSession) -> Tuple[float, dict]:
        """Calculate score based on post-earnings price action using Polygon data (0-20 points)"""
        try:
            price_data = {}
            score = 0
            
            # Get price data - try DB first, then fall back to Polygon API
            df = get_dataframe_from_db(ticker, days=30)
            if df is None or df.empty or len(df) < 5:
                df = await get_polygon_data(ticker, session)
            if df is None or df.empty or len(df) < 5:
                return 10, {'price_action': 'Insufficient data'}
            
            # Find earnings date or closest date
            earnings_date_str = earnings_date.strftime('%Y-%m-%d')
            
            # Get recent data around earnings
            recent_data = df.tail(10)  # Last 10 days
            
            if len(recent_data) < 3:
                return 10, {'price_action': 'Not enough recent data'}
            
            # Find pre-earnings and current prices
            earnings_idx = None
            for i, (date, row) in enumerate(recent_data.iterrows()):
                if date.strftime('%Y-%m-%d') >= earnings_date_str:
                    earnings_idx = i
                    break
            
            if earnings_idx is None or earnings_idx == 0:
                # Use approximate method - compare last 3 days vs previous 3 days
                recent_price = recent_data['close'].iloc[-1]
                older_price = recent_data['close'].iloc[-4] if len(recent_data) >= 4 else recent_data['close'].iloc[0]
                post_earnings_return = (recent_price - older_price) / older_price
            else:
                pre_earnings_close = recent_data['close'].iloc[earnings_idx - 1]
                current_price = recent_data['close'].iloc[-1]
                post_earnings_return = (current_price - pre_earnings_close) / pre_earnings_close
                price_data['pre_earnings_price'] = pre_earnings_close
            
            price_data['current_price'] = recent_data['close'].iloc[-1]
            
            # Score based on post-earnings performance
            if post_earnings_return > 0.08:  # >8% gain
                score = 20
                price_data['performance'] = f"+{post_earnings_return:.1%}"
                price_data['signal'] = "Very Strong Positive"
            elif post_earnings_return > 0.04:  # 4-8% gain
                score = 17
                price_data['performance'] = f"+{post_earnings_return:.1%}"
                price_data['signal'] = "Strong Positive"
            elif post_earnings_return > 0.01:  # 1-4% gain
                score = 14
                price_data['performance'] = f"+{post_earnings_return:.1%}"
                price_data['signal'] = "Positive"
            elif post_earnings_return > -0.01:  # -1% to +1%
                score = 10
                price_data['performance'] = f"{post_earnings_return:.1%}"
                price_data['signal'] = "Neutral"
            elif post_earnings_return > -0.04:  # -1% to -4%
                score = 6
                price_data['performance'] = f"{post_earnings_return:.1%}"
                price_data['signal'] = "Negative"
            elif post_earnings_return > -0.08:  # -4% to -8%
                score = 3
                price_data['performance'] = f"{post_earnings_return:.1%}"
                price_data['signal'] = "Strong Negative"
            else:  # <-8%
                score = 0
                price_data['performance'] = f"{post_earnings_return:.1%}"
                price_data['signal'] = "Very Negative"
            
            # Volume analysis using Polygon data
            if 'volume' in recent_data.columns:
                avg_volume = recent_data['volume'].iloc[:-2].mean()  # Exclude last 2 days
                recent_volume = recent_data['volume'].iloc[-2:].mean()  # Last 2 days
                
                volume_ratio = recent_volume / avg_volume if avg_volume > 0 else 1
                
                if volume_ratio > 2.0:  # Very high volume
                    price_data['volume_signal'] = "Very High Volume"
                    if post_earnings_return > 0:
                        score += 4  # Bonus for very high volume + positive return
                    price_data['volume_ratio'] = f"{volume_ratio:.1f}x"
                elif volume_ratio > 1.5:  # High volume
                    price_data['volume_signal'] = "High Volume"
                    if post_earnings_return > 0:
                        score += 2  # Bonus for high volume + positive return
                    price_data['volume_ratio'] = f"{volume_ratio:.1f}x"
                else:
                    price_data['volume_signal'] = "Normal Volume"
                    price_data['volume_ratio'] = f"{volume_ratio:.1f}x"
            
            # Volatility analysis
            price_changes = recent_data['close'].pct_change().dropna()
            volatility = price_changes.std()
            
            if volatility > 0.05:  # High volatility (>5% daily moves)
                price_data['volatility_signal'] = "High Volatility"
                if post_earnings_return > 0:
                    score += 1  # Small bonus for positive returns in high vol
            else:
                price_data['volatility_signal'] = "Normal Volatility"
            
            price_data['volatility'] = f"{volatility:.1%}"
            
            return min(score, 20), price_data
        except Exception as e:
            print(f"Error calculating price action score for {ticker}: {e}")
            return 10, {'price_action': f'Error: {str(e)}'}
    
    async def _calculate_financial_health_score(self, ticker: str, session: aiohttp.ClientSession) -> Tuple[float, dict]:
        """Calculate score based on financial health metrics using Polygon data (0-20 points)"""
        try:
            health_data = {}
            score = 10  # Base score
            
            # Get financial statements from Polygon
            url = f"https://api.polygon.io/vX/reference/financials"
            params = {
                'ticker': ticker,
                'limit': 4,  # Last 4 quarters for trend analysis
                'order': 'desc',
                'sort': 'filing_date',
                'apikey': API_KEY
            }
            
            async with session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get('results') and len(data['results']) >= 1:
                        latest = data['results'][0]
                        financials = latest.get('financials', {})
                        
                        # Get balance sheet data
                        balance_sheet = financials.get('balance_sheet', {})
                        income_statement = financials.get('income_statement', {})
                        
                        # Calculate debt metrics
                        total_debt = balance_sheet.get('liabilities', {}).get('value', 0) or 0
                        total_equity = balance_sheet.get('equity', {}).get('value', 0) or 0
                        
                        if total_equity and total_equity > 0:
                            debt_to_equity_ratio = total_debt / total_equity
                            
                            if debt_to_equity_ratio < 0.3:  # Very low debt
                                score += 5
                                health_data['debt_signal'] = "Low Debt"
                            elif debt_to_equity_ratio < 0.6:  # Moderate debt
                                score += 3
                                health_data['debt_signal'] = "Moderate Debt"
                            elif debt_to_equity_ratio < 1.0:  # High but manageable
                                score += 1
                                health_data['debt_signal'] = "High Debt"
                            else:  # Very high debt
                                score += 0
                                health_data['debt_signal'] = "Very High Debt"
                            
                            health_data['debt_to_equity'] = f"{debt_to_equity_ratio:.2f}"
                        
                        # Calculate ROE (Return on Equity)
                        net_income = income_statement.get('net_income_loss', {}).get('value', 0) or 0
                        if net_income and total_equity and total_equity > 0:
                            roe = net_income / total_equity
                            
                            if roe > 0.20:  # >20% ROE
                                score += 5
                                health_data['roe_signal'] = "Excellent ROE"
                            elif roe > 0.15:  # 15-20% ROE
                                score += 4
                                health_data['roe_signal'] = "Very Good ROE"
                            elif roe > 0.10:  # 10-15% ROE
                                score += 3
                                health_data['roe_signal'] = "Good ROE"
                            elif roe > 0.05:  # 5-10% ROE
                                score += 2
                                health_data['roe_signal'] = "Fair ROE"
                            else:  # <5% ROE
                                score += 0
                                health_data['roe_signal'] = "Poor ROE"
                            
                            health_data['roe'] = f"{roe:.1%}"
                        
                        # Calculate Profit Margins
                        revenues = income_statement.get('revenues', {}).get('value', 0) or 0
                        if net_income and revenues and revenues > 0:
                            profit_margin = net_income / revenues
                            
                            if profit_margin > 0.25:  # >25% margin
                                score += 5
                                health_data['margin_signal'] = "Excellent Margins"
                            elif profit_margin > 0.15:  # 15-25% margin
                                score += 4
                                health_data['margin_signal'] = "High Margins"
                            elif profit_margin > 0.08:  # 8-15% margin
                                score += 3
                                health_data['margin_signal'] = "Good Margins"
                            elif profit_margin > 0.03:  # 3-8% margin
                                score += 2
                                health_data['margin_signal'] = "Fair Margins"
                            else:  # <3% margin
                                score += 0
                                health_data['margin_signal'] = "Low Margins"
                            
                            health_data['profit_margin'] = f"{profit_margin:.1%}"
                        
                        # Calculate Current Ratio (Liquidity)
                        current_assets = balance_sheet.get('current_assets', {}).get('value', 0) or 0
                        current_liabilities = balance_sheet.get('current_liabilities', {}).get('value', 0) or 0
                        
                        if current_assets and current_liabilities and current_liabilities > 0:
                            current_ratio = current_assets / current_liabilities
                            
                            if current_ratio > 2.0:  # Very strong liquidity
                                score += 3
                                health_data['liquidity_signal'] = "Very Strong"
                            elif current_ratio > 1.5:  # Strong liquidity
                                score += 2
                                health_data['liquidity_signal'] = "Strong"
                            elif current_ratio > 1.0:  # Adequate liquidity
                                score += 1
                                health_data['liquidity_signal'] = "Adequate"
                            else:  # Weak liquidity
                                score += 0
                                health_data['liquidity_signal'] = "Weak"
                            
                            health_data['current_ratio'] = f"{current_ratio:.2f}"
                        
                        # Trend analysis if we have multiple quarters
                        if len(data['results']) >= 2:
                            prev_financials = data['results'][1].get('financials', {})
                            prev_income = prev_financials.get('income_statement', {})
                            prev_net_income = prev_income.get('net_income_loss', {}).get('value', 0) or 0
                            
                            if net_income and prev_net_income and prev_net_income != 0:
                                earnings_trend = (net_income - prev_net_income) / abs(prev_net_income)
                                
                                if earnings_trend > 0.10:  # >10% earnings growth
                                    score += 2
                                    health_data['earnings_trend'] = "Growing"
                                elif earnings_trend > -0.10:  # Stable earnings
                                    score += 1
                                    health_data['earnings_trend'] = "Stable"
                                else:  # Declining earnings
                                    score -= 1
                                    health_data['earnings_trend'] = "Declining"
            
            return min(max(score, 0), 20), health_data
        except Exception as e:
            print(f"Error calculating financial health for {ticker}: {e}")
            return 10, {'financial_health': 'Error fetching data'}
    
    async def _calculate_analyst_sentiment_score(self, ticker: str, session: aiohttp.ClientSession) -> Tuple[float, dict]:
        """Calculate score based on analyst sentiment using Polygon news data (0-15 points)"""
        try:
            analyst_data = {}
            score = 7.5  # Base score
            
            # Get recent analyst news and ratings
            url = f"https://api.polygon.io/v2/reference/news"
            params = {
                'ticker': ticker,
                'published_utc.gte': (datetime.now() - timedelta(days=14)).strftime('%Y-%m-%d'),
                'limit': 100,
                'apikey': API_KEY
            }
            
            analyst_signals = []
            upgrade_signals = []
            downgrade_signals = []
            target_signals = []
            
            async with session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get('results'):
                        for article in data['results']:
                            title = article.get('title', '').lower()
                            description = article.get('description', '').lower() if article.get('description') else ''
                            content = f"{title} {description}"
                            
                            # Look for analyst actions
                            if any(word in content for word in ['analyst', 'rating', 'price target', 'upgrade', 'downgrade']):
                                
                                # Upgrades
                                if any(word in content for word in ['upgrade', 'raises', 'lifts', 'boosts', 'increases']):
                                    if any(word in content for word in ['buy', 'overweight', 'positive', 'bullish']):
                                        upgrade_signals.append('strong_upgrade')
                                    else:
                                        upgrade_signals.append('upgrade')
                                
                                # Downgrades
                                elif any(word in content for word in ['downgrade', 'cuts', 'lowers', 'reduces']):
                                    if any(word in content for word in ['sell', 'underweight', 'negative', 'bearish']):
                                        downgrade_signals.append('strong_downgrade')
                                    else:
                                        downgrade_signals.append('downgrade')
                                
                                # Target price changes
                                elif any(word in content for word in ['target', 'price target']):
                                    if any(word in content for word in ['raises', 'increases', 'lifts']):
                                        target_signals.append('target_raise')
                                    elif any(word in content for word in ['cuts', 'lowers', 'reduces']):
                                        target_signals.append('target_cut')
                                
                                # General positive/negative sentiment
                                elif any(word in content for word in ['buy', 'strong buy', 'overweight']):
                                    analyst_signals.append('positive')
                                elif any(word in content for word in ['sell', 'underweight', 'avoid']):
                                    analyst_signals.append('negative')
                                elif any(word in content for word in ['hold', 'neutral', 'maintain']):
                                    analyst_signals.append('neutral')
            
            # Score based on analyst activity
            strong_upgrades = upgrade_signals.count('strong_upgrade')
            upgrades = upgrade_signals.count('upgrade')
            strong_downgrades = downgrade_signals.count('strong_downgrade')
            downgrades = downgrade_signals.count('downgrade')
            target_raises = target_signals.count('target_raise')
            target_cuts = target_signals.count('target_cut')
            
            positive_sentiment = analyst_signals.count('positive')
            negative_sentiment = analyst_signals.count('negative')
            
            # Calculate net sentiment
            net_upgrades = (strong_upgrades * 2 + upgrades) - (strong_downgrades * 2 + downgrades)
            net_targets = target_raises - target_cuts
            net_sentiment = positive_sentiment - negative_sentiment
            
            total_analyst_activity = len(upgrade_signals) + len(downgrade_signals) + len(target_signals) + len(analyst_signals)
            
            if total_analyst_activity > 0:
                if net_upgrades > 2 or (net_upgrades > 0 and net_targets > 0):
                    score += 15
                    analyst_data['sentiment'] = "Very Bullish"
                elif net_upgrades > 0 or net_targets > 1:
                    score += 12
                    analyst_data['sentiment'] = "Bullish" 
                elif net_upgrades == 0 and net_targets == 0 and net_sentiment >= 0:
                    score += 8
                    analyst_data['sentiment'] = "Neutral to Positive"
                elif net_upgrades < 0 or net_targets < 0:
                    score += 3
                    analyst_data['sentiment'] = "Bearish"
                else:
                    score += 5
                    analyst_data['sentiment'] = "Mixed"
                
                analyst_data['activity_summary'] = {
                    'upgrades': upgrades + strong_upgrades,
                    'downgrades': downgrades + strong_downgrades,
                    'target_raises': target_raises,
                    'target_cuts': target_cuts,
                    'total_mentions': total_analyst_activity
                }
            else:
                analyst_data['sentiment'] = "No Recent Activity"
                analyst_data['activity_summary'] = "Limited analyst coverage"
            
            # Coverage bonus - more activity generally means more institutional interest
            if total_analyst_activity >= 10:
                score += 3
                analyst_data['coverage_signal'] = "High Coverage"
            elif total_analyst_activity >= 5:
                score += 2
                analyst_data['coverage_signal'] = "Good Coverage"
            elif total_analyst_activity >= 2:
                score += 1
                analyst_data['coverage_signal'] = "Some Coverage"
            else:
                analyst_data['coverage_signal'] = "Limited Coverage"
            
            return min(score, 15), analyst_data
        except Exception as e:
            print(f"Error calculating analyst sentiment for {ticker}: {e}")
            return 7.5, {'analyst_sentiment': 'Error fetching data'}
    
    async def analyze_stock(self, ticker: str, earnings_date: datetime, session: aiohttp.ClientSession) -> Dict:
        """Perform comprehensive earnings quality analysis for a single stock"""
        print(f"Analyzing {ticker}...")
        
        # Calculate individual scores (all async now)
        earnings_score, earnings_data = await self._calculate_earnings_beat_score(ticker, session)
        guidance_score, guidance_data = await self._calculate_guidance_score(ticker, session)
        price_score, price_data = await self._calculate_price_action_score(ticker, earnings_date, session)
        health_score, health_data = await self._calculate_financial_health_score(ticker, session)
        analyst_score, analyst_data = await self._calculate_analyst_sentiment_score(ticker, session)
        
        # Calculate total score
        total_score = earnings_score + guidance_score + price_score + health_score + analyst_score
        
        # Determine recommendation
        if total_score >= 80:
            recommendation = "BUY IMMEDIATELY"
            timing = "Today"
            confidence = "High"
        elif total_score >= 60:
            recommendation = "BUY IN 1-2 DAYS"
            timing = "1-2 days"
            confidence = "Good"
        elif total_score >= 40:
            recommendation = "WAIT 3-5 DAYS"
            timing = "3-5 days"
            confidence = "Medium"
        elif total_score >= 20:
            recommendation = "WAIT 1-2 WEEKS"
            timing = "1-2 weeks"
            confidence = "Low"
        else:
            recommendation = "AVOID"
            timing = "Indefinite"
            confidence = "Very Low"
        
        return {
            'symbol': ticker,
            'earnings_date': earnings_date.strftime('%Y-%m-%d'),
            'analysis_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'total_score': round(total_score, 1),
            'max_score': 100,
            'recommendation': recommendation,
            'timing': timing,
            'confidence': confidence,
            'score_breakdown': {
                'earnings_beat': {'score': round(earnings_score, 1), 'max': 25, 'data': earnings_data},
                'guidance': {'score': round(guidance_score, 1), 'max': 20, 'data': guidance_data},
                'price_action': {'score': round(price_score, 1), 'max': 20, 'data': price_data},
                'financial_health': {'score': round(health_score, 1), 'max': 20, 'data': health_data},
                'analyst_sentiment': {'score': round(analyst_score, 1), 'max': 15, 'data': analyst_data}
            }
        }
    
    def _generate_summary(self, results: List[Dict]) -> Dict:
        """Generate summary statistics"""
        if not results:
            return {}
        
        scores = [r['total_score'] for r in results]
        recommendations = {}
        for result in results:
            rec = result['recommendation']
            recommendations[rec] = recommendations.get(rec, 0) + 1
        
        return {
            'average_score': round(np.mean(scores), 1),
            'highest_score': max(scores),
            'lowest_score': min(scores),
            'recommendations_breakdown': recommendations,
            'top_picks': [r for r in results if r['total_score'] >= 80],
            'avoid_list': [r for r in results if r['total_score'] < 40]
        }
    
    async def run_analysis_for_tickers(self, tickers: List[str], earnings_dates: Dict[str, datetime] = None) -> Dict:
        """Run earnings quality analysis for specific tickers with their earnings dates"""
        if not tickers:
            return {'stocks_analyzed': 0, 'results': []}
        
        # Analyze each ticker using async session
        results = []
        cached_count = 0
        async with aiohttp.ClientSession() as session:
            for ticker in tickers:
                try:
                    # Use actual earnings date if provided, otherwise use today
                    earnings_date = earnings_dates.get(ticker, datetime.now()) if earnings_dates else datetime.now()
                    
                    # Check cache first
                    cached_analysis = get_cached_analysis(ticker, earnings_date)
                    if cached_analysis:
                        results.append(cached_analysis)
                        cached_count += 1
                        continue
                    
                    # Not in cache, run fresh analysis
                    print(f"Analyzing {ticker}...")
                    analysis = await self.analyze_stock(ticker, earnings_date, session)
                    results.append(analysis)
                    # Small delay between analyses to be respectful to API
                    await asyncio.sleep(0.2)
                except Exception as e:
                    print(f"Error analyzing {ticker}: {str(e)}")
        
        if cached_count > 0:
            print(f"\n📊 Used {cached_count} cached result(s) from earlier today")
        
        # Sort by score (highest first)
        results.sort(key=lambda x: x['total_score'], reverse=True)
        
        return {
            'analysis_date': datetime.now().isoformat(),
            'stocks_analyzed': len(results),
            'results': results,
            'summary': self._generate_summary(results)
        }

    def print_results(self, analysis_results: Dict):
        """Print formatted analysis results"""
        if analysis_results['stocks_analyzed'] == 0:
            print("No stocks to analyze.")
            return
        
        print(f"\n🎯 EARNINGS QUALITY ANALYSIS RESULTS")
        print(f"Analysis Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Stocks Analyzed: {analysis_results['stocks_analyzed']}")
        print("=" * 80)
        
        results = analysis_results['results']
        
        # Print top recommendations
        top_picks = [r for r in results if r['total_score'] >= 80]
        if top_picks:
            print(f"\n🚀 TOP PICKS - BUY IMMEDIATELY:")
            for stock in top_picks:
                print(f"\n  {stock['symbol']} - Score: {stock['total_score']}/100")
                print(f"    Earnings Date: {stock['earnings_date']}")
                print(f"    Recommendation: {stock['recommendation']}")
                if 'signal' in stock:
                    print(f"    Post-Earnings: {stock.get('signal', 'N/A')}")
        
        # Print good opportunities
        good_picks = [r for r in results if 60 <= r['total_score'] < 80]
        if good_picks:
            print(f"\n✅ GOOD OPPORTUNITIES - BUY IN 1-2 DAYS:")
            for stock in good_picks:
                print(f"\n  {stock['symbol']} - Score: {stock['total_score']}/100")
                print(f"    Earnings Date: {stock['earnings_date']}")
                print(f"    Recommendation: {stock['recommendation']}")
                if 'signal' in stock:
                    print(f"    Post-Earnings: {stock.get('signal', 'N/A')}")
        
        # Print wait recommendations
        wait_picks = [r for r in results if 40 <= r['total_score'] < 60]
        if wait_picks:
            print(f"\n⏳ WAIT 3-5 DAYS:")
            for stock in wait_picks:
                print(f"\n  {stock['symbol']} - Score: {stock['total_score']}/100")
                print(f"    Earnings Date: {stock['earnings_date']}")
                print(f"    Recommendation: {stock['recommendation']}")
                if 'signal' in stock:
                    print(f"    Post-Earnings: {stock.get('signal', 'N/A')}")
        
        # Print avoid list
        avoid_picks = [r for r in results if r['total_score'] < 40]
        if avoid_picks:
            print(f"\n❌ AVOID OR WAIT LONGER:")
            for stock in avoid_picks:
                print(f"\n  {stock['symbol']} - Score: {stock['total_score']}/100")
                print(f"    Earnings Date: {stock['earnings_date']}")
                print(f"    Recommendation: {stock['recommendation']}")
                if 'signal' in stock:
                    print(f"    Post-Earnings: {stock.get('signal', 'N/A')}")
        
        # Print summary
        summary = analysis_results['summary']
        if summary:
            print(f"\n📊 SUMMARY STATISTICS:")
            print(f"  Average Score: {summary['average_score']}/100")
            print(f"  Score Range: {summary['lowest_score']} - {summary['highest_score']}")
            print(f"  Top Picks: {len(summary.get('top_picks', []))}")
            print(f"  Avoid List: {len(summary.get('avoid_list', []))}")

def get_cached_analysis(ticker: str, earnings_date: datetime) -> Optional[Dict]:
    """Check if we already have analysis for this ticker + earnings date from today"""
    db = SessionLocal()
    try:
        today = datetime.now().date()
        earnings_date_only = earnings_date.date() if isinstance(earnings_date, datetime) else earnings_date
        
        # Query for existing analysis from today
        result = db.query(EarningsQualityData)\
            .filter(
                EarningsQualityData.symbol == ticker,
                EarningsQualityData.earnings_date == earnings_date_only,
                EarningsQualityData.data_date == today
            )\
            .order_by(EarningsQualityData.created_at.desc())\
            .first()
        
        if result:
            print(f"✅ Using cached analysis for {ticker} (analyzed earlier today)")
            return json.loads(result.data_json)
        
        return None
    except Exception as e:
        print(f"⚠️ Error checking cache for {ticker}: {e}")
        return None
    finally:
        db.close()

def save_to_database(results: Dict):
    """Save earnings quality analysis results to database"""
    db = SessionLocal()
    try:
        now = datetime.now()
        for result in results.get('results', []):
            # Parse earnings date from string
            earnings_date = datetime.strptime(result['earnings_date'], '%Y-%m-%d').date()
            
            # Create database entry
            db_entry = EarningsQualityData(
                symbol=result['symbol'],
                earnings_date=earnings_date,
                data_date=now.date(),
                data_time=now.time(),
                data_json=json.dumps(result, default=str)
            )
            db.add(db_entry)
        
        db.commit()
        print(f"💾 Saved {len(results.get('results', []))} results to database")
    except Exception as e:
        print(f"❌ Error saving to database: {e}")
        db.rollback()
    finally:
        db.close()
    
async def main():
    """Main execution function"""
    analyzer = EarningsQualityAnalyzer()
    
    # Get earnings tickers for today and last 2 trading days
    print("🎯 EARNINGS QUALITY SCORE ANALYSIS")
    print("=" * 50)
    print("Getting earnings tickers (today + last 2 trading days) from Yahoo Finance...")
    
    # Get the last 3 trading days (excluding weekends)
    trading_days = get_last_n_trading_days(3)
    print(f"Checking trading days: {[d.strftime('%Y-%m-%d (%A)') for d in trading_days]}")
    
    # Try to get real earnings data with dates
    try:
        earnings_with_dates = {}  # ticker -> date mapping
        for target_date in trading_days:
            tickers = get_earnings_tickers(target_date)
            # Store each ticker with its earnings date (keep first occurrence)
            for ticker in tickers:
                if ticker not in earnings_with_dates:
                    earnings_with_dates[ticker] = target_date
        
        all_earnings_tickers = list(earnings_with_dates.keys())
    
    except Exception as e:
        print(f"⚠️ Yahoo Finance access limited: {e}")
        print("🔄 Using sample recent earnings tickers for demonstration...")
        earnings_with_dates = {}
        
        # Use some known recent earnings as a fallback (you can update these manually)
        # These are typical stocks that report earnings frequently
        sample_tickers = ['AAPL', 'MSFT', 'GOOGL', 'TSLA', 'META', 'NVDA', 'AMZN', 'NFLX']
        all_earnings_tickers = sample_tickers
    
    if not all_earnings_tickers:
        print("❌ No earnings tickers available")
        return
    
    print(f"\nFound {len(all_earnings_tickers)} tickers for analysis:")
    # Print tickers grouped by date
    if earnings_with_dates:
        date_groups = {}
        for ticker, date in earnings_with_dates.items():
            date_str = date.strftime('%Y-%m-%d')
            if date_str not in date_groups:
                date_groups[date_str] = []
            date_groups[date_str].append(ticker)
        
        for date_str in sorted(date_groups.keys(), reverse=True):
            tickers_list = date_groups[date_str]
            print(f"  {date_str}: {', '.join(tickers_list)}")
    else:
        print(f"  Sample tickers: {', '.join(all_earnings_tickers)}")
    
    print("\n" + "=" * 50)
    print("ANALYZING EARNINGS QUALITY...")
    print("=" * 50)
    
    # Run the analysis on earnings tickers with their actual dates
    results = await analyzer.run_analysis_for_tickers(all_earnings_tickers, earnings_with_dates)
    
    # Print results
    analyzer.print_results(results)
    
    # Save results to JSON
    import os
    os.makedirs('reports', exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f'reports/earnings_quality_analysis_{timestamp}.json'
    
    with open(filename, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    with open('reports/latest_earnings_quality.json', 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    print(f"\n💾 Results saved to: {filename}")
    print(f"💾 Latest results: reports/latest_earnings_quality.json")
    
    # Save to database
    save_to_database(results)

if __name__ == "__main__":
    print("✅ Using Yahoo Finance earnings calendar (pandas read_html)")
    print("📅 Analyzing ONLY stocks with earnings TODAY and YESTERDAY")
    print("=" * 80)
    asyncio.run(main())