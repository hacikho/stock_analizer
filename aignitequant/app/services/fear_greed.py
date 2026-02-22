"""
Fear & Greed Index Service

Fetches real-time market sentiment data from CNN's Fear & Greed Index.
The index ranges from 0-100 and aggregates 7 market indicators:
- Stock Price Momentum, Stock Price Strength, Stock Price Breadth
- Put/Call Ratios, Market Volatility (VIX)
- Safe Haven Demand, Junk Bond Demand

Score interpretation provided directly by CNN.
"""

from fear_and_greed import get


def get_cnn_fear_greed():
    """
    Fetch current CNN Fear & Greed Index with live sentiment.
    
    Returns:
        dict: Contains:
            - cnn_fear_greed_score (float): Index value 0-100
            - comment (str): CNN's sentiment label (e.g., "neutral", "fear", "greed")
            - last_update (str): ISO timestamp of last CNN update
            
    Example:
        {
            "cnn_fear_greed_score": 48.91,
            "comment": "neutral",
            "last_update": "2025-12-30T21:19:33+00:00"
        }
    """
    try:
        # Fetch live data from CNN via fear_and_greed library
        result = get()
        score = result.value
        
        # Validate score is within expected range
        if isinstance(score, (int, float)) and 0 <= score <= 100:
            return {
                "cnn_fear_greed_score": score,
                "comment": result.description,  # CNN's official sentiment label
                "last_update": result.last_update.isoformat()
            }
        
        return {"error": "Could not parse score from library"}
    
    except Exception as e:
        return {"error": str(e)}
