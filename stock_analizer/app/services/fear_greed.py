

from fear_and_greed import get

def get_cnn_fear_greed():
    try:
        result = get()
        score = result.value
        if isinstance(score, (int, float)) and 0 <= score <= 100:
            return {
                "cnn_fear_greed_score": score,
                "comment": result.description,
                "last_update": result.last_update.isoformat()
            }
        return {"error": "Could not parse score from library"}
    except Exception as e:
        return {"error": str(e)}

def interpret_cnn_score(score):
    if score >= 80:
        return "Extreme Greed"
    elif score >= 60:
        return "Greed"
    elif score >= 40:
        return "Neutral"
    elif score >= 20:
        return "Fear"
    else:
        return "Extreme Fear"
