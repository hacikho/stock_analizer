# Stock Analyzer Platform

A comprehensive stock analysis platform that implements multiple trading strategies and provides real-time market insights through automated screening and data analysis.

## 🚀 Features

### Trading Strategies
- **Mark Minervini's Stage 2 Strategy**: Screens for stocks in strong uptrend phases
- **Golden Cross Strategy**: Identifies bullish momentum with moving average crossovers
- **Bora Strategy**: Trend-following screener with multi-timeframe analysis
- **LEAP Options Strategy**: Long-term equity anticipation securities analysis
- **CANSLIM Strategy**: Growth stock screening based on William O'Neil's methodology
- **Earnings Quality Score**: Post-earnings analysis with 100-point quality scoring system

### Market Analysis Tools
- **Fear & Greed Index**: Real-time market sentiment tracking
- **Sector Analysis**: Performance ranking across all market sectors
- **Options Analysis**: Volume and volatility insights using Polygon data
- **Technical Indicators**: RSI, moving averages, volume analysis

## 🛠️ Tech Stack

- **Backend**: FastAPI (Python)
- **Database**: SQLAlchemy with SQLite/PostgreSQL
- **Task Queue**: Celery with Redis
- **Data Sources**: Polygon.io, Yahoo Finance, Financial Modeling Prep
- **Deployment**: Railway (configured)
- **Scheduling**: APScheduler for automated screening

## 📦 Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/stock_analizer.git
   cd stock_analizer
   ```

2. **Set up virtual environment**
   ```bash
   python -m venv .venv
   # Windows
   .venv\Scripts\activate
   # macOS/Linux  
   source .venv/bin/activate
   ```

3. **Install dependencies**
   ```bash
   pip install -e .
   ```

4. **Environment setup**
   ```bash
   cp .env.example .env
   # Edit .env file with your API keys
   ```

5. **Required API Keys**
   - **Polygon.io**: Sign up at [polygon.io](https://polygon.io/) for stock market data
   - **Financial Modeling Prep**: Get free API key at [financialmodelingprep.com](https://financialmodelingprep.com/)

## 🚀 Usage

### Start the API server
```bash
uvicorn stock_analizer.app.main:app --reload
```

### Run individual strategies
```bash
# CANSLIM screening
python stock_analizer/app/strategies/canslim_strategy.py

# Earnings Quality Analysis  
python stock_analizer/app/strategies/earnings_quality_score.py

# Sector Analysis
python stock_analizer/app/strategies/test_sector_analysis.py
```

### API Endpoints
- `GET /health` - Health check
- `GET /api/canslim` - Latest CANSLIM results
- `GET /api/sectors` - Sector analysis
- `GET /api/fear-greed` - Market sentiment
- `GET /api/earnings-quality` - Latest earnings quality scores

## 📊 Strategies Overview

### CANSLIM Strategy
Screens for growth stocks based on:
- **C**: Current quarterly earnings (25%+ growth)
- **A**: Annual earnings growth (25%+ growth)  
- **N**: New products, management, price highs
- **S**: Supply & demand (volume analysis)
- **L**: Leader or laggard (relative strength)
- **I**: Institutional sponsorship ($1B+ holdings)
- **M**: Market direction (uptrend confirmation)

### Earnings Quality Score (100-point system)
Post-earnings analysis evaluating:
- **Earnings Beat/Miss** (25 points): EPS vs estimates
- **Guidance Updates** (20 points): Forward PE changes  
- **Price Action** (20 points): Post-earnings performance
- **Financial Health** (20 points): Balance sheet metrics
- **Analyst Sentiment** (15 points): Recommendation changes

## 🚂 Deployment

### Railway (Configured)
The project includes Railway configuration:
```toml
# railway.toml
[deploy]
startCommand = "uvicorn stock_analizer.app.main:app --host 0.0.0.0 --port $PORT"
```

### Environment Variables for Production
```bash
API_KEY=your_polygon_api_key
FMP_API_KEY=your_fmp_api_key  
DATABASE_URL=postgresql://...
REDIS_URL=redis://...
```

## 📁 Project Structure
```
stock_analizer/
├── app/
│   ├── api/            # FastAPI routes
│   ├── services/       # Data fetching services  
│   ├── strategies/     # Trading strategy implementations
│   └── db.py          # Database models
├── reports/           # Generated analysis reports (gitignored)
├── railway.toml       # Railway deployment config
├── pyproject.toml     # Dependencies and project metadata
└── README.md
```

## 📈 Example Results

### CANSLIM Winners (Recent)
- **GOOGL**: 35.7% quarterly growth, 7.4x relative strength
- **ALL**: 2,582% quarterly growth  
- **LDOS**: 530% quarterly growth
- **VTR**: 298% quarterly growth

### Earnings Quality Top Picks
- **NNE**: 82.5/100 score, "BUY IMMEDIATELY"
- **HEI**: 79.5/100 score, strong post-earnings performance
- **ISSC**: 79.0/100 score, +15.6% post-earnings move

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable  
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## ⚠️ Disclaimer

This software is for educational and research purposes only. Not financial advice. Always do your own research before making investment decisions.