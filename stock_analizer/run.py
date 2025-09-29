# run.py

import uvicorn
from app.main import app  # This is where your FastAPI app instance lives

if __name__ == "__main__":
    uvicorn.run("app.main:app", host="localhost", port=8000, reload=True)