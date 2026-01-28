import os
import re
import sqlite3
import hashlib
import asyncio
import requests
from datetime import datetime, timezone, timedelta

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from jinja2 import Template

app = FastAPI()

# --------------------
# DB helpers (упрощенная версия)
# --------------------
def db():
    conn = sqlite3.connect("/opt/freerg/data/data.sqlite3")
    return conn

# --------------------
# Основной маршрут (упрощенный)
# --------------------
@app.api_route("/", methods=["GET", "HEAD"], response_class=HTMLResponse)
def index():
    html = """
    <!doctype html>
    <html>
    <head><title>Test</title></head>
    <body>
        <h1>FreeRedeemGames Test</h1>
        <p>Если видишь это - приложение работает!</p>
    </body>
    </html>
    """
    return HTMLResponse(content=html)

# --------------------
# Health check
# --------------------
@app.api_route("/health", methods=["GET", "HEAD"])
def health():
    return {"ok": True}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)