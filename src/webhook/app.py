"""
File:           app.py
Author:         Dibyaranjan Sathua
Created on:     07/05/22, 7:09 pm
"""
from fastapi import FastAPI

from src.webhook.models import TradingViewSignal


app = FastAPI()


@app.get("/test/")
async def test():
    return {"message": "Hello from SathuaLabs"}


@app.post("/tradingview/webhook/")
async def tradingview_webhook(signal: TradingViewSignal):
    print(signal)
    return {"message": "Signal received successfully"}
