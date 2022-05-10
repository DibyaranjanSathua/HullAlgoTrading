"""
File:           app.py
Author:         Dibyaranjan Sathua
Created on:     07/05/22, 7:09 pm
"""
from fastapi import FastAPI

from src.webhook.models import TradingViewSignal
from src.utils.redis_backend import RedisBackend


app = FastAPI()
redis_backend = RedisBackend()      # It is use as publisher
redis_backend.connect()


@app.get("/test/")
async def test():
    return {"message": "Hello from SathuaLabs"}


@app.post("/tradingview/webhook/")
async def tradingview_webhook(signal: TradingViewSignal):
    signal_as_str = signal.json()
    redis_backend.publish("strategy1", signal_as_str)
    return {"message": "Signal received successfully"}
