"""
File:           redis_backend.py
Author:         Dibyaranjan Sathua
Created on:     09/05/22, 9:26 pm
"""
from typing import Optional
import os

import redis
from dotenv import load_dotenv

from src import BASE_DIR


dotenv_path = BASE_DIR / 'env' / '.env'
load_dotenv(dotenv_path=dotenv_path)


class RedisBackend:
    """ Connect to redis backend and perform pub/sub """

    def __init__(self):
        self._host: str = os.environ.get("REDIS_HOST", "localhost")
        self._port: int = int(os.environ.get("REDIS_PORT", 6379))
        self._redis: Optional[redis.Redis] = None
        self._pubsub: Optional[redis.client.PubSub] = None

    def connect(self) -> None:
        self._redis = redis.Redis(host=self._host, port=self._port)
        self._pubsub = self._redis.pubsub()

    def subscribe(self, channel: str) -> None:
        self._pubsub.subscribe(channel)

    def publish(self, channel: str, message: str) -> None:
        self._redis.publish(channel=channel, message=message)

    def get_message(self) -> Optional[str]:
        message = self._pubsub.get_message(ignore_subscribe_messages=True)
        if message is None:
            return message
        return message["data"].decode("utf-8")
