from pydantic import BaseModel
from typing import Literal
import time
import asyncio, json, websockets
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode())

class TradeRecord(BaseModel):
    symbol:    str
    timestamp: int
    price:     float
    qty:    float
    side:      Literal['buy', 'sell']

async def stream_trade(symbol: str = 'btcusdt'):
    url = f"wss://stream.binance.com:9443/ws/{symbol}@trade"

    async with websockets.connect(url, ping_interval=20) as ws:
        async for raw in ws:
            msg = json.loads(raw)
            record = {
                'symbol':    symbol.upper(),
                'timestamp': msg['T'],
                'price':     float(msg['p']),
                'qty':       float(msg['q']),
                'side':      'buy' if not msg['m'] else 'sell' ,
            }
            record = TradeRecord(**record)
            print("Sending record:", record)
            producer.send('raw_trade', value=record.model_dump())
