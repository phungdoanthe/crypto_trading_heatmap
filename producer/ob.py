from pydantic import BaseModel
from typing import Literal
import time
import asyncio, json, websockets
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode()
)

class OrderBookSnapshot(BaseModel):
    symbol:    str
    timestamp: int
    price: int
    qty: int
    order_type: Literal['bid', 'ask']

async def stream_order_book(symbol: str = 'btcusdt'):
    # @depth20@100ms = 20-level book, pushed every 100ms
    url = f"wss://stream.binance.com:9443/ws/{symbol}@depth20@100ms"

    async with websockets.connect(url, ping_interval=20) as ws:
        async for raw in ws:
            msg = json.loads(raw)
            for order_type in ['bids', 'asks']:
                for price, qty in msg[order_type]:
                    record = {
                        'symbol':    symbol.upper(),
                        'timestamp': int(time.time() * 1000),          
                        'price':     float(price),   
                        'qty':    float(qty),
                        'order_type': order_type[:-1]  # 'bids' -> 'bid', 'asks' -> 'ask'
                    }
                    record = OrderBookSnapshot(**record)
                    producer.send('raw_order_book', value=record.model_dump())
