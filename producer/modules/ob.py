from pydantic import BaseModel
from typing import Literal
import time
import asyncio, json, websockets
from utils import create_producer, safe_send

class OrderBookSnapshot(BaseModel):
    symbol:    str
    ts:        int
    price:     float
    qty:       float
    order_type: Literal['bid', 'ask']

async def stream_order_book(symbol: str = 'btcusdt'):
    # @depth20@100ms = 20-level book, pushed every 100ms
    url = f"wss://stream.binance.com:9443/ws/{symbol}@depth20@100ms"

    producer = create_producer()

    async with websockets.connect(url, ping_interval=20) as ws:
        async for raw in ws:
            msg = json.loads(raw)
            for order_type in ['bids', 'asks']:
                for price, qty in msg[order_type]:
                    record = {
                        'symbol':    symbol.upper(),
                        'ts': int(time.time() * 1000),          
                        'price':     float(price),   
                        'qty':    float(qty),
                        'order_type': order_type[:-1]
                    }
                    record = OrderBookSnapshot(**record)
                    print("Sending ob record:", record)
                    safe_send(producer, 'raw_order_book', record.model_dump())
