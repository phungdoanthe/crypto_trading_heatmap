from pydantic import BaseModel
from typing import Literal
import time
import asyncio, json, websockets
from utils import create_producer

class TradeRecord(BaseModel):
    symbol:    str
    timestamp: int
    price:     float
    qty:    float
    side:      Literal['buy', 'sell']

async def stream_trade(symbol: str = 'btcusdt'):
    url = f"wss://stream.binance.com:9443/ws/{symbol}@trade"

    producer = create_producer()
    
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
            print("Sending trade record:", record)
            safe_send(producer, 'raw_order_book', record.model_dump())
