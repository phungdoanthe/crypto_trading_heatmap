import asyncio
from ob import stream_order_book
from trade import stream_trade

async def main():
    await asyncio.gather(
        stream_order_book('btcusdt'),
        stream_trades('btcusdt'),
    )

if __name__ == "__main__":
    asyncio.run(main())