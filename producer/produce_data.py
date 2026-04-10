import asyncio
from modules import stream_order_book, stream_trade

async def main():
    await asyncio.gather(
        stream_order_book('btcusdt'),
        stream_trade('btcusdt'),
    )

if __name__ == "__main__":
    asyncio.run(main())