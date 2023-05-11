import asyncio
import websockets
import json
from decimal import Decimal

async def track_binance_prices():
    uri = 'wss://stream.binance.com:9443/ws'

    symbols = ["xrpusdt", "ltcusdt", "adausdt", "dotusdt",
           "uniusdt", "linkusdt", "xrmusdt", "bchusdt", "xmrudst", "eosusdt",
           "trxusdt", "dashusdt", "dogeusdtT", "zecusdt", "etcusdt", "xtzusdt",
           "atomusdt", "algousdt"]
    
    streams = [f'{symbol}@bookTicker' for symbol in symbols]
    streams_uri = '/'.join(streams)

    async with websockets.connect(f'{uri}/{streams_uri}') as websocket:
        while True:
            try:
                response = await websocket.recv()
                data = json.loads(response)
                symbol = data['s']
                bid_price = data['b']
                ask_price = data['a']
                spread = (Decimal(ask_price) - Decimal(bid_price)) / Decimal(ask_price) * 100
                print(f"Symbol: {symbol}")
                print(f"Best Bid: {bid_price}")
                print(f"Best Ask: {ask_price}")
                print(f"Spread: {spread:.4f}%")
                print()
            except Exception as e:
                print(f"Error: {str(e)}")
                break

async def main():
    await track_binance_prices()

if __name__ == '__main__':
    asyncio.run(main())
