import asyncio
import websockets
import json

async def track_kucoin_prices():
    token = "2neAiuYvAU61ZDXANAGAsiL4-iAExhsBXZxftpOeh_55i3Ysy2q2LEsEWU64mdzUOPusi34M_wGoSf7iNyEWJwq7A43M3JDbhPV8Q-xiuGJfwerKYsT00tiYB9J6i9GjsxUuhPw3BlrzazF6ghq4L2IYNEnY5_fDA6P70VLLDsw=.ZYmmTuIehSGwGsM4Q9GztQ=="

    uri = 'wss://ws-api-spot.kucoin.com/?token=' + token

    markets = [
        "BTC-USDT", "ETH-USDT", "XRP-USDT", "DOGE-USDT", "ADA-USDT"  # TOP 10
        "GMX-USDT", "ARB-USDT", "MKR-USDT", "OP-USDT", "FXS-USDT",  # TOP 100
        "SKL-USDT", "KDA-USDT", "HFT-USDT", "DODO-USDT", "FET-USDT",  # TOP 1000
        "RDNT-USDT", "CAKE-USDT", "WRX-USDT", "ZEC-USDT", "ENS-USDT"  # TOP 1000
    ]
    markets_string = ','.join(markets)
    # topics = [f'/market/level2:{symbol}' for symbol in symbols]
    async with websockets.connect(uri) as websocket:
        subscription_msg = {
            "type": "subscribe",
            "topic": "/market/ticker:" + markets_string,
            "privateChannel": False,
            "response": True
        }
        await websocket.send(json.dumps(subscription_msg))
        print("Subscribed to ticker data.")

        while True:
            try:
                response = await websocket.recv()
                data = json.loads(response)
                print(data)
                print()
            except Exception as e:
                print(f"Error: {str(e)}")
                break

async def main():
    await track_kucoin_prices()

if __name__ == '__main__':
    asyncio.run(main())
