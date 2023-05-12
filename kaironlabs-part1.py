import time
import asyncio
import aiohttp
import sqlite3
from datetime import datetime
from prettytable import PrettyTable


# Create the database
def create_database():
    # Connect to the SQLite database
    conn = sqlite3.connect('market_data.db')
    c = conn.cursor()

    # Create the market_data table if it doesn't exist
    c.execute('''CREATE TABLE IF NOT EXISTS market_data (
                    Market TEXT,
                    KuCoinTimestamp TEXT,
                    KuCoinBid REAL,
                    KuCoinAsk REAL,
                    KuCoinSpread REAL,
                    KuCoinSlippage REAL,
                    PRIMARY KEY (Market, KuCoinTimestamp, KuCoinBid, KuCoinAsk, KuCoinSpread)
                )''')

    # Commit the changes and close the connection
    conn.commit()
    conn.close()


# Fetch data from the specified URL using the provided session
async def fetch_data(session, url):
    async with session.get(url) as response:
        return await response.json()


# Get the market data from KuCoin and Binance
async def get_market_data(session):
    market_data = []

    # Create tasks for fetching data from KuCoin and Binance for each market
    kucoin_tasks = [fetch_data(session, kucoin_url.format(market.replace("/", "-"))) for market in markets]
    binance_tasks = [fetch_data(session, binance_url.format(market.replace("/", ""))) for market in markets]

    # Gather the responses from KuCoin and Binance for all markets
    kucoin_responses = await asyncio.gather(*kucoin_tasks)
    binance_responses = await asyncio.gather(*binance_tasks)

    # Process the responses and extract the required data for each market
    for kucoin_data, binance_data, market in zip(kucoin_responses, binance_responses, markets):
        try:
            kucoin_bid = float(kucoin_data['data']['bestBid'])
            kucoin_ask = float(kucoin_data['data']['bestAsk'])
            timestamp = kucoin_data['data']['time']
            binance_bid = float(binance_data['bidPrice'])
            binance_ask = float(binance_data['askPrice'])
            kucoin_spread_relative = (kucoin_ask - kucoin_bid) / kucoin_ask
            binance_spread_relative = (binance_ask - binance_bid) / binance_ask
            kucoin_spread_absolute = kucoin_ask - kucoin_bid
            kucoin_actual_buy = kucoin_ask + 0.02 * kucoin_spread_absolute  # Calculate slippage within 2% of the spread
            slippage = (kucoin_actual_buy - kucoin_ask) / kucoin_ask # Kucoin ask price is the expected buy price 

            market_data.append({
                'Market': market,
                'KuCoin Timestamp': datetime.fromtimestamp(timestamp / 1000.0).replace(microsecond=0),
                'KuCoin Bid': kucoin_bid,
                'KuCoin Ask': kucoin_ask,
                'KuCoin Spread [%]': round(kucoin_spread_relative * 100, 5),
                'KuCoin Slippage [%]': round(slippage * 100, 6),
                'Binance Bid': binance_bid,
                'Binance Ask': binance_ask,
                'Binance Spread [%]': round(binance_spread_relative * 100, 5)
            })
            
        except (KeyError, ValueError):
            print(f"Error reading data for market {market}. Skipping iteration.")
            continue

    return market_data

def store_market_data(market_data):
    # Convert market_data list of dictionaries into a list of tuples
    data_tuples = [(d['Market'], d['KuCoin Timestamp'], d['KuCoin Bid'], d['KuCoin Ask'], d['KuCoin Spread [%]'],
                    d['KuCoin Slippage [%]'])
                for d in market_data]

    # Insert data, ignore if duplicate
    c.executemany('''INSERT OR IGNORE INTO market_data
                    VALUES (?, ?, ?, ?, ?, ?)''', data_tuples)
    conn.commit()

# Start the event loop
async def monitor_and_store():
    async with aiohttp.ClientSession() as session:
        while True:
            print("[FETCHING]", datetime.fromtimestamp(time.time()))
            market_data = await get_market_data(session)
            store_market_data(market_data)
            
            # Display current market data in a table
            x = PrettyTable()
            x.field_names =['Market', 'KuCoin Timestamp', 'KuCoin Bid', 'KuCoin Ask',
                            'KuCoin Spread [%]', 'KuCoin Slippage [%]', 'Binance Bid', 
                            'Binance Ask', 'Binance Spread [%]']
            for i in range(len(markets)):
                x.add_row(list(market_data[i].values()))
            print(x)
            
            # Delay before fetching data again
            await asyncio.sleep(frequency)

if __name__ == "__main__":
    # Define the URLs and other constants
    kucoin_url = 'https://api.kucoin.com/api/v1/market/orderbook/level1?symbol={}'
    binance_url = 'https://api.binance.com/api/v3/ticker/bookTicker?symbol={}'
    frequency = 15  # Fetch data approximately every 15 seconds

    # 20 markets to monitor
    markets = [
        "BTC/USDT", "ETH/USDT", "XRP/USDT", "DOGE/USDT", "ADA/USDT",  # TOP 10
        "GMX/USDT", "ARB/USDT", "MKR/USDT", "OP/USDT", "FXS/USDT",  # TOP 100
        "SKL/USDT", "KDA/USDT", "HFT/USDT", "DODO/USDT", "FET/USDT",  # TOP 1000
        "RDNT/USDT", "CAKE/USDT", "WRX/USDT", "ZEC/USDT", "ENS/USDT"  # TOP 1000
    ]
    
    create_database()
    conn = sqlite3.connect('market_data.db')
    c = conn.cursor()
    asyncio.run(monitor_and_store())
    conn.close()
