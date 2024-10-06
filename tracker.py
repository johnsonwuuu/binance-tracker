import asyncio
import json
import os
import signal  # Import signal module
from datetime import datetime
import pytz
from websockets import connect
from termcolor import cprint

# Global variable to control the running state
running = True

# Signal handler to stop the stream
#ctrl c in terminal to exit
def signal_handler(sig, frame):
    global running
    running = False

# Register the signal handler
signal.signal(signal.SIGINT, signal_handler)

symbols = ['btcusdt', 'ethusdt', 'solusdt', 'bnbusdt', 'dogeusdt']  # Fixed assignment
websocket_url_base = 'wss://fstream.binance.com/ws/'
trades_filename = 'binance_trades.csv'

if not os.path.isfile(trades_filename):
    with open(trades_filename, 'w') as f:
        f.write ('Event Time, Symbol, Aggregate Trade ID, Price Quantity, First Trade ID, Buyer Maker\n')


async def balance_trade_stream(uri, symbol, filename):
    async with connect(uri) as websocket:
        while running:  # Check the running state
            try:
                message = await websocket.recv()
                data = json.loads(message)
                event_time = int(data['E'])
                agg_trade_id = data['a']
                price = float(data ['p'])
                quantity = float(data['q'])
                trade_time = int(data ['T'])
                is_buyer_maker = data['m']
                est = pytz.timezone('US/Eastern') 
                readable_trade_time = datetime.fromtimestamp(trade_time/ 1000, est).strftime('%H:%M')

                usd_size = price * quantity
                display_symbol = symbol.upper().replace('USDT', '')

                if usd_size > 14999:
                    trade_type = 'SELL' if is_buyer_maker else "BUY"
                    color = 'red' if trade_type == 'SELL' else 'green'
                    stars = ''  # Initialize stars as an empty string
                    repeat_count = 1  # Default repeat count

                    if usd_size >= 500000:
                        stars = '*' * 2  # Set stars to '**' for very large trades
                        repeat_count = 1
                        color = 'magenta' if trade_type == 'SELL' else 'blue'
                        
                    elif usd_size >= 100000:
                        stars = '*' * 2  # Set stars to '**' for trades over 100k
                        repeat_count = 1  # Ensure repeat count is set correctly

                    elif usd_size >= 50000:
                        stars = '*' * 1  # Set stars to '*' for trades over 50k
                        repeat_count = 1  # Ensure repeat count is set correctly

                    # Ensure that trades over 50k are displayed with one star and over 100k with two stars
                    output = f"{stars} {trade_type} {display_symbol} {readable_trade_time} ${usd_size:,.2f}"  # Added commas to usd_size
                    for _ in range(repeat_count):
                        cprint(output, 'white', f'on_{color}', attrs=['bold'] if stars else [])  # Use stars correctly

                    with open (filename, 'a') as f:
                        f.write(f"{event_time}, {symbol.upper()}, {agg_trade_id}, {str(price)[:-3]},{str(quantity)[:-3]},"  # Display exact number without rounding
                                f"{trade_time}, {is_buyer_maker}\n")
                        
            except Exception as e:
                await asyncio.sleep(5)

async def main():
    filename = 'binance_trades.csv'

    tasks = []
    for symbol in symbols:
        stream_url = f"{websocket_url_base}{symbol}@aggTrade"
        tasks.append(balance_trade_stream(stream_url, symbol, filename))  # Corrected function name

    await asyncio.gather(*tasks)
    
asyncio.run(main())

