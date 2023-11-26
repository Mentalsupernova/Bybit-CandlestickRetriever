#!/usr/bin/env python
# coding: utf-8

import json
import os
import random
import subprocess
import sys
import time
from datetime import date, datetime, timedelta
import numpy as np
import requests
import pandas as pd
import preprocessing as pp

START_TIME = (datetime.now() - timedelta(hours=0, minutes=5))
API_BASE = 'https://api.bybit.com/v5/'

LABELS = [
    'open_time',
    'open',
    'high',
    'low',
    'close',
    'volume',
    'quote_asset_volume',
]

category = 'spot'

def get_batch(symbol, interval='1', start_time=0, limit=1000):
    """Use a GET request to retrieve a batch of candlesticks. Process the JSON into a pandas
    dataframe and return it. If not successful, return an empty dataframe.
    """

    params = {
        'category': category,
        'symbol': symbol,
        'interval': interval,
        'start': start_time,
        'limit': limit
    }
    try:
        # timeout should also be given as a parameter to the function
        response = requests.get(f'{API_BASE}market/kline', params, timeout=30)
        data = response.json()['result']['list']
        data.reverse()

    except requests.exceptions.ConnectionError:
        print('Connection error, Cooling down for 5 mins...')
        time.sleep(5 * 60)
        return get_batch(symbol, interval, start_time, limit)
    
    except requests.exceptions.Timeout:
        print('Timeout, Cooling down for 5 min...')
        time.sleep(5 * 60)
        return get_batch(symbol, interval, start_time, limit)
    
    except requests.exceptions.ConnectionResetError:
        print('Connection reset by peer, Cooling down for 5 min...')
        time.sleep(5 * 60)
        return get_batch(symbol, interval, start_time, limit)

    except Exception as e:
        print(f'Unknown error: {e}, Cooling down for 5 min...')
        time.sleep(5 * 60)
        return get_batch(symbol, interval, start_time, limit)

    if response.status_code == 200:
        df = pd.DataFrame(data, columns=LABELS)
        df['open_time'] = df['open_time'].astype(np.int64)
        df = df[df.open_time < START_TIME.timestamp() * 1000]
        return df
    print(f'Got erroneous response back: {response}')
    return pd.DataFrame([])

def all_candles_to_csv(base, quote, interval='1'):
    """Collect a list of candlestick batches with all candlesticks of a trading pair,
    concat into a dataframe and write it to CSV.
    """

    # see if there is any data saved on disk already
    try:
        batches = [pd.read_csv(f'data/{base}-{quote}.csv')]
        last_timestamp = batches[-1]['open_time'].max()
        new_file = False
    except FileNotFoundError:
        batches = [pd.DataFrame([], columns=LABELS)]
        # last_timestamp = int((datetime.now() - timedelta(days=7)).timestamp() * 1000)
        last_timestamp = 1640995200000  # 2022-01-01
        new_file = True
    old_lines = len(batches[-1].index)

    # gather all candlesticks available, starting from the last timestamp loaded from disk or 0
    # stop if the timestamp that comes back from the api is the same as the last one
    previous_timestamp = None

    while previous_timestamp != last_timestamp:
        # stop if we reached data from today
        #if date.fromtimestamp(last_timestamp / 1000) >= date.today():
        #    break

        previous_timestamp = last_timestamp

        new_batch = get_batch(
            symbol=base+quote,
            interval=interval,
            start_time=last_timestamp+1
        )

        # requesting candles from the future returns empty
        # also stop in case response code was not 200
        if new_batch.empty and not new_file:
            break

        if new_batch.empty:
            last_timestamp = last_timestamp + 86400000
        else:
            last_timestamp = new_batch['open_time'].max()

        # sometimes no new trades took place yet on date.today();
        # in this case the batch is nothing new
        if previous_timestamp == last_timestamp:
            break

        if not new_batch.empty:
            batches.append(new_batch)
            new_file = False
        last_datetime = datetime.fromtimestamp(last_timestamp / 1000)

        covering_spaces = 20 * ' '
        print(datetime.now(), base, quote, interval + 'min', str(last_datetime)+covering_spaces, end='\r', flush=True)

    # write clean version of csv to parquet
    parquet_name = f'{base}-{quote}.parquet'
    full_path = f'compressed/{parquet_name}'
    df = pd.concat(batches, ignore_index=True)
    df = pp.quick_clean(df)

    pp.write_raw_to_parquet(df, full_path)

    # in the case that new data was gathered write it to disk
    if len(batches) > 1:
        df.to_csv(f'data/{base}-{quote}.csv', index=False)
        return len(df.index) - old_lines
    return 0


def main():
    """Main loop; loop over all currency pairs that exist on the exchange. Once done upload the
    compressed (Parquet) dataset to Kaggle.
    """
    global category
    if len(sys.argv) > 1:
        category = sys.argv[1]

    # get all pairs currently available
    all_symbols = pd.DataFrame(requests.get(f'{API_BASE}market/instruments-info?category=' + category).json()['result']['list'])
    all_symbols = all_symbols[all_symbols['quoteCoin'] == 'USDT']
    blacklist = ['EUR', 'GBP', 'AUD', 'BCHABC', 'BCHSV', 'DAI', 'PAX', 'WBTC', 'BUSD', 'TUSD', 'UST', 'USDC', 'USDSB', 'USDS', 'SUSD', 'USDP']
    for coin in blacklist:
        all_symbols = all_symbols[all_symbols['baseCoin'] != coin]
    all_pairs = [tuple(x) for x in all_symbols[['baseCoin', 'quoteCoin']].to_records(index=False)]
    
    # sort reverse alphabetical, to ensure USDT pairings are updated first
    all_pairs.sort(key=lambda x:x[1], reverse=True)
    filtered_pairs = []
    for pair in all_pairs:
        if pair[0][-2:] == '2L' or pair[0][-2:] == '3L' or pair[0][-2:] == '2S' or pair[0][-2:] == '3S':
            print("Skipping", pair[0])
        else:
            filtered_pairs.append(pair)

    # randomising order helps during testing and doesn't make any difference in production
    #random.shuffle(filtered_pairs)

    # make sure data folders exist
    os.makedirs('data', exist_ok=True)
    os.makedirs('compressed', exist_ok=True)

    # do a full update on all pairs
    n_count = len(filtered_pairs)
    for n, pair in enumerate(filtered_pairs, 1):
        base, quote = pair
        new_lines = all_candles_to_csv(base=base, quote=quote)
        if new_lines > 0:
            print(f'{datetime.now()} {n}/{n_count} Wrote {new_lines} new lines to file for {base}-{quote}')
        else:
            print(f'{datetime.now()} {n}/{n_count} Already up to date with {base}-{quote}')


if __name__ == '__main__':
    main()
