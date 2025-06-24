#!/usr/bin/env python
# coding: utf-8

import json
import os
import asyncio
import aiohttp
import sys
import time
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import preprocessing as pp

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

async def async_get_batch(session, symbol, category, interval='1', start_time=0, end_time=0, limit=1000, semaphore=None):
    """Use an async GET request to retrieve a batch of candlesticks from start_time to end_time.
    Process the JSON into a pandas dataframe and return it. If not successful, return an empty dataframe.
    """
    params = {
        'category': category,
        'symbol': symbol,
        'interval': interval,
        'start': start_time,
        'end': end_time,
        'limit': limit
    }
    async with semaphore:
        try:
            async with session.get(f'{API_BASE}market/kline', params=params, timeout=30) as response:
                if response.status == 200:
                    data = (await response.json())['result']['list']
                    data.reverse()
                    df = pd.DataFrame(data, columns=LABELS)
                    df['open_time'] = df['open_time'].astype(np.int64)
                    return df
                else:
                    print(f'Got erroneous response back for {symbol} ({category}): {response.status}')
                    return pd.DataFrame([])
        except (aiohttp.ClientConnectionError, aiohttp.ClientConnectorError):
            print(f'Connection error for {symbol} ({category}), Cooling down for 5 mins...')
            await asyncio.sleep(5 * 60)
            return await async_get_batch(session, symbol, category, interval, start_time, end_time, limit, semaphore)
        except asyncio.TimeoutError:
            print(f'Timeout for {symbol} ({category}), Cooling down for 5 mins...')
            await asyncio.sleep(5 * 60)
            return await async_get_batch(session, symbol, category, interval, start_time, end_time, limit, semaphore)
        except Exception as e:
            print(f'Unknown error for {symbol} ({category}): {e}, Cooling down for 5 mins...')
            await asyncio.sleep(5 * 60)
            return await async_get_batch(session, symbol, category, interval, start_time, end_time, limit, semaphore)

async def all_candles_to_csv(base, quote, category, interval='1', semaphore=None, session=None):
    """Collect klines starting from current timestamp, moving backwards until no klines are returned.
    Concat into a dataframe and write to CSV and Parquet.
    """
    symbol = base + quote
    now = int(datetime.now().timestamp() * 1000)
    end_time = now
    start_time = end_time - (1000 * 60 * 1000)
    batches = []

    try:
        existing_df = pd.read_csv(f'data/{base}-{quote}.csv')
        old_lines = len(existing_df.index)
    except FileNotFoundError:
        existing_df = pd.DataFrame([], columns=LABELS)
        old_lines = 0

    while True:
        new_batch = await async_get_batch(
            session=session,
            symbol=symbol,
            category=category,
            interval=interval,
            start_time=start_time,
            end_time=end_time,
            limit=1000,
            semaphore=semaphore
        )

        if new_batch.empty:
            break

        batches.append(new_batch)
        end_time = start_time - 1
        start_time = start_time - (1000 * 60 * 1000)

        last_datetime = datetime.fromtimestamp(new_batch['open_time'].iloc[-1] / 1000)
        covering_spaces = 20 * ' '
        print(f'{datetime.now()} {base} {quote} {category} {interval}min {str(last_datetime)}{covering_spaces}', end='\r', flush=True)

    if not batches:
        print(f"{datetime.now()} No new klines for {base}-{quote} ({category})")
        return 0

    new_df = pd.concat(batches, ignore_index=True)
    df = pd.concat([existing_df, new_df], ignore_index=True)
    df = pp.quick_clean(df)
    df = df.drop_duplicates(subset=['open_time'], keep='last')
    df = df.sort_values(by='open_time').reset_index(drop=True)

    parquet_name = f'{base}-{quote}.parquet'
    full_path = f'compressed/{parquet_name}'
    pp.write_raw_to_parquet(df, full_path)
    df.to_csv(f'data/{category}_{base}{quote}.csv', index=False)

    new_lines = len(df.index) - old_lines
    return new_lines

async def process_symbol_pair(pair, semaphore, session):
    """Process a single symbol pair and return the number of new lines written."""
    base, quote, category = pair
    new_lines = await all_candles_to_csv(base=base, quote=quote, category=category, semaphore=semaphore, session=session)
    return base, quote, category, new_lines

async def check_symbol_status(session, symbol, category, semaphore):
    """Check if a symbol is tradeable by fetching its instrument info."""
    params = {'category': category, 'symbol': symbol}
    async with semaphore:
        try:
            async with session.get(f'{API_BASE}market/instruments-info', params=params) as response:
                if response.status == 200:
                    data = (await response.json())['result']['list']
                    if data and 'status' in data[0]:
                        return data[0]['status'] == 'Trading'
                return False
        except Exception as e:
            print(f"Error checking status for {symbol} ({category}): {e}")
            return False

async def main():
    """Main loop; fetch spot and linear symbols, merge, and process concurrently, 10 at a time."""
    async with aiohttp.ClientSession() as session:
        symbols = []
        for cat in ['spot', 'linear']:
            async with session.get(f'{API_BASE}market/instruments-info?category={cat}') as response:
                if response.status == 200:
                    data = (await response.json())['result']['list']
                    df = pd.DataFrame(data)
                    df['category'] = cat
                    symbols.append(df)
                else:
                    print(f"Failed to fetch {cat} symbols: {response.status}")
                    return
        
        all_symbols = pd.concat(symbols, ignore_index=True)
        all_symbols = all_symbols[all_symbols['quoteCoin'].isin(['USDT', 'USDC'])]
        blacklist = ['EUR', 'GBP', 'AUD', 'BCHABC', 'BCHSV', 'DAI', 'PAX', 'WBTC', 'BUSD', 'TUSD', 'UST', 'USDC', 'USDSB', 'USDS', 'SUSD', 'USDP']
        for coin in blacklist:
            all_symbols = all_symbols[all_symbols['baseCoin'] != coin]
        #15 20
        filtered_pairs = []
        pairs = 5
        semaphore = asyncio.Semaphore(pairs)
        lst = os.listdir("./data")
        already_done = []
        for i in lst:
            print(i)
            cpy = i
            already_done.append(cpy.split("_")[1].replace(".csv",""))


        all_symbols = all_symbols[
            ~all_symbols.apply(lambda x: x['baseCoin'] + x['quoteCoin'], axis=1).isin(already_done)
        ]

        for _, row in all_symbols.iterrows():
            symbol = row['baseCoin'] + row['quoteCoin']
            if row['baseCoin'][-2:] in ['2L', '3L', '2S', '3S']:
                print(f"Skipping {row['baseCoin']} ({row['category']})")
                continue
            if await check_symbol_status(session, symbol, row['category'], semaphore):
                filtered_pairs.append((row['baseCoin'], row['quoteCoin'], row['category']))
            else:
                print(f"Skipping {symbol} ({row['category']}) due to inactive status")

        filtered_pairs.sort(key=lambda x: x[1], reverse=True)

        os.makedirs('data', exist_ok=True)
        os.makedirs('compressed', exist_ok=True)

        async with aiohttp.ClientSession() as session:
            n_count = len(filtered_pairs)
            for i in range(0, len(filtered_pairs), pairs):
                batch = filtered_pairs[i:i+pairs]
                tasks = [process_symbol_pair(pair, semaphore, session) for pair in batch]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for j, result in enumerate(results):
                    if isinstance(result, Exception):
                        print(f"Error processing pair {batch[j]}: {result}")
                    else:
                        base, quote, category, new_lines = result
                        if new_lines > 0:
                            print(f"{datetime.now()} {i+j+1}/{n_count} Wrote {new_lines} new lines for {base}-{quote} ({category})")
                        else:
                            print(f"{datetime.now()} {i+j+1}/{n_count} Already up to date with {base}-{quote} ({category})")

if __name__ == '__main__':
    asyncio.run(main())