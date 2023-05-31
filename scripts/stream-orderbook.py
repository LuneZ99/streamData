import argparse
import asyncio
import gzip
import os
import time
from collections import defaultdict, deque
from datetime import datetime
from pathlib import Path
from pprint import pprint

import ujson as json
import yaml
from cryptofeed import FeedHandler
from cryptofeed.defines import (
    L2_BOOK,
    TRADES,
    LIQUIDATIONS,
    TICKER,
    # ORDER_INFO,
    # FUNDING
)
from cryptofeed.exchanges import BinanceFutures

# parser = argparse.ArgumentParser(description='Stream orderbook data from Quest')
# parser.add_argument('--config', type=str, default='stream-orderbook-config.yaml',
#                     help='Path to the YAML configuration file')
#
# args = parser.parse_args()
#
# with open(args.config, 'r') as config_file:
#     config = yaml.safe_load(config_file)
#
# dry_run = config.get('dry_run', True)
# output_dir = config.get('output_dir', "/mnt/hdd-2/stream_orderbook/collected/")
# timeit = config.get('timeit', True)

parser = argparse.ArgumentParser(
    description='Stream orderbook data from Quest')
parser.add_argument('--dry-run', action='store_true',
                    help='Print the stream to stdout')
parser.add_argument('--output-dir', type=str, default=None)
parser.add_argument('--timeit', action='store_true')

args = parser.parse_args()

FILE_HANDLE = {}  # Symbol -> (Filepath, Filehandle)
PREV_SECTION_ID = None

QUEUE_LOCK = defaultdict(asyncio.Lock)
TRADES_QUEUE = defaultdict(deque)
LIQUIDATIONS_QUEUE = defaultdict(deque)
# FUNDS_QUEUE = defaultdict(deque)
# ORDERS_QUEUE = defaultdict(deque)
TICKS_QUEUE = defaultdict(deque)

_TIMESTAMP_IDX = 0
_DATA_IDX = 1
_TRADE_TIMEOUT = 0.3  # s


def async_timeit(func):
    """
    https://gist.github.com/Integralist/77d73b2380e4645b564c28c53fae71fb
    """

    async def process(func, *args, **params):
        if asyncio.iscoroutinefunction(func):
            # print('this function is a coroutine: {}'.format(func.__name__))
            return await func(*args, **params)
        else:
            print('this is not a coroutine')
            return func(*args, **params)

    async def helper(*args, **params):
        ts = time.time()
        result = await process(func, *args, **params)
        te = time.time()
        time_ms = (te - ts) * 1000
        print(f"(async)\t{func.__name__: <32}\t>>> {time_ms:.4f} ms\t@ {int(te * 1000)} ms")
        return result

    return helper


def conditional_decorator(dec, condition):
    def decorator(func):
        if not condition:
            # Return the function unchanged, not decorated.
            return func
        return dec(func)

    return decorator


def update_file_handle(symbol, book_timestamp):
    global FILE_HANDLE

    # Build directory to save
    save_dir = Path(os.path.join(args.output_dir, symbol))
    save_dir.mkdir(exist_ok=True, parents=True)

    # Calculate filepath for today
    cur_time = datetime.utcfromtimestamp(book_timestamp)
    section_id = cur_time.hour // 8
    filename = cur_time.strftime("%Y%m%d") + f"_{section_id:02d}.jsonl.gz"  # with compression
    save_path = os.path.join(save_dir, filename)

    # Build file handle for symbol if necessary
    if symbol in FILE_HANDLE and FILE_HANDLE[symbol]["path"] == save_path:
        return
    else:
        # Close old file
        if symbol in FILE_HANDLE:
            FILE_HANDLE[symbol]["handle"].close()
        # Open new file
        # TODO: potentially compress the old file
        FILE_HANDLE[symbol] = {
            "path": save_path,
            "handle": gzip.open(save_path, 'at')
        }


@conditional_decorator(async_timeit, args.timeit)
async def save_book(book, receipt_timestamp):
    book_dict = book.to_dict(numeric_type=str)
    symbol = book_dict["symbol"]

    # aggregate other info
    book_ts = book_dict["timestamp"]  # in seconds

    async with QUEUE_LOCK[symbol]:

        trades_for_cur_book = []
        n_timeout_trades = 0
        while len(TRADES_QUEUE[symbol]) > 0:
            top_queue_ts_diff = book_ts - TRADES_QUEUE[symbol][0][_TIMESTAMP_IDX]
            if top_queue_ts_diff > _TRADE_TIMEOUT:
                TRADES_QUEUE[symbol].popleft()  # remove too old trades
                n_timeout_trades += 1
            elif top_queue_ts_diff >= 0:
                trades_for_cur_book.append(TRADES_QUEUE[symbol].popleft()[_DATA_IDX])
        book_dict["trades"] = trades_for_cur_book

        liquidations_for_cur_book = []
        while len(LIQUIDATIONS_QUEUE[symbol]) > 0:
            top_queue_ts_diff = book_ts - LIQUIDATIONS_QUEUE[symbol][0][_TIMESTAMP_IDX]
            if top_queue_ts_diff >= 0:
                liquidations_for_cur_book.append(LIQUIDATIONS_QUEUE[symbol].popleft()[_DATA_IDX])
        book_dict["liquidations"] = liquidations_for_cur_book

        ticks_for_cur_book = []
        while len(TICKS_QUEUE[symbol]) > 0:
            top_queue_ts_diff = book_ts - TICKS_QUEUE[symbol][0][_TIMESTAMP_IDX]
            if top_queue_ts_diff >= 0:
                ticks_for_cur_book.append(TICKS_QUEUE[symbol].popleft()[_DATA_IDX])
        book_dict["ticks"] = ticks_for_cur_book

    if not args.dry_run:
        update_file_handle(symbol, book_ts)
        # Write to file
        # async with aiofiles.open(FILE_HANDLE[symbol]["handle"], closefd=False) as f:
        # NOTE: we are writing in sync (blocking) mode to reuse file handle - fix this later
        FILE_HANDLE[symbol]["handle"].write(json.dumps(book_dict) + "\n")
    else:
        pass

    cur_timestamp_ms = datetime.now().timestamp() * 1000
    book_timestamp_ms = book.timestamp * 1000
    receipt_timestamp_ms = receipt_timestamp * 1000
    recv_delay = receipt_timestamp_ms - book_timestamp_ms
    tot_delay = cur_timestamp_ms - book_timestamp_ms
    print(
        f"* update book: {book.symbol} @ book time {book_timestamp_ms:.3f} ms "
        f" processed {len(trades_for_cur_book):<3} trades (timeout {n_timeout_trades}),"
        f" {len(liquidations_for_cur_book)} liquidations,"
        f" {len(ticks_for_cur_book)} ticks "
        f"(recv-delay {recv_delay:.3f} ms, tot-delay {tot_delay:.3f} ms) "
        f"(queue length: {len(TRADES_QUEUE[symbol])} {len(LIQUIDATIONS_QUEUE[symbol])} "
        f"{len(TICKS_QUEUE[symbol])} )"
    )


@conditional_decorator(async_timeit, args.timeit)
async def process_trades(trades, receipt_timestamp):
    # https://github.com/bmoscon/cryptofeed/blob/e54ea1ac8995fc031455962da9926abd1a083af7/cryptofeed/exchanges/binance.py#L171
    raw_resp: dict = trades.raw
    symbol = trades.symbol
    async with QUEUE_LOCK[symbol]:
        TRADES_QUEUE[symbol].append((trades.timestamp, raw_resp))


@conditional_decorator(async_timeit, args.timeit)
async def process_liquidations(liquidations, receipt_timestamp):
    # https://github.com/bmoscon/cryptofeed/blob/e54ea1ac8995fc031455962da9926abd1a083af7/cryptofeed/exchanges/binance.py#L221
    raw_resp: dict = liquidations.raw
    symbol = liquidations.symbol
    async with QUEUE_LOCK[symbol]:
        LIQUIDATIONS_QUEUE[symbol].append((liquidations.timestamp, raw_resp))


# @conditional_decorator(async_timeit, timeit)
# async def process_funds(funds, receipt_timestamp):
#     raw_resp: dict = funds.raw
#     symbol = funds.symbol
#     async with QUEUE_LOCK[symbol]:
#         FUNDS_QUEUE[symbol].append((funds.timestamp, raw_resp))
#
#
# @conditional_decorator(async_timeit, timeit)
# async def process_orders(orders, receipt_timestamp):
#     raw_resp: dict = orders.raw
#     symbol = orders.symbol
#     async with QUEUE_LOCK[symbol]:
#         ORDERS_QUEUE[symbol].append((orders.timestamp, raw_resp))


@conditional_decorator(async_timeit, args.timeit)
async def process_ticks(ticks, receipt_timestamp):
    raw_resp: dict = ticks.raw
    symbol = ticks.symbol
    async with QUEUE_LOCK[symbol]:
        TICKS_QUEUE[symbol].append((ticks.timestamp, raw_resp))


def main():
    pprint(vars(args))

    if not args.dry_run and args.output_dir is None:
        print("Must specify output directory!")
        return

    f = FeedHandler()
    # f.add_feed(BinanceFutures(
    #     symbols=[
    #         "BTC-USDT-PERP",
    #     ],
    #     channels=[
    #         TRADES,
    #         LIQUIDATIONS,
    #         L2_BOOK,
    #         TICKER
    #     ],
    #     callbacks={
    #         TRADES: process_trades,
    #         LIQUIDATIONS: process_liquidations,
    #         L2_BOOK: save_book,
    #     },
    #     retries=-1,
    #     max_depth=100
    # ))

    f.add_feed(BinanceFutures(
        symbols=[
            "ETH-USDT-PERP"
        ],
        channels=[
            TRADES,
            LIQUIDATIONS,
            L2_BOOK,
            TICKER,
            # FUNDING,
            # ORDER_INFO
        ],
        callbacks={
            TRADES: process_trades,
            LIQUIDATIONS: process_liquidations,
            TICKER: process_ticks,
            # FUNDING: process_funds,
            # ORDER_INFO: process_orders,
            L2_BOOK: save_book,
        },
        retries=-1,
        max_depth=100
    ))

    f.run()


if __name__ == '__main__':
    main()
