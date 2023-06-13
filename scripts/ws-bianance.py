import os
from typing import Optional, IO

import websocket
import multiprocessing
import pandas as pd
import datetime
import pytz


websocket.enableTrace(True)
proxy_host = "i.tech.corgi.plus"
proxy_port = "7890"


class StreamCsvHandler:
    date: str
    symbol: str
    stream: str
    handle: Optional[IO]
    headers: str
    file_name: str

    def __init__(self, path, symbol, stream):
        self.parent_path = path
        self.symbol = symbol
        self.stream = stream
        self.handle = None
        self.headers = ""
        self.file_name = ""

    def on_start(self):

        now = datetime.datetime.now()
        utc_now = now.astimezone(pytz.utc)
        utc_date = str(utc_now)[:10]
        self.date = utc_date

        self._reset_handle()

    def on_close(self):
        if self.handle:
            self.handle.close()

    def process_line(self, info):
        now = datetime.datetime.now()
        utc_now = now.astimezone(pytz.utc)
        utc_date = str(utc_now)[:10]
        if self.date > utc_date:
            self.date = utc_date
            self._reset_handle()
        line = self._process_line(info)
        if len(line) > 0:
            self.handle.write("\n" + ",".join(map(str, line)))

    def _reset_handle(self):

        if self.handle:
            self.handle.close()

        self.file_name = f"{self.parent_path}/{self.symbol}/{self.date}/{self.stream}.csv"

        if not os.path.exists(f"{self.parent_path}/{self.symbol}/{self.date}"):
            os.makedirs(f"{self.parent_path}/{self.symbol}/{self.date}")

        if os.path.exists(self.file_name):
            self.handle = open(self.file_name, "a")
        else:
            self.handle = open(self.file_name, "a")
            self._write_csv_header()

    def _write_csv_header(self):
        self.handle.write(self.headers)

    def _process_line(self, info):
        raise NotImplementedError




# class SnapCsvHandler(StreamCsvHandler):
#
#     def __init__(self, path, date, symbol, stream="snap"):
#         super().__init__(path, date, symbol, stream)
#
#     def write_csv_header(self):
#         header = ""
#         self.handle.write()

class AggTradeHandler(StreamCsvHandler):

    def __init__(self, path, symbol, stream="aggTrade"):
        super().__init__(path, symbol, stream)
        self.headers = "OrigTime,ID,Price,Volume,TradeFirst,TradeLast,TradeTime,isSell"

    def _process_line(self, info):
        return [
            info['E'],
            info['a'],
            info['p'],
            info['q'],
            info['f'],
            info['l'],
            info['T'],
            info['m']
        ]


class KlineHandler(StreamCsvHandler):

    def __init__(self, path, symbol, stream):
        assert "kline" in stream, "This handler is only supported for kline streams"
        super().__init__(path, symbol, stream)
        self.headers = "OrigTime,TimeStart,TimeEnd,TradeFirst,TradeLast,Open,Close,High,Low,Volume,TradeCount,Money,BuyVolume,BuyMoney"

    def _process_line(self, info):

        if info['k']['x']:
            return [
                info['E'],
                info['k']['t'],
                info['k']['T'],
                info['k']['f'],
                info['k']['L'],
                info['k']['o'],
                info['k']['c'],
                info['k']['h'],
                info['k']['l'],
                info['k']['v'],
                info['k']['n'],
                info['k']['q'],
                info['k']['V'],
                info['k']['Q']
            ]

        else:
            return []


class BookTickerHandler(StreamCsvHandler):

    def __init__(self, path, symbol, stream="bookTicker"):
        super().__init__(path, symbol, stream)
        self.headers = "OrigTime,ID,TradeTime,BP1,BV1,SP1,SV1"

    def _process_line(self, info):
        return [
            info['E'],
            info['u'],
            info['T'],
            info['b'],
            info['B'],
            info['a'],
            info['A']
        ]


class ForceOrderHandler(StreamCsvHandler):

    def __init__(self, path, symbol, stream="forceOrder"):
        super().__init__(path, symbol, stream)
        self.headers = "OrigTime,OrderSide,OrderType,TimeInForce,Price,Volume,AvgPrice,OrderStatus,LastTradedVolume,TotalTradedVolume,TradeTime"

    def _process_line(self, info):
        return [
            info['E'],
            info['o']['S'],
            info['o']['o'],
            info['o']['f'],
            info['o']['p'],
            info['o']['q'],
            info['o']['ap'],
            info['o']['X'],
            info['o']['l'],
            info['o']['z'],
            info['o']['T']
        ]


class ForceOrderHandler(StreamCsvHandler):

    def __init__(self, path, symbol, stream="forceOrder"):
        super().__init__(path, symbol, stream)
        self.headers = "OrigTime,OrderSide,OrderType,TimeInForce,Price,Volume,AvgPrice,OrderStatus,LastTradedVolume,TotalTradedVolume,TradeTime"

    def _process_line(self, info):
        return [
            info['E'],
            info['o']['S'],
            info['o']['o'],
            info['o']['f'],
            info['o']['p'],
            info['o']['q'],
            info['o']['ap'],
            info['o']['X'],
            info['o']['l'],
            info['o']['z'],
            info['o']['T']
        ]