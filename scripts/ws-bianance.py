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

        self.reset_handle()

    def on_close(self):
        if self.handle:
            self.handle.close()

    def process_line(self, info):
        now = datetime.datetime.now()
        utc_now = now.astimezone(pytz.utc)
        utc_date = str(utc_now)[:10]
        if self.date > utc_date:
            self.date = utc_date
            self.reset_handle()
        line = self._process_line(info)
        self.handle.write("\n" + line)

    def reset_handle(self):

        if self.handle:
            self.handle.close()

        self.file_name = f"{self.parent_path}/{self.symbol}/{self.date}/{self.stream}.csv"
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
        return ",".join([
            info['E'],
            info['a'],
            info['p'],
            info['q'],
            info['f'],
            info['l'],
            info['T'],
            info['m']
        ])


