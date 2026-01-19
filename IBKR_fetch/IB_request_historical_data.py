# -*- coding: utf-8 -*-
"""
Created on Tue Jul 22 12:54:59 2025

@author: sgarcia
"""

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
import threading
import time
import pandas as pd  


class TradingApp(EWrapper,EClient):
    def __init__(self):
        EClient.__init__(self,self)
        self.data = {}
        self.end_flags = {}

    def historicalData(self, reqId, bar):
        if reqId not in self.data:
            self.data[reqId] = []
        self.data[reqId].append({"date":bar.date,"px":bar.close,"volume":bar.volume}) 
        
    def historicalDataEnd(self, reqId, start, end):
        if reqId in self.end_flags:
            self.end_flags[reqId].set() 
            
            
def websocket_con():
    
    app = TradingApp()
    app.connect("127.0.0.1",4002,7)
    con_thread = threading.Thread(target= app.run, daemon = True)  
    con_thread.start()
    time.sleep(1)

    return app



def usTechStk(symbol, sec_type = 'STK',currency='USD',exchange='SMART'):

    contract = Contract()
    contract.symbol = symbol
    contract.secType = sec_type
    contract.currency = currency
    contract.exchange = exchange
    
    return contract


def histData(app, req_num,contract,duration,candle_size):
    
    app.end_flags[req_num] = threading.Event()

    app.reqHistoricalData(
        reqId = req_num,
        contract = contract, 
        endDateTime = '', 
        durationStr = duration, 
        barSizeSetting = candle_size, 
        whatToShow = 'ADJUSTED_LAST', 
        useRTH = 1, 
        formatDate = 2, 
        keepUpToDate = 0, 
        chartOptions = []
        )
    
    app.end_flags[req_num].wait()
    
    df = pd.DataFrame(app.data[req_num])
    df['date'] = [time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(x))) for x in df['date']]
    df.set_index("date",inplace=True)
    df.index =  pd.to_datetime(df.index)
    
    return df

def dataDataframe(tradeapp_obj,tickers):
    df_dict = {}
    for ticker in tickers:
        df_dict[ticker] = pd.DataFrame(tradeapp_obj.data[tickers.index(ticker)]).drop_duplicates(subset='date')
        df_dict[ticker].set_index("date",inplace=True)
        df_dict[ticker].index =  pd.to_datetime(df_dict[ticker].index, unit='s')
    return df_dict




