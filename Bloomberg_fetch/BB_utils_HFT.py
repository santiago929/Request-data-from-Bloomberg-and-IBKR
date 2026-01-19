# -*- coding: utf-8 -*-
"""
Created on Mon Sep  9 11:26:42 2024

@author: sgarcia
"""

import datetime as dt
import pandas as pd
import blpapi

session_options = blpapi.SessionOptions()
session_options.setServerHost('localhost')
session_options.setServerPort(8194)

# Create a session to connect to the Bloomberg API
session = blpapi.Session(session_options)
if not session.start():
    print("Failed to start Bloomberg session.")
    exit()

if not session.openService("//blp/refdata"):
    print("Failed to open //blp/refdata service.")
    exit()

refDataService = session.getService("//blp/refdata")


def get_realtime_volume(tickers, exchange, asset_type):
    """
    Parameters
    ----------
    tickers : list
        Use the first letters of the ticker, without space. Example: 'AAPL'.
    exchange : string
        US, UN, UW, etc.
    asset_type : string
        Equity, Index, etc.

    Returns
    -------
    df : pandas.DataFrame

    """
    
    request = refDataService.createRequest("ReferenceDataRequest")

    for t in tickers:
        ticker = f"{t} {exchange} {asset_type}"
        request.getElement("securities").appendValue(ticker)
    request.getElement("fields").appendValue("VOLUME")

    session.sendRequest(request)

    data = []
    while True:
        ev = session.nextEvent()
        for msg in ev:
            if msg.messageType() == blpapi.Name("ReferenceDataResponse"):
                for security_data in msg.getElement("securityData").values():
                    ticker_ = security_data.getElementAsString("security").split(" ")[0]
                    field_data = security_data.getElement("fieldData")
                    volume = field_data.getElementAsInteger("VOLUME") if field_data.hasElement("VOLUME") else None
                    data.append((ticker_, volume))
        if ev.eventType() == blpapi.Event.RESPONSE:
            break

    df = pd.DataFrame(data, columns=["Ticker", "Volume"])
    
    return df



def req_underlying_data(stock_list, exchange, asset_type, start_date, end_date, interval, rspl = False):
    """
    Parameters
    ----------
    stock_list : list
        Just use the first letters of the ticker, without space. For Example: 'AAPL'.
    exchange : string
        US, UN, UW, etc.
    asset_type : string
        Equity, Index, etc.
    start_date : string
        YYYYMMDD.
    end_date : string
        YYYYMMDD.
    interval : int
        Length of the bar defined in minutes. The minimum supported bar size bar is 1 min. 
        The maximum supported bar size is 1,440 minutes, (=24 hours)..
    rspl : bool
        Resample data to a convenient frequency of time series. 

    Returns
    -------
    underlying_data : Dict[string, pandas.DataFrame]

    """
    underlying_data = {}
    for stock in stock_list:
        exchange = exchange if asset_type == 'Equity' else ''
        ticker = f"{stock} {exchange} {asset_type}"
        
        # Create the historical data request
        request = refDataService.createRequest("IntradayBarRequest")
        request.set("security", ticker)
        request.set("eventType","TRADE")
        request.set("startDateTime", dt.datetime.strptime(start_date, '%Y%m%d'))#.replace(hour=14, minute=30)) # just for VIX. Else delete
        request.set("endDateTime", dt.datetime.strptime(end_date, '%Y%m%d')) #
        request.set("interval", interval)
        
        # Send the request
        session.sendRequest(request)
    
        # Process the response
        while True:
            ev = session.nextEvent()
            for msg in ev:
                if msg.hasElement("barData", True):
                    bar_data = msg.getElement("barData").getElement("barTickData")
                    data = []
                    for i in range(bar_data.numValues()):
                        bar = bar_data.getValue(i)
                        data.append({
                            "date": bar.getElementAsDatetime("time"),
                            "px": bar.getElementAsFloat("close"),
                            "volume": bar.getElementAsInteger("volume"),
                            })
                    underlying_data[stock] = pd.DataFrame(data, columns=["date", "px", "volume"])
                    
                    underlying_data[stock] = underlying_data[stock].set_index('date')
                   
                    if rspl:
                        underlying_data[stock] = underlying_data[stock].resample(rspl).agg({
                            'px': 'last',
                            'volume': 'sum'
                            }).dropna()
                    
            if ev.eventType() == blpapi.Event.RESPONSE:
                break
    
    return underlying_data



if __name__ == '__main__':
    tickers = ['AAPL', 'MSFT']
    realtime_volume = get_realtime_volume(tickers, 'US', 'Equity')
    historical_prices = req_underlying_data(tickers, 'US', 'Equity', '20260101', '20260110', interval=1)
