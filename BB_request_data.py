# -*- coding: utf-8 -*-
"""
Created on Fri Mar 28 16:38:01 2025

@author: sgarcia
"""

import blpapi
import pandas as pd

session_options = blpapi.SessionOptions()
session_options.setServerHost('localhost')
session_options.setServerPort(8194)

session = blpapi.Session(session_options)
if not session.start():
    print("Failed to start Bloomberg session.")
    exit()

if not session.openService("//blp/refdata"):
    print("Failed to open //blp/refdata service.")
    exit()

refDataService = session.getService("//blp/refdata")


def index_members(ticker):
    """
    Parameters
    ----------
    ticker : string
        tickers should be inserted as of Bloomberg's format. Example: 'SPX Index'.

    Returns
    -------
    members : list

    """
    
    request = refDataService.createRequest("ReferenceDataRequest")
    request.getElement("securities").appendValue(ticker)
    request.getElement("fields").appendValue("INDX_MEMBERS")
    
    session.sendRequest(request)
    
    members = []
    while True:
        event = session.nextEvent()
        for msg in event:
            if msg.hasElement("securityData"):
                security_data = msg.getElement("securityData")
                for i in range(security_data.numValues()):
                    field_data = security_data.getValue(i).getElement("fieldData")
                    if field_data.hasElement("INDX_MEMBERS"):
                        member_array = field_data.getElement("INDX_MEMBERS")
                        for j in range(member_array.numValues()):
                            member = member_array.getValue(j)
                            ticker = member.getElementAsString("Member Ticker and Exchange Code").split()[0]
                            members.append(ticker)     
                            
        if event.eventType() == blpapi.Event.RESPONSE:
            break
        
    return members


def req_underlying_data(stock_list, exchange, asset_type, start_date, end_date, periodicity):
    """
    Parameters
    ----------
    stock_list : list
        Use the first letters of the ticker, without space. Example: 'AAPL'.
    exchange : string
        US, UN, UW, etc.
    asset_type : string
        Equity, Index, etc.
    start_date : string
        YYYYMMDD.
    end_date : string
        YYYYMMDD.
    periodicity : string
        Example: 'DAILY'.

    Returns
    -------
    underlying_data : Dict[string, pandas.DataFrame]

    """
    
    underlying_data = {}
    for stock in stock_list:
        exchange = exchange if asset_type == 'Equity' else ''
        ticker = f"{stock} {exchange} {asset_type}"
    
        request = refDataService.createRequest("HistoricalDataRequest")
        request.getElement("securities").appendValue(ticker)
        request.getElement("fields").appendValue("PX_LAST")  
        
        request.set("startDate", start_date)  
        request.set("endDate", end_date)   
        request.set("periodicitySelection", periodicity)
    
        session.sendRequest(request)
    
        while True:
            ev = session.nextEvent(500)
            for msg in ev:
                if msg.hasElement("securityData", True):
                    security_data = msg.getElement("securityData")
                    field_data_array = security_data.getElement("fieldData")

                    data = []
                    for i in range(field_data_array.numValues()):
                        field_data = field_data_array.getValueAsElement(i)
                        date = field_data.getElementAsDatetime("date")#.strftime("%Y-%m-%d")
                        px_last = field_data.getElementAsFloat("PX_LAST")
                        data.append([date, px_last])
                    
                    underlying_data[stock] = pd.DataFrame(data, columns=["date", "px"]) 
                    underlying_data[stock]['date'] = pd.to_datetime(underlying_data[stock]['date'])
                    underlying_data[stock].set_index('date', inplace = True)
            if ev.eventType() == blpapi.Event.RESPONSE:
                break
    
    return underlying_data


def req_volume_data(stock_list, exchange, asset_type, start_date, end_date, periodicity):
    """
    Parameters
    ----------
    stock_list : list
        Use the first letters of the ticker, without space. Example: 'AAPL'.
    exchange : string
        US, UN, UW, etc.
    asset_type : string
        Equity, Index, etc.
    start_date : string
        YYYYMMDD.
    end_date : string
        YYYYMMDD.
    periodicity : string
        Example: 'DAILY'.

    Returns
    -------
    underlying_data : Dict[string, pandas.DataFrame]

    """
    
    underlying_data = {}
    for stock in stock_list:
        exchange = exchange if asset_type == 'Equity' else ''
        ticker = f"{stock} {exchange} {asset_type}" 
        
        request = refDataService.createRequest("HistoricalDataRequest")
        request.getElement("securities").appendValue(ticker)
        request.getElement("fields").appendValue("VOLUME")
        
        request.set("startDate", start_date) 
        request.set("endDate", end_date)    
        request.set("periodicitySelection", periodicity)
        
        session.sendRequest(request)
        
        while True:
            ev = session.nextEvent(500)
            for msg in ev:
                if msg.hasElement("securityData"):
                    field_data_array = msg.getElement("securityData").getElement("fieldData")
                    
                    data = []
                    for i in range(field_data_array.numValues()):
                        entry = field_data_array.getValue(i)
                        date = entry.getElementAsDatetime("date")
                        volume = entry.getElementAsInteger("VOLUME") if entry.hasElement("VOLUME") else None
                        data.append([date, volume])
                    
                    underlying_data[stock] = pd.DataFrame(data, columns=["date", "volume"]) 
                    underlying_data[stock]['date'] = pd.to_datetime(underlying_data[stock]['date'])
                    underlying_data[stock].set_index('date', inplace = True)
            if ev.eventType() == blpapi.Event.RESPONSE:
                break
        
    return underlying_data


if __name__ == "__main__":
    index = index_members('SPX Index')
    historical_prices = req_underlying_data(index, 'US', 'Equity', '20260101', '20260105', 'DAILY')
    historical_volume = req_volume_data(index, 'US', 'Equity', '20260101', '20260105', 'DAILY')
