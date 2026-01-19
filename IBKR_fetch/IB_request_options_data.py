# -*- coding: utf-8 -*-
"""
Created on Sat Feb 10 15:16:48 2024

@author: Santiago

NOTE: The code is not working well, some adjustments should be made, specially when defining days to expiration. 
Check the BB req data code

"""
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract

import pandas as pd
import threading
import time
import mibian as mb
import datetime as dt
import numpy as np


class TradeApp(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.hist_data = pd.DataFrame(columns=['date', 'umid'])
        self.impvol_data = pd.DataFrame(columns=['date', 'umid'])
        
    def historicalData(self, reqId, bar):
        row = {'date' : bar.date, 'umid' : bar.close}
        if reqId == 0:
            self.hist_data = pd.concat([self.hist_data, pd.DataFrame.from_dict([row])],
                                               ignore_index=True)
        else:
            self.impvol_data = pd.concat([self.impvol_data, pd.DataFrame.from_dict([row])],
                                               ignore_index=True)
        
def websocket_con():
    app.run()
    
def daycount(df, expiry, i):
    """Calculate days to maturity."""
    dte  = ((dt.datetime.strptime(expiry, '%Y-%m-%d').date() - dt.datetime.strptime(df.date[i],'%Y-%m-%d %H:%M:%S').date()).days)
    if dte == 0:
        dte = 1  
        
    return int(dte)

def get_strikes(df, secType, otm_contracts = False):
    """otm_contracts: specify how many contracts from the current spot(ATM). Default: 0.
    #Option chain's steps: '1' for stocks ('STK'), '5' for Index ('IND')."""
    if otm_contracts:
        strikes = np.arange(df.umid.min() - otm_contracts, df.umid.max()+otm_contracts, np.where(secType == 'STK', 1, 5))
        strikes = np.round(strikes)
    else:
        strikes = np.arange(df.umid.min() - 0, df.umid.max()+0, np.where(secType == 'STK', 1, 5))
        strikes = np.round(strikes)
    
    return strikes

def get_expiries(df):
    _df = pd.to_datetime(df['date']).copy(deep=True)
    df['day'] = _df.dt.day_name()
    #fridays = df.loc[df.day == 'Friday', 'date']
    #Convert format date received (Epoch Date) to day & time date
    #expiries = [dt.datetime.strptime(fridays.values[x], '%Y-%m-%d %H:%M:%S') for x in range(0, len(fridays))]
    expiries = [dt.datetime.strptime(df.date.values[x], '%Y-%m-%d %H:%M:%S') for x in range(0, len(df.date))]
    #Convert datetime object to string and get rid of time info
    expiries = np.array([expiries[x].strftime('%Y-%m-%d') for x in range(0, len(expiries))])
    expiries = np.unique(expiries)
    
    return expiries
    
app = TradeApp()
app.connect(host = '127.0.0.1', port = 4002, clientId = 1)
con_thread = threading.Thread(target=websocket_con, daemon=True)
con_thread.start()
time.sleep(1)

contract = Contract()
contract.symbol = 'SPY'
contract.secType = 'STK'
contract.currency = 'USD'
contract.exchange = 'SMART'

app.reqHistoricalData(reqId = 0, contract = contract, 
                      endDateTime = '', durationStr = '1 M',
                      barSizeSetting = '1 hour', 
                      whatToShow = 'ADJUSTED_LAST',
                      useRTH = 1, formatDate = 2, 
                      keepUpToDate = 0, chartOptions= [])
time.sleep(5)

app.reqHistoricalData(reqId = 1, contract = contract, 
                      endDateTime = '', durationStr = '1 M',
                      barSizeSetting = '1 hour', 
                      whatToShow = 'OPTION_IMPLIED_VOLATILITY',
                      useRTH = 1, formatDate = 2, 
                      keepUpToDate = 0, chartOptions= [])
time.sleep(5)

data = app.hist_data
data['date'] = [time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(x))) for x in data['date']]

impVolData = app.impvol_data
impVolData['date'] = [time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(x))) for x in impVolData['date']]
impVolData = impVolData.rename(columns={'umid':'iv'})

strikes = get_strikes(data, 'STK', 10)
expiries = get_expiries(data)#np.unique([dt.datetime.strptime(data.date[_], '%Y-%m-%d %H:%M:%S').date() for _ in range(0, len(data.date))])
put_call = ['C', 'P']
data = pd.concat([data, impVolData['iv']], axis = 1)
data['c'] = 0
data['delta'] = 0
data['gamma'] = 0
#data['vega'] = 0
data['symbol'] = np.nan
rate = 0.05
data['dte'] = 0
for _i in range(0, len(data.index)):
   data.loc[_i, 'dte'] = (dt.datetime.strptime(data.loc[_i, 'expiry'], '%m/%d/%y').date() - data.loc[_i, 'date']).days

df_new = pd.DataFrame(np.repeat(data.values,0, axis=0))
df_new.columns = data.columns

for contract in put_call:
    for K in strikes:
        for x in range(0, len(expiries)):
            for i, r in data.iterrows():
                """
                while r['dte'] == 0:
                    try:
                        r['dte'] = 0.5
                    except ZeroDivisionError:
                        print('float div by zero')
                """
                bs = mb.BS([r['umid'], K, rate, 0.5], volatility = r['iv']*100) #Adjustment for 0DTE
                if contract == 'C':
                    data['c'].at[i] = bs.callPrice
                    data['delta'].at[i] = bs.callDelta
                    data['gamma'].at[i] = bs.gamma
                    data['symbol'].at[i] = 'C-'+str(int(K))+'-'+str(expiries[x])
                    
                else:
                    data['c'].at[i] = bs.putPrice
                    data['delta'].at[i] = bs.putDelta  
                    data['gamma'].at[i] = bs.gamma
                    data['symbol'].at[i] = 'P-'+str(int(K))+'-'+str(expiries[x])
            
            df_new = pd.concat([df_new, data], axis = 0)
            
       
df_new = df_new[df_new['symbol'].notna()]
df_new.set_index('date', inplace=True)
df_new.to_csv('C:/Users/Santiago/OneDrive - Lyras Financial/SyncFiles/BACKTESTING/spy_6m_hourly.csv')    
    
       