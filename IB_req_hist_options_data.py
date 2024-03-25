# -*- coding: utf-8 -*-
"""
Created on Sat Feb 10 15:16:48 2024

@author: Santiago

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
    dte  = ((dt.datetime.strptime(expiry, '%Y-%m-%d') - dt.datetime.strptime(df.date[i],'%Y-%m-%d %H:%M:%S')).days)
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
    fridays = df.loc[df.day == 'Friday', 'date']
    #Convert format date received (Epoch Date) to day & time date
    expiries = [dt.datetime.strptime(fridays.values[x], '%Y-%m-%d %H:%M:%S') for x in range(0, len(fridays))]
    #Convert datetime object to string and get rid of time info
    expiries = np.array([expiries[x].strftime('%Y-%m-%d') for x in range(0, len(expiries))])
    expiries = np.unique(expiries)
    
    return expiries
    
app = TradeApp()
app.connect(host = '127.0.0.1', port = 7496, clientId = 1)
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
                      barSizeSetting = '5 mins', 
                      whatToShow = 'ADJUSTED_LAST',
                      useRTH = 1, formatDate = 2, 
                      keepUpToDate = 0, chartOptions= [])
time.sleep(5)

app.reqHistoricalData(reqId = 1, contract = contract, 
                      endDateTime = '', durationStr = '1 M',
                      barSizeSetting = '5 mins', 
                      whatToShow = 'OPTION_IMPLIED_VOLATILITY',
                      useRTH = 1, formatDate = 2, 
                      keepUpToDate = 0, chartOptions= [])
time.sleep(5)

data = app.hist_data
data['date'] = [time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(x))) for x in data['date']]

impVolData = app.impvol_data
impVolData['date'] = [time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(x))) for x in impVolData['date']]
impVolData.set_index('date', inplace=True)

strikes = get_strikes(data, 'STK')
expiries = get_expiries(data)
put_call = ['C', 'P']
data['iv'] = impVolData['umid']
data['c'] = 0
data['delta'] = 0
data['vega'] = 0
data['symbol'] = np.nan
rate = 0.0535

df_new = pd.DataFrame(np.repeat(data.values,0, axis=0))
df_new.columns = data.columns

for contract in put_call:
    for K in strikes:
        for x in range(0, len(expiries)):
            for i, r in data.iterrows():
                dte = daycount(data, expiries[x], i)
                if dte > 0:
                    bs = mb.BS([r['umid'], K, rate, dte], volatility = r['iv']*100) 
                    if contract == 'C':
                        data['c'].at[i] = bs.callPrice
                        data['delta'].at[i] = bs.callDelta
                        data['vega'].at[i] = bs.vega
                        data['symbol'].at[i] = 'C-'+str(int(K))+'-'+str(expiries[x])
                    else:
                        data['c'].at[i] = bs.putPrice
                        data['delta'].at[i] = bs.putDelta  
                        data['vega'].at[i] = bs.vega
                        data['symbol'].at[i] = 'P-'+str(int(K))+'-'+str(expiries[x])
                else:
                    continue
            df_new = pd.concat([df_new, data], axis = 0)
            
       
df_new = df_new[df_new['symbol'].notna()]
df_new.set_index('date', inplace=True)
df_new.to_csv('path/spy_new.csv')    
    

       
