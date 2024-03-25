# Request_data
Request instant and historical data from Bloomberg's BQuant and Interactive Brokers.

Bloomberg BQuant <BQNT>.

- We make use of the mibian library and the interpolate function to estimate the greeks and implied volatility that are not retrieved from Bloomberg BQuant, specially for OTM contracts and long maturities that lack liquidity. 

- Getting the instant option chain is useful to calculate the Local Volatility of the contract and to build strategies using current prices. 

- The maturity list is manually extracted from the Option Monitor (OMON) to obtain the regular contracts (see below), although this process could be optimized using OOP as we do with get_expiries() in the IB_req_hist_options_data.py file.

![image](https://github.com/santiago929/Request_data/assets/78446067/f0486256-fc63-464c-b26a-a4d7f8ae2625)


Interactive Brokers API.

- Get options' historical data from IBKR API with the purpose of backtesting strategies using pyqstrat(https://github.com/abbass2/pyqstrat), and to work in additional projects such as: https://github.com/santiago929/Minimum_variance_delta
   
![image](https://github.com/santiago929/Request_data/assets/78446067/e69dbb65-f5b4-4b83-a5b5-befd560b19f5)

- We copy the same format from pyqstrat, as we find it useful for easy looping through the dataframe without overlapping the information.

- Further improvements are in queue to shorten the execution time, although it is understandable to have delays given the amount of data that we are requesting.
