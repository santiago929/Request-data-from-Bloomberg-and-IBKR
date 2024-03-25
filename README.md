# Request_data
Request instant and historical data from Bloomberg's BQuant and Interactive Brokers.

Bloomberg BQuant <BQNT>

-We make use of the mibian library and the iterpolate function to estimate the options contracts that are not retrieved from Bloomberg BQuant, specially for OTM contracts and long maturities that lack liquidity.
-Getting the instant option chain is helpul to calculate the Local Volatility of the contract and to build strategies using current prices. 
-The maturity list is manually extracted from the Option Monitor (OMON) to obtain the regular contracts, although this process could be optimized using an OOP as we do with get_expiries() in the IB_req_hist_options_data.py file.
