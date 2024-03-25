# Request_data
Request instant and historical data from Bloomberg's BQuant and Interactive Brokers.

We make use of the mibian library and the iterpolate function to estimate the options contracts that are not retrieved from Bloomberg BQuant, specially for OTM contracts and long maturities that lack liquidity.

Getting the instant option chain is helpul to calculate the Local Volatility of the contract and to build strategies using current prices. 

The maturity list is manually extracted from the Option Monitor (OMON)...
