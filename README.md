# MutualFundProject
Data Collection/Scraping,Data Manipulation and Real time Data Integration 

### MutualFundProject
The main noteebok MutualFundProject is a pipeline to process the Histroical Mutual fund price data in different CSV file.
-->It extracts the folders from zip/rar file.
-->Combines all the excel to single CSV
-->Calculates different Technical and Statistical Values
-->And Saves this CSV

### MutualfundDownstream
This file fetchs the latest price and fetches historic data from SQL server, calculates the Statistical values and inserts to SQL DB
-->Fetches data from Kite URL endpoint https://api.kite.trade/mf/instruments

