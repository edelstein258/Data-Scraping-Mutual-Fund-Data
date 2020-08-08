import requests 
import pandas as pd
import logging
from sqlalchemy import create_engine
from sqlalchemy import exc

# configuring the log
logging.basicConfig(filename='downstream.log', format='%(asctime)s : [%(levelname)s] [%(filename)s:%(lineno)d] %(message)s', level=logging.DEBUG)

# api-endpoint 
URL = "https://api.kite.trade/mf/instruments"
  
#######################################################################################
# Data Fetching
#######################################################################################

# get request and saving the response as response object 
r = requests.get(url = URL)
# check if the data was fetched successfully
if(r.status_code != 200):
  logging.error("Data fetch failed - file not found")
  exit()

logging.info("Data fetch successful")

#######################################################################################
# Data Preprocessing
# Two DataFrames - fund_data, fund_price_data
#######################################################################################

df = pd.DataFrame([sub.split(",") for sub in r.text.split('\r\n')])
df.columns = df.iloc[0]
df = df.drop(df.index[0])
df['last_price_date'] = pd.to_datetime(df['last_price_date'])

# using dictionary to convert specific columns 
convert_dict = {'tradingsymbol': str,
                'amc': str,
                'name': str,
                'purchase_allowed': float,
                'redemption_allowed': float,
                'minimum_purchase_amount': float,
                'purchase_amount_multiplier': float,
                'minimum_additional_purchase_amount': float,
                'minimum_redemption_quantity': float,
                'redemption_quantity_multiplier': float,
                'dividend_type': str,
                'scheme_type': str,
                'plan': str,
                'settlement_type': str,
                'last_price': float,
               } 
df = df.astype(convert_dict)

fund_data = df.iloc[:, :14]
fund_price_data = df.iloc[:, 14:]
fund_price_data['tradingsymbol'] = fund_data['tradingsymbol']

#######################################################################################
# Connecting and Writing to Database
#######################################################################################

# creating connection
engine = create_engine('mysql://root:admin@localhost/mutualfund')
# opening connection
try:
    con = engine.connect()
    fund_data.to_sql(name='fund_data',con=con,if_exists='replace', index=False)
    con.close()
except:
    logging.error("fund_data - Connection to Database failed")
    
logging.info("fund_data - successfully written to Database")

# opening connection
try:
    con = engine.connect()
    fund_price_data.to_sql(name='fund_price_data',con=con,if_exists='append', index=False)
    con.close()
except:
    logging.error("fund_price_data - Connection to Database failed")
    
logging.info("fund_price_data - Data successfully written to Database")
