import requests as rq
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy import types
from datetime import date
from datetime import timedelta
import pytz


# variables editable
latest_url = "https://api.currencyapi.com/xxx/xxx"
api_key = "xxxxxx"
today = date.today()
utcnow = datetime.now(pytz.timezone("UTC")).strftime("%Y-%m-%d, %H:%M:%S")
yesterday = today - timedelta(days=1)
base_currency = "USD"
target_currency = "CAD,GBP,EUR,HKD"

# getting responses
latest_data = rq.get(
    latest_url,
    params={
        "apikey": api_key,
        "base_currency": base_currency,
        "currencies": target_currency,
    },
)
# writing to the database
sql_engine = create_engine(
    "mysql+pymysql://xxxx:xxxx@xxx.xxx.xx.xxx:xxxx/xxxx"
)

# get current data from sql
historical_data = pd.read_sql(
    f"SELECT * FROM xxxx WHERE `Date` <= '{str(yesterday)}'", sql_engine
)

# turn API request to json
latest_data = latest_data.json()

#
for i in latest_data["data"]:
    latest_data["data"][i] = latest_data["data"][i]["value"]

# extract the date from the datetime string
latest_data["meta"] = latest_data["meta"]["last_updated_at"].split("T")[0]

# extract the currency values into a variable
currency_info = latest_data["data"]

# pull the currency values 1 level up in the dictionary
for key, value in currency_info.items():
    latest_data[key] = value

# insert the date value
latest_data["Date"] = latest_data["meta"]

# delete data and meta key value pairs
del latest_data["data"]
del latest_data["meta"]

# turn that dictionary to DataFrame
latest_df = pd.DataFrame(latest_data, index=[0])

# appending latest data to historical data
exchange_rates_table = pd.concat([historical_data, latest_df], ignore_index=True)

# change the Date column to the appropriate datatype
exchange_rates_table["Date"] = pd.to_datetime(
    exchange_rates_table["Date"], yearfirst=True
)

# drop duplicates
exchange_rates_table = exchange_rates_table.drop_duplicates(subset="Date", keep="last")

# reset the index
exchange_rates_table = exchange_rates_table.reset_index(drop=True)

# adding two dummy rows for next two days
added_row = [exchange_rates_table.loc[len(exchange_rates_table) - 1]]
added_row[0][0] += timedelta(days=1)
added_row2 = [exchange_rates_table.loc[len(exchange_rates_table) - 1]]
added_row2[0][0] += timedelta(days=2)

# append added rows to the main table
added_row = pd.DataFrame(added_row)
added_row2 = pd.DataFrame(added_row2)
exchange_rates_table = pd.concat(
    [exchange_rates_table, added_row, added_row2], ignore_index=True
)

exchange_rates_table.to_sql(
    "xxxx",
    sql_engine,
    if_exists="replace",
    index=False,
    dtype={
        "Date": types.DATE,
        "CAD": types.DECIMAL(precision=30, scale=5),
        "EUR": types.DECIMAL(precision=30, scale=5),
        "GBP": types.DECIMAL(precision=30, scale=5),
        "HKD": types.DECIMAL(precision=30, scale=5),
    },
)

sql_engine.dispose()

try:
    rq.get("https://hc-ping.com/xxxx-xxxx-xxxx-xxxx-xxxx", timeout=10)
except rq.RequestException as e:
    # Log ping failure here...
    print("Ping failed: %s" % e)
