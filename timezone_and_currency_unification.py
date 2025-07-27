import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from google.cloud import bigquery
from google.oauth2 import service_account
import pendulum
import smtplib
from email.message import EmailMessage
import ssl
import time
import requests

start_time = time.time()


# ssl context
context = ssl.create_default_context()

# Momomedia DB credentials
sql_engine = create_engine(
    "mysql+pymysql://xxxxxxx:xxxxxx@xxx.xxx.xxx.xxx:xxx/xxxxxx"
)

# Big query constants
gbq_creds = service_account.Credentials.from_service_account_file(
    "./bigquery-jobrunner-key.json",
)
gbq_proj = "xxxx-xxxxx"
gbq_dataset = "xxxx"
gbq_table = "xxxxx"  # the destination table for this script

# Big query database objects
client = bigquery.Client(gbq_proj, credentials=gbq_creds)
dataset = client.dataset(gbq_dataset)
table_ref = dataset.table(gbq_table)

# Grab coupler data from Momo GBQ
adsetdata = pd.read_gbq(
    "SELECT * FROM `xxxx-xxxx.xxxx.xxxx`",
    credentials=gbq_creds,
)

# Clean up the headers and datatypes
adsetdata.columns = adsetdata.columns.str.lower()

# change the data types for data manipulation
adsetdata = adsetdata.astype(
    {
        "date_start": "str",
        "date_stop": "str",
        "account_id": "str",
        "campaign_name": "str",
        "ad_set_id": "str",
        "ad_set_name": "str",
    }
)

# order values by date ascending
adsetdata = adsetdata.sort_values(by=["date_start"])
adsetdata = adsetdata.rename(
    columns={"hourly_stats_aggregated_by_advertiser_time_zone": "hour"}
)

# get the first time value of the hour info and concatenate it with the date to get datetime
adsetdata["hour"] = adsetdata["hour"].str.split(" - ").str[0]
adsetdata["source_datetime"] = adsetdata["date_start"] + "T" + adsetdata["hour"]

# get the timezone info
account_timezones = pd.read_sql(
    "SELECT adaccount_id, timezone FROM xxxx", sql_engine
)
account_timezones["timezone"] = account_timezones["timezone"].str.rsplit(" ").str[-1]

# join the tables
adsetdata = adsetdata.merge(
    right=account_timezones, how="left", left_on="account_id", right_on="adaccount_id"
)

# delete the ad account id from the timezone table
adsetdata = adsetdata.drop("adaccount_id", axis=1)

# set error variables for email notification
errorvariable = False
errortimezones = []

# localize the timezones using pendulum
for i in adsetdata.index:
    try:
        adsetdata.loc[i, "source_datetime"] = pendulum.parse(
            adsetdata.loc[i, "source_datetime"], tz=adsetdata.loc[i, "timezone"]
        )
    except:
        errorvariable = True
        errortimezones.append(adsetdata.loc[i, "timezone"])
        adsetdata.loc[i, "source_datetime"] = pendulum.datetime(1999, 1, 1)
        continue


# email variables
email_sender = "xxxx@gmail.com"
email_pwd = "xxxxx"  # this is an "app password" for google account
email_receiver = "xxxxx@gmail.com"
subject = "Unknown timezone code in Autoloan!!"
message = f"there is an unknown timezone code or blank timezone code in the Autoloan Adset Cost Update Gsheet\n\nWhich is the following:\n[{','.join(errortimezones)}]"

# email module
em = EmailMessage()
em["From"] = email_sender
em["To"] = email_receiver
em["Subject"] = subject
em.set_content(message)

# send the error email if there is an unknown timezone info
if errorvariable == True:
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as smtp:
        smtp.login(email_sender, email_pwd)
        smtp.sendmail(email_sender, email_receiver, em.as_string())
adsetdata["pacific_datetime"] = None

# fill in the pacific datetime column
for i in adsetdata.index:
    adsetdata.loc[i, "pacific_datetime"] = adsetdata.loc[i, "source_datetime"].in_tz(
        "US/Pacific"
    )


# change the timezone aware timestamp info to string so SQL can process it
adsetdata = adsetdata.astype(
    {
        "source_datetime": "str",
        "pacific_datetime": "str",
        "amount_spend": "float64",
    }
)


# define function for checking if a table exists
def if_tbl_exists(client, table_ref):
    from google.cloud.exceptions import NotFound

    try:
        client.get_table(table_ref)
        return True
    except NotFound:
        return False


# check if the table exists in the database, if not then insert the data as is
if not if_tbl_exists(client, table_ref):
    adsetdata.to_gbq(
        f"{gbq_proj}.{gbq_dataset}.{gbq_table}",
        credentials=gbq_creds,
        if_exists="replace",
        progress_bar=True,
        table_schema=[
            {"name": "date_start", "type": "DATE"},
            {"name": "date_stop", "type": "DATE"},
        ],
    )
    print("table not found, inserting data as is")
    end_time = time.time()
    print(end_time - start_time)
    exit()
else:
    print("table is found, continuing the data transformation")


# grab the historical dataset
gbq_df = pd.read_gbq(
    "SELECT * FROM `xxxx-xxxx.xxxx.xxxx`",
    credentials=gbq_creds,
)

# set the source info
adsetdata["source"] = "1. Coupler"
gbq_df["source"] = "2. GBQ"

# change the datatype of the dates to string
gbq_df = gbq_df.astype({"date_start": "str", "date_stop": "str"})

# combine the table
merged_table = pd.concat([adsetdata, gbq_df]).astype(
    dtype={
        "date_start": "str",
        "date_stop": "str",
        "source_datetime": "str",
        "amount_spend": "float",
    }
)

# order the table by the source to prioritize coupler source
merged_table = merged_table.sort_values(by="source", ascending=True)

# only keep unique datetime + campaign id + ad set id
merged_table = merged_table.drop_duplicates(
    subset=["source_datetime", "account_id", "ad_set_id"], keep="first"
)

# only keep unique datetime + campaign name + ad set name
# merged_table = merged_table.drop_duplicates(
#     subset=["source_datetime", "campaign_name", "ad_set_id"], keep="first"
# )

# sort the values by datetime and reset index
merged_table = merged_table.sort_values(by=["source_datetime"]).reset_index(drop=True)

# delete the source column
merged_table = merged_table.drop("source", axis=1)

# insert the merged table to gbq
merged_table.to_gbq(
    f"{gbq_proj}.{gbq_dataset}.{gbq_table}",
    credentials=gbq_creds,
    if_exists="replace",
    progress_bar=True,
    table_schema=[
        {"name": "date_start", "type": "DATE"},
        {"name": "date_stop", "type": "DATE"},
    ],
)

# ping health checks
try:
    requests.get("https://hc-ping.com/xxxxx-xxxxx-xxxxx-xxxxx-xxxxx", timeout=10)
except requests.RequestException as e:
    # Log ping failure here...
    print("Ping failed: %s" % e)

# dispose sql engine
sql_engine.dispose()

end_time = time.time()
print(end_time - start_time)
