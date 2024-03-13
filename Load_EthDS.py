# Sharath Mohan
# KrypcSensor ETL - Extract Script

# Libraries
import os
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from dotenv import load_dotenv
import pandas as pd

import psycopg2
import devart.postgresql


# Secrets
load_dotenv()
POSTGRES_ADDRESS = os.getenv('PG_ADDRESS') 
POSTGRES_PORT = '5439'
POSTGRES_USERNAME = os.getenv('PG_USERNAME') 
POSTGRES_PASSWORD = os.getenv('PG_PASSWORD') 
POSTGRES_DBNAME = os.getenv('PG_DBNAME')

# A long string that contains the necessary Postgres login information
postgres_str = ('postgresql+pg8000://{username}:{password}@{ipaddress}:{port}/{dbname}'
.format(username=POSTGRES_USERNAME,
password=POSTGRES_PASSWORD,
ipaddress=POSTGRES_ADDRESS,
port=POSTGRES_PORT,
dbname=POSTGRES_DBNAME))


# Create the connection
print("Pulling Data from database..\n PGURL: {}".format(postgres_str))
# cnx = create_engine(postgres_str)
cnx = devart.postgresql.connect(
    Server=POSTGRES_ADDRESS,
    Database=POSTGRES_DBNAME,
    UserId=POSTGRES_USERNAME,
    Password=POSTGRES_PASSWORD
)
# cnx = psycopg2.connect(database=POSTGRES_DBNAME,
#                         host=POSTGRES_ADDRESS,
#                         user=POSTGRES_USERNAME,
#                         password=POSTGRES_PASSWORD,
#                         port=POSTGRES_PORT)
df = pd.read_sql_query("""SELECT * FROM public.transaction_stores limit 10;""", con=cnx)
print(df.info())



