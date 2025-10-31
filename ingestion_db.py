import pandas as pd
import os
from sqlalchemy import create_engine
import logging
import time

# Ensure logs directory exists
os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    filename="logs/ingestion_db.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

engine = create_engine("sqlite:///inventory.db")

def ingest_db(df, table_name, engine):
    """This function will ingest the dataframe into database table"""
    df.to_sql(table_name, con=engine, if_exists="replace", index=False)

def load_raw_data():
    """This function will load the CSVs as dataframe and ingest into db"""
    start = time.time()
    for file in os.listdir("data"):
        if file.lower().endswith(".csv"):
            table_name = os.path.splitext(file)[0].replace(" ", "_").lower()
            try:
                df = pd.read_csv(os.path.join("data", file))
                logging.info(f"Ingesting {file} into db as {table_name}")
                ingest_db(df, table_name, engine)
            except Exception as e:
                logging.error(f"Failed to ingest {file}: {e}")
    end = time.time()
    total_time = (end - start) / 60
    logging.info("-------Ingestion Complete-------")
    logging.info(f"Total Time Taken: {total_time:.2f} minutes")
