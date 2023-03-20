import logging
import os

import pandas as pd

logger = logging.getLogger(__name__)


def read_stock_data():
    """Read stock data from local location."""

    historical_stock_price = {}

    directory = "input_data/Stocks"
    load_stocks = ["fb.us.txt", "goog.us.txt"]
    for filename in os.listdir(directory):
        if filename in load_stocks:
            if filename == "fb.us.txt":
                fbdata = pd.read_csv(
                    f"{directory}/{filename}", header=0, parse_dates=["Date"]
                )
                # Enrich facebook with the latest data
                latest_data = pd.read_csv("input_data/Facebook_cleaned.csv")
                latest_data["date"] = pd.to_datetime(latest_data["date"])

                facebook_historical = pd.concat([latest_data, fbdata])
                facebook_historical["stock_name"] = "Facebook"
                historical_stock_price["fb"] = facebook_historical

            elif filename == "goog.us.txt":
                ggdata = pd.read_csv(
                    f"{directory}/{filename}", header=0, parse_dates=["Date"]
                )
                latest_data = pd.read_csv("input_data/Google_cleaned.csv")
                latest_data["date"] = pd.to_datetime(latest_data["date"])
                google_historical = pd.concat([latest_data, ggdata])
                google_historical["stock_name"] = "Google"
                historical_stock_price["google"] = google_historical
            else:
                logger.info(
                    "Not included in this analysis yet,please check back later."
                )

    return historical_stock_price


def read_earning():
    """Read earning data from local path."""
    fb_y_earning = "input_data/fb_yearly_earning_index.csv"
    fb_q_earning = "input_data/fb_quarterly_earning_index.csv"
    fb_yearly_earning = pd.read_csv(fb_y_earning, sep=",")
    fb_quarterly_earning = pd.read_csv(fb_q_earning, sep=",")
    return fb_quarterly_earning, fb_yearly_earning


def get_nasdaq_index_data():
    """Read NASDAQ data from local path."""
    nasdaq = "input_data/nasdaq_index.csv"
    nasdaq_index = pd.read_csv(nasdaq, sep=",")
    return nasdaq_index


def read_raw_data_from_postgres(table_name, query, conn):
    """
    Reads data from a PostgreSQL table into a Pandas DataFrame.

    Args:
        table_name: Name of the PostgreSQL table to read data from.
        conn: psycopg2 connection object to the PostgreSQL database.

    Returns:
        Pandas DataFrame containing the data from the PostgreSQL table.
    """
    # Execute a SELECT query to fetch the data from the table.
    logger.info(f"Start collecting data for {table_name}")
    cursor = conn.cursor()
    cursor.execute(query)

    logger.info("Fetch the data as a list of tuples.")
    data = cursor.fetchall()

    logger.info("Get the column names from the cursor description")
    columns = [desc[0] for desc in cursor.description]

    logger.info("Create a Pandas DataFrame from the data and column names")
    df = pd.DataFrame(data, columns=columns)

    # Close the cursor.
    cursor.close()

    return df
