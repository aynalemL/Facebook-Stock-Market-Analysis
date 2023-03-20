import logging
import pandas as pd

logger = logging.getLogger(__name__)


def clean_stock_data(df):
    """Clean DataFrame"""

    logger.info("Convert date data type to datetime.")
    if df["Date"].dtype == "object":
        df["Date"] = pd.to_datetime(df["Date"])

    logger.info("With assumption that all 0's are na  decided to drop all OpenInt")
    df = df.drop("OpenInt", axis=1)

    # Rename columns. We can also, use inplace=True if needed.
    df = df.rename(
        columns={
            "Date": "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        }
    )

    return df

# TODO : Add function to do the analysis locally like extract the monthly, quarterly and yearly ROI either with
#  python or sql query. Because of time constraint, we couldn't add this step
