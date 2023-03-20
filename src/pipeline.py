import logging

import util
from etl import transform, load
from etl import extract

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

DB_NAME = 'historical_stock_price'


class Pipeline:
    def __init__(self, credentials):
        self.credentials = credentials

    def run(self):
        "Run the pipeline."
        table_names = [
            "companies_historical_stock_price_facebook"
            "companies_historical_stock_price_Google",
            "fb_yearly_earning",
            "fb_quarterly_earning",
            "fb_yearly_earning",
        ]
        for table_name in table_names:
            logger.info(f"Create connection")
            conn = util.connect_to_postgres(self.credentials)

            logger.info(f"Get raw data.")
            nasdaq_index = extract.get_nasdaq_index_data()
            fb_quarterly_earning, fb_yearly_earning = extract.read_earning()
            if table_name == "companies_historical_stock_price_facebook":
                stock_data = extract.read_stock_data()
                facebook_historical_stock = stock_data["fb"]
                cleaned_facebook_historical_stock = transform.clean_stock_data(
                    facebook_historical_stock
                )
                logger.info("Upload Facebook data.")
                util.create_table_stock_table(conn=conn, table_name=table_name, db_name=DB_NAME)
                load.upload_df_to_postgres(
                    df=cleaned_facebook_historical_stock,
                    conn=conn,
                    table_name=table_name,
                )

            if table_name == "companies_historical_stock_price_Google":
                stock_data = extract.read_stock_data()
                google_historical_stock = stock_data["google"]
                cleaned_google_historical_stock = transform.clean_stock_data(
                    google_historical_stock
                )

                logger.info("Upload Google data.")
                util.create_table_stock_table(conn=conn, table_name=table_name, db_name=DB_NAME)
                load.upload_df_to_postgres(
                    df=cleaned_google_historical_stock, conn=conn, table_name=table_name
                )

            if table_name == "nasdaq_index":
                logger.info("Upload NASDAQ data to none-partitioned table.")
                load.upload_data_to_postgres(
                    df=nasdaq_index, table_name="nasdaq_index", conn=conn
                )
            if table_name == "fb_yearly_earning":
                logger.info("Upload yearly earning data to none-partitioned tables.")

                engine_str = f'postgresql://{"username"}:{"password"}@{"host"}:5432/{"database"}'

                # Call function to create table and write data
                util.create_table_from_df(fb_yearly_earning, 'my_table', engine_str)
                load.upload_data_to_postgres(
                    df=fb_yearly_earning, table_name="fb_yearly_earning", conn=conn
                )
            if table_name == "fb_quarterly_earning":
                logger.info("Upload yearly earning data to none-partitioned tables.")
                load.upload_data_to_postgres(
                    df=fb_quarterly_earning,
                    table_name="fb_quarterly_earning",
                    conn=conn,
                )

            logger.info("Completed uploading the data postgres table.")

            conn.close()

    def get_raw_data_(self):
        """get raw data from postgres table"""
        logger.info(f"Create connection")
        conn = util.connect_to_postgres(self.credentials)
        data = extract.read_raw_data_from_postgres(
            table_name="companies_historical_stock_price_facebook",
            conn=conn,
            query="SELECT * FROM companies_historical_stock_price_facebook;",
        )
        return data
