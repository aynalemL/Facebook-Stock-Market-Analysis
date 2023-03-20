import logging
from io import StringIO

import psycopg2
import psycopg2.extras
logger = logging.getLogger(__name__)


def batch_upload(df, table_name, conn, batch_size=1000):
    """
    Upload a Pandas DataFrame to a PostgreSQL table in batches.

    Parameters
    ----------
    df  : pandas.dataFrame
        The DataFrame to upload to the PostgreSQL table.
    table_name : str
        The name of the PostgreSQL table to upload the data to.
    conn:
       psycopg2 connection object to the PostgreSQL database.
    batch_size : int, optional
        The number of rows to upload to the database at a time. Defaults to 1000.
    """
    try:
        logger.info(f"Start uploading df into {table_name} in a batch")
        num_rows = len(df)
        for start_index in range(0, num_rows, batch_size):
            end_index = min(start_index + batch_size, num_rows)
            df_batch = df[start_index:end_index]

            df_batch.to_sql(table_name, conn, if_exists="append", index=False)
            logger.info(f"Finished uploading the {num_rows} to {table_name}")
    except Exception as e:
        logger.exception(f"Filed to complete data upload to {table_name} due to {e}")

    conn.close()


def upload_df_to_postgres(df, table_name, conn):
    """
    Uploads a Pandas DataFrame to a PostgreSQL database in batch.

    Parameters
    ----------
    df : Pandas.DataFrame to upload
        A DataFrame to upload.
    table_name : str
        Name of the PostgreSQL table to upload the DataFrame to.
    conn :
    psycopg2 connection object to the PostgreSQL database.

    Returns:
        None.
    """
    # Create a cursor object to interact with the database.
    cursor = conn.cursor()

    # Create a StringIO object to write the DataFrame to as a CSV.
    try:
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False, header=False)
        csv_buffer.seek(0)

        cursor.copy_from(csv_buffer, table_name, sep=",")

        conn.commit()
    except Exception as e:
        logger.info(f"Filed while uploading DataFrame: reason, {e}")
        cursor.close()


def upload_data_to_postgres(df, conn, table_name):
    if len(df) > 0:
        df_columns = list(df)

        columns = ",".join(df_columns)

        values = "VALUES({})".format(",".join(["%s" for _ in df_columns]))

        insert_stmt = f"""INSERT INTO {table_name} ({columns}) {str(values)}"""

        cur = conn.cursor()
        psycopg2.extras.execute_batch(cur, insert_stmt, df.values)
        conn.commit()
    else:
        logger.info("No DataFrame to upload.")
