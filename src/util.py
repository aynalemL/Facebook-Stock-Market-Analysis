import boto3 as boto3

import logging

import psycopg2
from sqlalchemy import create_engine
from sqlalchemy import text

logger = logging.getLogger(__name__)


def create_db_instance(client, db_params, conn):
    """
    Create Postgres database instance.

    Parameters
    ----------
    client :
        Client with authenticate and authorize to request AWS service requests.
    db_params : dict
        A key value parameters that contains details of the database we want to create with the user password and user
    conn :
        postgres connection
    """

    if not check_database_exists(dbname=db_params["DBInstanceIdentifier"], conn=conn):
        # Create the new database instance/ Make sure the name you create is unique
        try:
            response = client.create_db_instance(**db_params)
            db_instance = response["DBInstance"]["DBInstanceIdentifier"]
            logger.info(f"database created with: {db_instance}")
        except Exception as e:
            logger.exception(f"Failed Creating db instance because: {e}.")
    else:
        logger.info(
            f"Failed to create database because the database already exist with the {db_params['DBName']},"
            "please double check the database name and retry again."
        )


def create_client(aws_access_key_id, aws_secret_access_key, region_name):
    """
    Create connection client to login into the aws account.

    Parameters
    ----------
    aws_access_key_id : str
        A string representing the access key ID for an AWS user
    aws_secret_access_key : str
        A string representing the secret access key for an AWS user
    region_name : str
        A string representing the AWS region to be used for AWS service requests.

    Returns
    -------
    client :
        AWS client
    """
    return boto3.client(
        "rds",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name,
    )


def connect_to_postgres(credentials):
    """
    Create connection to postgres database.

    Parameters
    ----------
    credentials : dict-like

    Returns
    -------
    conn :
        postgres connection
    """
    host = credentials["host"]
    database = credentials["database"]
    user = credentials["user"]
    password = credentials["password"]

    try:
        logger.info("Connect to the PostgreSQL databas")
        conn = psycopg2.connect(
            host=host, database=database, user=user, password=password
        )
        logger.info(
            "Set autocommit to True to avoid issues with transaction boundaries"
        )
        conn.autocommit = True
        return conn
    except Exception as e:
        print(f"Failed to connect to database: {e}")


def create_partitioned_table(conn, table_name):
    try:
        cur = conn.cursor()

        logger.info(
            "Define the SQL statements to create the parent table and child tables."
        )
        companies_historical_stock_price = """
            CREATE TABLE companies_historical_stock_price (
                date DATE NOT NULL,
                open DECIMAL(8, 2) NOT NULL,
                high DECIMAL(8, 2) NOT NULL,
                low DECIMAL(8, 2) NOT NULL,
                close DECIMAL(8, 2) NOT NULL,
                volume BIGINT NOT NULL,
                stock_name Varchar(25) NOT NULL,
                PRIMARY KEY (date)
            )
        """
        companies_historical_stock_price_fb = ""
        if table_name == "Facebook":
            companies_historical_stock_price_fb = f"""
                CREATE TABLE companies_historical_stock_price_{table_name.lower()} (
                    CHECK (created_date >= DATE '2019-01-01' AND created_date < DATE '2020-01-01')
                ) INHERITS (companies_historical_stock_price)
            """
        companies_historical_stock_price_goog = ""
        if table_name == "Google":
            companies_historical_stock_price_goog = f"""
                CREATE TABLE companies_historical_stock_price_{table_name.lower()}(
                    CHECK (created_date >= DATE '2020-01-01' AND created_date < DATE '2021-01-01')
                ) INHERITS (companies_historical_stock_price)
            """
        else:
            "No table to create"
        logger.info("Define the SQL statement to create the insert trigger function.")
        insert_trigger_function_query = """
            CREATE OR REPLACE FUNCTION insert_companies_historical_stock_price()
            RETURNS TRIGGER AS $$
            BEGIN
                IF NEW.stock_name >= 'Facebook' THEN
                    INSERT INTO companies_historical_stock_price_fb VALUES (NEW.*);
                ELSIF NEW.stock_name 'Google THEN
                    INSERT INTO companies_historical_stock_price_goog VALUES (NEW.*);
                ELSE
                    RAISE EXCEPTION 'stock_name not found. Fix the function!';
                END IF;
                RETURN NULL;
            END;
            $$ LANGUAGE plpgsql;
        """

        logger.info(
            "Define the SQL statement to attach the insert trigger function to the parent table."
        )
        attach_trigger_query = """
            CREATE TRIGGER insert_my_partitioned_table_trigger
                BEFORE INSERT ON my_partitioned_table
                FOR EACH ROW EXECUTE FUNCTION insert_my_partitioned_table()
        """

        logger.info("Execute the SQL statements to create the partitioned table.")
        cur.execute(companies_historical_stock_price)
        cur.execute(companies_historical_stock_price_fb)
        cur.execute(companies_historical_stock_price_goog)
        cur.execute(insert_trigger_function_query)
        cur.execute(attach_trigger_query)

        logger.info("Commit the transaction.")
        conn.commit()

        logger.info("Partitioned table created successfully!")
    except Exception as error:
        logger.info("Error while creating partitioned table", error)
    return table_name


def create_table_stock_table(conn, table_name, db_name):
    if check_database_exists(conn=conn, dbname=db_name):
        if not check_table_exists(table_name=table_name, conn=conn):

            query = f"""
                CREATE TABLE {table_name} (
                        date DATE NOT NULL,
                        open DECIMAL(8, 2) NOT NULL,
                        high DECIMAL(8, 2) NOT NULL,
                        low DECIMAL(8, 2) NOT NULL,
                        close DECIMAL(8, 2) NOT NULL,
                        volume BIGINT NOT NULL,
                        stock_name Varchar(25) NOT NULL,
                        PRIMARY KEY (date)
                    )
                """
            try:
                cur = conn.cursor()
                cur.execute(query)
                logger.info(" Table created successfully!")
            except Exception as error:
                logger.info("Error while creating table", error)
        else:
            logger.info(f"Table already exist with this {table_name}")

    else:
        logger.info("Database doesn't exit")


def create_table_from_df(df, table_name, engine_str):
    # Create database connection engine
    engine = create_engine(engine_str)
    conn = engine.connect()

    # Infer schema from DataFrame
    dtypes = {col: df[col].dtype for col in df.columns}
    column_types = {col: 'INTEGER' if dtype == 'int64' else 'DOUBLE PRECISION' if dtype == 'float64' else 'VARCHAR' for
                    col, dtype in dtypes.items()}
    columns = ', '.join([f'{col} {column_types[col]}' for col in df.columns])

    # Create table with inferred schema
    create_table_query = f'CREATE TABLE IF NOT EXISTS {table_name} ({columns})'
    conn.execute(text(create_table_query))

    # Write data to table
    df.to_sql(table_name, engine, if_exists='append', index=False)

    # Close database connection
    conn.close()


def check_database_exists(dbname, conn):
    cursor = conn.cursor()
    logger.info("Check if the database exists")
    cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{dbname}'")
    exists = cursor.fetchall()

    if not exists:
        print(f"Database '{dbname}' does not exist")
        return False


def check_table_exists(table_name, conn):
    cursor = conn.cursor()
    # Check if the table exists
    cursor.execute(
        f"SELECT * FROM information_schema.tables WHERE table_name = '{table_name}'"
    )

    exists = cursor.fetchone()
    if not exists:
        logger.info(f"Table '{table_name}' does not exist")
        return False

    logger.info(f"Table with '{table_name}' already exist")
    return True
