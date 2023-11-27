import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
import logging

def load_price(csv_file, table_name):
    # Configure logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    try:
        # Load environment variables
        dotenv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
        load_dotenv(dotenv_path)

        db_host = os.environ.get("DB_HOST")
        db_name = os.environ.get("DB_NAME")
        db_user = os.environ.get("DB_USER")
        db_password = os.environ.get("DB_PASSWORD")

        # Read the CSV file into a DataFrame
        csv_folder = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir, 'csv'))
        file_path = os.path.join(csv_folder, csv_file)
        df = pd.read_csv(file_path)

        # PostgreSQL connection parameters
        db_params = {
            'user': db_user,
            'password': db_password,
            'host': db_host,
            'port': 5432,
            'database': db_name
        }
        logger.info(f'{db_params}')   

        # Create a SQLAlchemy engine
        engine = create_engine(f'postgresql+psycopg2://{db_params["user"]}:{db_params["password"]}@{db_params["host"]}:{db_params["port"]}/{db_params["database"]}')

        # Insert the DataFrame into the PostgreSQL database, avoiding duplicates
        logger.info(f"Inserting data into {table_name} table")
        # df.to_sql(table_name, engine, index=False, if_exists='append', index_label=['date', 'symbol'], method='multi', chunksize=1000)

        logger.info("Task completed successfully.")

    except Exception as e:
        # Log an error if an exception occurs
        logger.error(f"Error in task: {str(e)}")

if __name__ == "__main__":
    load_price('stock_prices_clean', 'price')