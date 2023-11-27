import pandas as pd
import os
import logging

def get_stock_info():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    try:
        csv_folder = os.path.join(os.getcwd(), 'dags', 'csv')

        stock_info = os.path.join(csv_folder, 'robinhood.csv')

        logger.info(f"Reading data from {stock_info}")
        df = pd.read_csv(stock_info)

        columns_to_keep = ["name", "symbol", "type", "description", "ceo", "headquarters_city", "industry"]
        df_t = df[columns_to_keep]

        columns_to_check = ["ceo", "headquarters_city"]
        df_cleaned = df_t.dropna(subset=columns_to_check)

        df_fix = df_cleaned[df_cleaned["type"] == 'stock']
        symbols = df_fix["symbol"]

        # Log information about the data
        logger.info(f"Columns after filtering: {df_fix.columns.tolist()}")
        logger.info(f"Number of symbols: {len(symbols)}")

        # Save data to CSV
        symbols.to_csv(os.path.join(csv_folder, 'stock_symbols.csv'), index=False)
        df_fix.to_csv(os.path.join(csv_folder, 'stock_info.csv'), index=False)

        logger.info("Task completed successfully.")

    except Exception as e:
        # Log an error if an exception occurs
        logger.error(f"Error in task: {str(e)}")

        
if __name__ == "__main__":
    get_stock_info()