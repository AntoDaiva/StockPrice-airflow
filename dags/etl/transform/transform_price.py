import pandas as pd
import os
import logging

def transform_price():
    # Configure logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    try:
        # Access csv folder path
        csv_folder = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir, 'csv'))

        # Read the CSV file into a DataFrame
        logger.info("Reading stock prices from stock_prices.csv")
        stock_df = pd.read_csv(os.path.join(csv_folder, 'stock_prices.csv'))

        # Log information about the data
        logger.info(f"Number of rows in stock_df: {len(stock_df)}")
        logger.info(f"Columns in stock_df: {stock_df.columns.tolist()}")

        # Select attributes
        clean_df = stock_df[['date', 'Symbol', 'Open', 'High', 'Low', 'Close', 'Volume', 'Dividends']]

        # Convert datetime and uniform to UTC
        clean_df['date'] = pd.to_datetime(clean_df['date'], utc=True)

        # Assure columns to desired data types
        clean_df['Symbol'] = clean_df['Symbol'].astype('object')
        clean_df['Open'] = clean_df['Open'].astype('float64')
        clean_df['High'] = clean_df['High'].astype('float64')
        clean_df['Low'] = clean_df['Low'].astype('float64')
        clean_df['Close'] = clean_df['Close'].astype('float64')
        clean_df['Volume'] = clean_df['Volume'].astype('int64')
        clean_df['Dividends'] = clean_df['Dividends'].astype('float64')

        # Lowercase all column names
        clean_df.columns = clean_df.columns.str.lower()

        # Drop null values
        clean_df.dropna(inplace=True)

        # Save to CSV
        clean_df.to_csv(os.path.join(csv_folder, 'stock_prices_clean.csv'), index=False)

        logger.info("Task completed successfully.")

    except Exception as e:
        # Log an error if an exception occurs
        logger.error(f"Error in task: {str(e)}")

if __name__ == "__main__":
    transform_price()