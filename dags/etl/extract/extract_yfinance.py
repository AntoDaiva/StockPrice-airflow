import yfinance as yf
import pandas as pd
import datetime
import logging
import os

def extract_prices():
  # Configure logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    try:
        csv_folder = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir, 'csv'))

        logger.info("Reading symbols from stock_symbols.csv")
        symbols = pd.read_csv(os.path.join(csv_folder, 'stock_symbols.csv'))
        symbols = symbols["symbol"].tolist()

        df = pd.DataFrame(columns=['Symbol','Open', 'High', 'Low', 'Close', 'Volume', 'Dividends', 'Stock Splits'])

        for symbol in symbols:
            logger.info(f"Fetching data for symbol: {symbol}")
            stock = yf.Ticker(symbol)

            # get historical market data
            hist = stock.history(period='1d')
            hist['Symbol'] = symbol
            df = pd.concat([df, hist])

        df.reset_index(inplace=True)
        df.rename(columns={'index': 'date'}, inplace=True)
        df.to_csv(os.path.join(csv_folder, 'stock_prices.csv'), index=False)

        logger.info("Task completed successfully.")

    except Exception as e:
        # Log an error if an exception occurs
        logger.error(f"Error in task: {str(e)}")

if __name__ == "__main__":
  extract_prices()