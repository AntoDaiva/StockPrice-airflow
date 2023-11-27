import pandas as pd

def get_symbols():
    # Specify the path to your TXT file
    file_path = 'excluded_stocks.txt'

    # Initialize an empty list to store symbols
    symbols = []

    # Open the file and read its content
    with open(file_path, 'r') as file:
        # Read lines from the file
        lines = file.readlines()

        # Iterate through each line
        for line in lines:
            # Split each line at ':', and get the symbol before ':'
            symbol = line.split(':')[0].strip()

            # Append the symbol to the list if it's not empty
            if symbol:
                symbols.append(symbol)
    
    return symbols

def filter_symbols(symbols):
    # filter stock symbols
    sym_df = pd.read_csv('csv/stock_symbols.csv')
    # Drop rows where the 'symbol' column is in the list
    df_filtered = sym_df[~sym_df['symbol'].isin(symbols)]


    df_filtered.to_csv('csv/stock_symbols.csv', index=False)

    # filter stock info
    info_df = pd.read_csv('csv/stock_info.csv')
    # Drop rows where the 'symbol' column is in the list
    df_filtered = info_df[~info_df['symbol'].isin(symbols)]

    df_filtered.to_csv('csv/stock_info.csv', index=False)

if __name__ == "__main__":
    symbols = get_symbols()
    filter_symbols(symbols)