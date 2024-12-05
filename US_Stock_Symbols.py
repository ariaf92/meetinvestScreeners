import requests
import pandas as pd

# Fetch symbol lists
def get_symbols_fmp():
    API_KEY = 'ADD KEY HERE'
    url = f'https://financialmodelingprep.com/api/v3/stock/list?apikey={API_KEY}'
    return requests.get(url).json()

def get_symbols_finnhub():
    API_KEY = 'ADD KEY HERE'
    url = f'https://finnhub.io/api/v1/stock/symbol?exchange=US&token={API_KEY}'
    return requests.get(url).json()

# Fetch and convert to DataFrames
symbols_fmp = get_symbols_fmp()
symbols_finnhub = get_symbols_finnhub()
fmp_df = pd.DataFrame(symbols_fmp).rename(columns={"name": "name_fmp", "exchangeShortName": "exchangeShortName_fmp"})
finnhub_df = pd.DataFrame(symbols_finnhub).rename(columns={"description": "name_finnhub"})

# Combine datasets
merged_df = pd.merge(
    fmp_df,
    finnhub_df,
    on='symbol',
    how='outer',
    suffixes=('_fmp', '_finnhub')
)

# Debug merged columns
print("Columns in merged_df:", merged_df.columns)

# Step 1: Deduplicate symbols
merged_df = merged_df.drop_duplicates(subset=['symbol'], keep='first')

# Step 2: Focus on US stocks
# Define relevant US stock market exchanges
us_exchanges = ['NYSE', 'NASDAQ', 'AMEX', 'OTC']
# Handle missing columns gracefully
if 'exchangeShortName_fmp' not in merged_df.columns:
    merged_df['exchangeShortName_fmp'] = None
if 'mic' not in merged_df.columns:
    merged_df['mic'] = None

# Filter by US exchanges
merged_df = merged_df[
    merged_df['exchangeShortName_fmp'].isin(us_exchanges) | 
    merged_df['mic'].isin(['XNYS', 'XNAS', 'XASE'])
]

# Step 3: Clean up and prioritize columns
merged_df['name'] = merged_df['name_fmp'].combine_first(merged_df['name_finnhub'])
merged_df['type'] = merged_df['type_fmp'].combine_first(merged_df['type_finnhub'])

# Drop redundant columns
merged_df = merged_df.drop(columns=['name_fmp', 'name_finnhub', 'type_fmp', 'type_finnhub'])

# Save the cleaned dataset
merged_df.to_csv('cleaned_us_symbols.csv', index=False)

# Display for verification
print(merged_df.head())

# Extract unique symbols from the merged DataFrame
unique_symbols = merged_df['symbol'].drop_duplicates().reset_index(drop=True)

# Save to a new CSV file
unique_symbols.to_csv('unique_symbols.csv', index=False)

# Display the first few rows for verification
print(unique_symbols.head())
