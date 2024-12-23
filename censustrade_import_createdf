# Retrieves import and export data from U.S. Census International Trade Data API and converts to pandas dataframe
# Specify Variables: start_date, end_date, 10 digit HS codes, and fields to retreive 
# imp_fields and exp_fields lists must be in the same order ie: match corresponding imp and exp fields
# API documentation, including possible fields - https://www.census.gov/foreign-trade/reference/guides/Guide_to_International_Trade_Datasets.pdf
# Request free API key from census.gov - or remove key parameter from params if making less than 500 api calls per day
# Accepts 10 digit HS codes
# can typically return data from current month -2 (can return Oct 2024 data in Dec 2024)


import requests
import pandas as pd

# Variables
api_key = "api key goes here"
start_date = '2024-08'
end_date = '2024-09'
imp_fields = "I_COMMODITY,I_COMMODITY_SDESC,I_COMMODITY_LDESC,CTY_CODE,CTY_NAME,GEN_VAL_MO,GEN_VAL_YR,GEN_QY1_MO,GEN_QY1_YR,UNIT_QY1"
impcon_fields = "I_COMMODITY,I_COMMODITY_SDESC,I_COMMODITY_LDESC,CTY_CODE,CTY_NAME,CON_VAL_MO,CON_VAL_YR,CON_QY1_MO,CON_QY1_YR,UNIT_QY1"
exp_fields = "E_COMMODITY,E_COMMODITY_SDESC,E_COMMODITY_LDESC,CTY_CODE,CTY_NAME,ALL_VAL_MO,ALL_VAL_YR,QTY_1_MO,QTY_1_YR,UNIT_QY1"
commodity_codes = ['7601103000','7601106000','7601209045','7602000030','7602000091','7602000096','2825600000','2827399000','8112926000','8112926500','8112991000']  # List of 10 digit HS codes

# Generate a list of months in YYYY-MM format
date_range = pd.date_range(start=start_date, end=end_date, freq='MS').strftime("%Y-%m").tolist()

# Column name creation
imp_cols = imp_fields.split(",") + ["time", "hs_extra", "agg_extra"]
exp_cols = exp_fields.split(",") + ["time", "hs_extra", "agg_extra"]

# Function to fetch data through API
def fetch_census_trade_data(date_range, commodity_codes, trade_type, fields):
    # Base URL based on trade type
    base_url = f"https://api.census.gov/data/timeseries/intltrade/{trade_type}/hs"
    all_data = []

    # Iterate through each year and commodity code
    for date in date_range:
        for commodity_code in commodity_codes:
            params = {
                "get": fields,
                "time": date,
                "key": {api_key},
                f"{'I' if trade_type == 'imports' else 'E'}_COMMODITY": commodity_code,
                "SUMMARY_LVL": "DET"
            }

            try:
                response = requests.get(base_url, params=params)
                response.raise_for_status()
                data = response.json()
                all_data.extend(data[1:])

            except requests.exceptions.HTTPError as http_err:
                print(f"HTTP error occurred for year {date} and commodity code {commodity_code}: {http_err}")
            except Exception as err:
                print(f"An error occurred for year {date} and commodity code {commodity_code}: {err}")

    return all_data

# Main execution
if __name__ == "__main__":
    # Fetch import and export data
    census_imp_data = fetch_census_trade_data(date_range, commodity_codes, "imports", imp_fields)
    census_impcon_data = fetch_census_trade_data(date_range, commodity_codes, "imports", impcon_fields)
    census_exp_data = fetch_census_trade_data(date_range, commodity_codes, "exports", exp_fields)

    # Convert to pandas DataFrames
    imp_df = pd.DataFrame(census_imp_data, columns=imp_cols).iloc[:, :-2]
    imp_df["direction"] = "imports_general"

    impcon_df = pd.DataFrame(census_impcon_data, columns=imp_cols).iloc[:, :-2]
    impcon_df["direction"] = "imports_consumption"

    exp_df = pd.DataFrame(census_exp_data, columns=exp_cols).iloc[:, :-2]
    exp_df["direction"] = "exports"

    # Change exp_df column headers to match imp_df and append dataframes
    exp_df.columns = imp_df.columns
    combined_df = pd.concat([imp_df, impcon_df, exp_df], ignore_index=True)

    # Split 'time' column into year and month, then drop the time column
    combined_df[['year', 'month']] = combined_df['time'].str.split('-', expand=True)
    combined_df = combined_df.drop(columns=['time'])

# Set option to display all columns and rows
# pd.set_option('display.max_columns', None)
# pd.set_option('display.max_rows', None)

print(combined_df)
