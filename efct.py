import requests
import pandas as pd

# Variables
start_date = '2022-01'
end_date = '2024-12'
imp_fields = "I_COMMODITY,I_COMMODITY_SDESC,I_COMMODITY_LDESC,CTY_CODE,CTY_NAME,GEN_VAL_YR,GEN_QY1_YR,UNIT_QY1"
impcon_fields = "I_COMMODITY,I_COMMODITY_SDESC,I_COMMODITY_LDESC,CTY_CODE,CTY_NAME,CON_VAL_YR,CON_QY1_YR,UNIT_QY1"
exp_fields = "E_COMMODITY,E_COMMODITY_SDESC,E_COMMODITY_LDESC,CTY_CODE,CTY_NAME,ALL_VAL_YR,QTY_1_YR,UNIT_QY1"
commodity_codes = ['7601103000','7601106030','7601209045','7602000035','7602000095','7602000097','2603000010','7402000000','7403000000','7404000000','7408110000']  # List of 10 digit HS codes

# Generate a list of months in YYYY-MM format, but only include December months
date_range = pd.date_range(start=start_date, end=end_date, freq='MS').strftime("%Y-%m").tolist()
date_range = [date for date in date_range if date.endswith('-12')]  # Filter to include only December

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
    # Add primary key column
    combined_df['id'] = combined_df.apply(lambda row: f"{row['year']}_{row['month']}_{row['CTY_CODE']}_{row['I_COMMODITY']}_{row['direction']}", axis=1)

# Set option to display all columns and rows
# pd.set_option('display.max_columns', None)
# pd.set_option('display.max_rows', None)

filename = 'C:\\data\\census_trade\\sample_aluminum_copper.csv' # name your file

# Save the DataFrame to a CSV file
combined_df.to_csv(filename, index=False, sep='|')
