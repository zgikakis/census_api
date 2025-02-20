# 1) Copy and paste this code into notepad
# 2) Edit the variables section to specify which data to retrieve
# 3) Go to: https://jupyter.org/try-jupyter/lab/
# 4) Create a blank Python (Pyodide) notebook
# 5) Copy the code from notepad and paste into jupyter notebook
# 6) Press "Run this cell and advance"
# 7) Wait for file to generate; Following error code is not an issue: "An error occurred for year 2023-12 and commodity code 2617900030: Expecting value: line 1 column 1 (char 0)" 
# 8) After file has finished generating, it will appear under "notebooks" on the left side of page; right click and download

import requests
import pandas as pd

# Variables: specify date range, HTS codes, and filename
#---------------------------------------------------------------------------------------------------------------
# Enter a start year and end year for the data
start_year = '2023'
end_year = '2024'

# Enter list of 10 digit HTS codes; each list item must be surrounded by quotes "" and separated by comma
commodity_codes = ["2504101000", "2504105000", "7602000035","7602000095"]

# Name the file, ending in .csv
filename = 'sample_01.csv'

#---------------------------------------------------------------------------------------------------------------
# Fields to pull
imp_fields = "I_COMMODITY,I_COMMODITY_SDESC,I_COMMODITY_LDESC,CTY_CODE,CTY_NAME,GEN_VAL_YR,GEN_QY1_YR,UNIT_QY1"
impcon_fields = "I_COMMODITY,I_COMMODITY_SDESC,I_COMMODITY_LDESC,CTY_CODE,CTY_NAME,CON_VAL_YR,CON_QY1_YR,UNIT_QY1"
exp_fields = "E_COMMODITY,E_COMMODITY_SDESC,E_COMMODITY_LDESC,CTY_CODE,CTY_NAME,ALL_VAL_YR,QTY_1_YR,UNIT_QY1"

# Generates a list of dates in YYYY-MM format and only includes December
start_date = f'{start_year}-01'
end_date = f'{end_year}-12'
date_range = pd.date_range(start=start_date, end=end_date, freq='MS').strftime("%Y-%m").tolist()
date_range = [date for date in date_range if date.endswith('-12')]  # Filter to include only December

# Create column names
imp_cols = imp_fields.split(",") + ["time", "hs_extra", "agg_extra"]
exp_cols = exp_fields.split(",") + ["time", "hs_extra", "agg_extra"]

# Fetch data through API
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

# Execution
if __name__ == "__main__":
    # Fetch import and export data
    census_imp_data = fetch_census_trade_data(date_range, commodity_codes, "imports", imp_fields)
    census_impcon_data = fetch_census_trade_data(date_range, commodity_codes, "imports", impcon_fields)
    census_exp_data = fetch_census_trade_data(date_range, commodity_codes, "exports", exp_fields)

    # Convert to pandas dataframes
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


# Save the DataFrame to a CSV file
combined_df.to_csv(filename, index=False, sep='|')

print(f"File has finished generating: {filename}")
