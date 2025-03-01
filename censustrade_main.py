# Retrieves import and export data from U.S. Census International Trade Data API for a specified date range, converts to pandas df, writes to PostgreSQL DB
# Specify Variables: api_key, DB conn_string, destination table_name, start_date, end_date, 10 digit HS codes, and fields to retreive 
# imp_fields and exp_fields lists must be in the same order ie: match corresponding imp and exp fields
# API documentation, including possible fields - https://www.census.gov/foreign-trade/reference/guides/Guide_to_International_Trade_Datasets.pdf
# Request free API key from census.gov - or remove key parameter from params if making less than 500 api calls per day
# Accepts 10 digit HS codes - https://hts.usitc.gov/
# data from 2 months prior is typically available the first week of every month (2024-10 will be available December 5, 2024)
# data release schedule - https://www.census.gov/foreign-trade/schedule.html

import requests
import pandas as pd
import psycopg
from psycopg import sql
from io import StringIO

# Variables
api_key = "api key goes here"
conn_string = "postgresql://username:password@host:port/dbname"
table_name = "destination table name goes here"
start_date = "2019-01"
end_date = "2024-10"
imp_fields = "I_COMMODITY,I_COMMODITY_SDESC,I_COMMODITY_LDESC,CTY_CODE,CTY_NAME,GEN_VAL_MO,GEN_QY1_MO,GEN_VAL_YR,GEN_QY1_YR,UNIT_QY1"
impcon_fields = "I_COMMODITY,I_COMMODITY_SDESC,I_COMMODITY_LDESC,CTY_CODE,CTY_NAME,CON_VAL_MO,CON_QY1_MO,CON_VAL_YR,CON_QY1_YR,UNIT_QY1"
exp_fields = "E_COMMODITY,E_COMMODITY_SDESC,E_COMMODITY_LDESC,CTY_CODE,CTY_NAME,ALL_VAL_MO,QTY_1_MO,ALL_VAL_YR,QTY_1_YR,UNIT_QY1"
commodity_codes = ['7601103000','7601106000','7601209045','7602000030','7602000091','7602000096','2825600000','2827399000','8112926000','8112926500','8112991000']  # List of 10 digit HS codes

# Generate a list of months in YYYY-MM format
date_range = pd.date_range(start=start_date, end=end_date, freq='MS').strftime("%Y-%m").tolist()

# Column name creation
imp_cols = imp_fields.split(",") + ["time", "hs_extra", "agg_extra"]
exp_cols = exp_fields.split(",") + ["time", "hs_extra", "agg_extra"]

# Function to fetch data through census API
def fetch_census_trade_data(date, commodity_codes, trade_type, fields):
    # Base URL based on trade type
    base_url = f"https://api.census.gov/data/timeseries/intltrade/{trade_type}/hs"
    all_data = []

    # Iterate through each commodity code
    for commodity_code in commodity_codes:
        params = {
            "get": fields,
            "time": date,
            "key": api_key,
            f"{'I' if trade_type == 'imports' else 'E'}_COMMODITY": commodity_code,
            "SUMMARY_LVL": "DET"
        }

        try:
            response = requests.get(base_url, params=params)
            response.raise_for_status()
            data = response.json()
            all_data.extend(data[1:])

        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred for date {date} and commodity code {commodity_code}: {http_err}")
        except Exception as err:
            print(f"An error occurred for date {date} and commodity code {commodity_code}: {err}")

    return all_data

# Function to write dataframe to PostgreSQL
def copy_chunk(conn, df_chunk: pd.DataFrame, table_name: str) -> None:
    """Upload a single chunk to the database using the COPY command."""
    with conn.cursor() as cur:
        # Create a buffer
        buffer = StringIO()
        df_chunk.to_csv(buffer, index=False, header=False)
        buffer.seek(0)

        copy_sql = sql.SQL('COPY {} FROM STDIN WITH (FORMAT CSV)').format(sql.Identifier(table_name))
        # Load data into the table using copy
        with buffer as f:
            with cur.copy(copy_sql) as copy:
                while data := f.read(10):
                    copy.write(data)

    conn.commit()

# Main execution
if __name__ == "__main__":
    # Loop through each month in the date range
    for date in date_range:
        # Fetch import and export data for the current month
        census_imp_data = fetch_census_trade_data(date, commodity_codes, "imports", imp_fields)
        census_impcon_data = fetch_census_trade_data(date, commodity_codes, "imports", impcon_fields)
        census_exp_data = fetch_census_trade_data(date, commodity_codes, "exports", exp_fields)

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

        # Split 'time' column into year and month columns, then drop the time column
        combined_df[['year', 'month']] = combined_df['time'].str.split('-', expand=True)
        combined_df = combined_df.drop(columns=['time'])
        # Add primary key column
        combined_df['id'] = combined_df.apply(lambda row: f"{row['year']}_{row['month']}_{row['CTY_CODE']}_{row['I_COMMODITY']}_{row['direction']}", axis=1)

        # Establish connection and write to table_name
        with psycopg.connect(conn_string) as conn:
            copy_chunk(conn, combined_df, table_name)
            print(f"{date} was successfully uploaded")

        # Close the connection
        conn.close()
