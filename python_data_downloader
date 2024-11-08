# Import dependencies
import requests
import pandas as pd

def download_data(country_code):
    # Nager API URL
    url = f"https://date.nager.at/api/v3/publicholidays/2024/{country_code}"

    # Download the data
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        # Parse the response content as JSON
        data = response.json()
        # Convert JSON data to a pandas DataFrame
        df = pd.DataFrame(data)
        # Select specific attributes (columns)
        selected_columns = df[['date', 'name', 'countryCode']]
        # Convert the date column to datetime format using .loc
        selected_columns.loc[:, "date"] = pd.to_datetime(selected_columns.loc[:, "date"])
        # Save the DataFrame as a Parquet file
        selected_columns.to_parquet(f'{country_code}.parquet', engine='pyarrow', index=False)

    else:
        print("Failed to retrieve data:", response.status_code)

if __name__ == '__main__':
    country_code = 'US'
    download_data(country_code)
