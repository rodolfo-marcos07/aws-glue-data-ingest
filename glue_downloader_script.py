import sys
import requests
import pandas as pd
import boto3
from awsglue.utils import getResolvedOptions

# Initialize the S3 client
s3 = boto3.client('s3')

def download_data(country_code):
    # URL of the JSON data
    url = f"https://date.nager.at/api/v3/publicholidays/2024/{country_code}"

    # Download the data
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        # Parse the response content as JSON
        data = response.json()
        # Convert JSON data to a DataFrame
        df = pd.DataFrame(data)
        # Select specific attributes (columns)
        selected_columns = df[['date', 'name', 'countryCode']]
        # Convert the date column to datetime format using .loc
        selected_columns.loc[:, "date"] = pd.to_datetime(selected_columns.loc[:, "date"])
        # Save the DataFrame as a Parquet file
        selected_columns.to_parquet(f'{country_code}.parquet', engine='pyarrow', index=False)
        return f'{country_code}.parquet'

    else:
        print("Failed to retrieve data:", response.status_code)

def upload_to_s3(file_name):
    
    # Define S3 bucket and key (file path within the bucket)
    bucket_name = '{your bucket name}'
    file_key = f'raw/{file_name}'
    
    # Upload the parquet file
    s3.upload_file(file_name, bucket_name, file_key)

if __name__ == '__main__':
    # Define the expected arguments
    args = getResolvedOptions(sys.argv, ['country_code'])
    # Access the parameters
    country_code = args['country_code']
    
    file_name = download_data(country_code)
    upload_to_s3(file_name)
