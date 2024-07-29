import requests
import json
import datetime
import pandas as pd
import os

api_key = '5f599589ff274053915a35ac351afa6e'  # Your API key
start_date = '2024-07-01'
end = datetime.date.today()

start = datetime.datetime.strptime(start_date, "%Y-%m-%d")
end = datetime.datetime.strptime(str(end), "%Y-%m-%d")
date_generated = [start + datetime.timedelta(days=x) for x in range(0, (end - start).days + 1)]

for date in date_generated:
    # Create a directory for the year and month if they don't exist
    year = date.strftime("%Y")
    month = date.strftime("%B")
    dir_path = os.path.join(year, month)
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    
    # Define the file path
    file_name = f"exchange_rates_{date.strftime('%Y-%m-%d')}.parquet"
    file_path = os.path.join(dir_path, file_name)
    print(file_path)
    # Check if the file already exists
    if os.path.exists(file_path):
        print(f"File for {date.strftime('%d-%m-%Y')} already exists. Skipping.")
        continue

    url = f'https://openexchangerates.org/api/historical/{date.strftime("%Y-%m-%d")}.json?app_id={api_key}'  # Default API URL
    json_obj = requests.get(url)
    dataq = json_obj.json()

    # Check if the response contains 'rates' to avoid errors
    if 'rates' in dataq:
        # Convert the 'rates' dictionary to a DataFrame and add a column for the date
        df = pd.DataFrame(list(dataq['rates'].items()), columns=['Currency', 'Rate'])
        df['Date'] = date.strftime("%Y-%m-%d")

        # Save the DataFrame to a Parquet file
        df.to_parquet(file_path, index=False)

        print(f"Saved data for {date.strftime('%d-%m-%Y')} to {file_path}")
    else:
        print(f"No rates data available for {date.strftime('%d-%m-%Y')}")

    print(date.strftime("%d-%m-%Y"))
