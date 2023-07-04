import os
import json
import pandas as pd
import mysql.connector
from datetime import datetime, timedelta
import re

path = "C:/ETLS/" # path for dump_dir

# Path for lastrun.log and table_to_export
lastrun_path = "C:/20230703_task/"
table_to_export_path = "C:/20230703_task/"

filename = os.path.join(lastrun_path, "lastrun.txt")
dump_dir = os.path.join(path, "dump_dir")

# Check if the path exists, create it if it doesn't
if not os.path.exists(path):
    os.makedirs(path)

# Check if the dump directory exists, create it if it doesn't
if not os.path.exists(dump_dir):
    os.makedirs(dump_dir)

# Check if the file exists
if not os.path.isfile(filename):
    # File doesn't exist, create it and write the initial value
    with open(filename, "w") as file:
        file.write("2020-01-01 00:00:00")

# Read the content of the file and store it in start_date variable
with open(filename, "r") as file:
    start_date = file.read()

# Check if the start_date is empty, set default value if it is
if not start_date:
    start_date = "2020-01-01 00:00:00"

# Parse the start_date string into a datetime object and convert it to the desired format
start_datetime = datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S")

# Subtract 1 minute from the current datetime to get the end_datetime
end_datetime = datetime.now() - timedelta(minutes=1)

# Format the start_datetime and end_datetime as strings in the desired format
start_date = start_datetime.strftime("%Y-%m-%d %H:%M:%S")
end_date = end_datetime.strftime("%Y-%m-%d %H:%M:%S")

start_date_formatted = start_datetime.strftime("%Y%m%d_%H%M")  # Format for CSV file names
end_date_formatted = end_datetime.strftime("%Y%m%d_%H%M")  # Format for CSV file names

file_name_end_date = re.sub(r'[^\w\-_.]', '_', end_date)

print("Last run value:", start_datetime)
print("End date value:", end_date)

# Path and filename for the JSON file
json_filename = os.path.join(table_to_export_path, "table_for_export.txt")

# Read the content of the file
with open(json_filename, "r") as file:
    content = file.read()

# Split the content by newline to get a list of JSON objects
json_list = content.split("\n")

# MySQL database connection details
host = 'localhost'
user = 'root'
password = ''
database = 'db1'
port = 3306

# Connect to the MySQL database
conn = mysql.connector.connect(
    host=host,
    user=user,
    password=password,
    database=database,
    port=port
)

# Initialize an empty dictionary to store DataFrames
dataframes = {}

# Parse each JSON object in the list
for json_str in json_list:
    # Skip empty lines
    if not json_str:
        continue

    # Parse the JSON string into a dictionary
    json_obj = json.loads(json_str)

    # Extract the table_name and field_list from the dictionary
    table_name = json_obj.get("table_name")
    field_list = json_obj.get("field_list")

    # Convert field_list to a list if it's a string
    if isinstance(field_list, str):
        field_list = [field.strip() for field in field_list.split(',')]

    # Include created_at and modified_at fields even if not in field_list
    if "created_at" not in field_list:
        field_list.append("created_at")
    if "modified_at" not in field_list:
        field_list.append("modified_at")

    # Formulate the SQL query
    query = f"SELECT {','.join(field_list)} FROM {table_name}"

    # Execute the SQL query and load results into a DataFrame
    df = pd.read_sql_query(query, conn)

    # Convert start_datetime and end_datetime to pandas Timestamp objects
    start_timestamp = pd.Timestamp(start_datetime)
    end_timestamp = pd.Timestamp(end_datetime)

    # Filter rows based on the conditions
    filtered_df = df[
        ((df['created_at'] > start_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')) & (
                    df['created_at'] <= end_datetime.strftime('%Y-%m-%d %H:%M:%S.%f'))) |
        ((df['modified_at'] > start_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')) & (
                    df['modified_at'] <= end_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')))
        ]

    # Convert the 'created_at' and 'modified_at' columns to the desired format
    filtered_df['created_at'] = filtered_df['created_at'].dt.strftime('%Y-%m-%d %H:%M:%S.%f')
    filtered_df['modified_at'] = filtered_df['modified_at'].dt.strftime('%Y-%m-%d %H:%M:%S.%f')

    # Store the filtered DataFrame in the dataframes dictionary using table_name as the key
    dataframes[table_name] = filtered_df

    # Save the filtered DataFrame to a CSV file
    csv_filename = f"{table_name}{{{start_date_formatted}}}.csv"
    csv_filename = re.sub(r'[^\w\-_.{}]', '_', csv_filename)  # Replace invalid filename characters with underscores
    csv_filepath = os.path.join(dump_dir, csv_filename)  # Set the file path
    filtered_df.to_csv(csv_filepath, index=False)

# Write the end_date to the log file
with open(filename, "w") as file:
    file.write(end_date)

# Close the MySQL connection
conn.close()

# Create a blank file with the named "end_date{(yyyymmdd_hhmm)}.done"
done_file_name = f"{{{end_date_formatted}}}.done.txt"
done_file_path = os.path.join(dump_dir, done_file_name)
with open(done_file_path, 'w'):
    pass

print("CSV files saved in the dump directory.")
