import pandas as pd
from datetime import datetime, timedelta
import argparse
import os
import glob
import boto3
import time

def fix_time_occ(time_val):
    """
    Description: Function used to fix time occ field
    Args:
        - time_val : int value of time
    Returns:
        - modified integer as a string e.g "09:00"
    """
    time_val_str = str(time_val)

    if len(time_val_str) == 4:
        return time_val_str[:2] + ":" + time_val_str[2:len(time_val_str)]
    if len(time_val_str) == 3:
        return "0" + time_val_str[:1] + ":" + time_val_str[1:len(time_val_str)]



def clear_directory():
    """
    Description: Clears the la_crime* files from data/output
    Args: None
    Returns: None
    """
    file_path = glob.glob("data/output/la_crime*")

    if file_path:
        for f in file_path:
            os.remove(f)


def push_to_s3(file_name, delay=0):
    """
    Description: Function used to upload files to S3.
    Args: 
        - file_name: name of file to push to s3 (string)
        - delay: The number of seconds before uploading (int)
    """
    BUCKET_NAME = "la-crime-snowflake-df"

    client = boto3.client("s3")
    
    try:
        if delay != 0:
            time.sleep(delay)
        client.upload_file(f"{file_name}",
                            BUCKET_NAME,
                            f"raw/{os.path.basename(file_name)}")
    except Exception as e:
        print(e)



if __name__ == "__main__":
    """
    Script used to generate daily csv files and upload them to S3 with a delay interval
    """
    # Clear any old files from output directory
    clear_directory()

    # Parse arguments
    parser = argparse.ArgumentParser(description='Split csv file into parts by Date')
    parser.add_argument('-s', '--start', type=str, default="2023-01-01" , help='Start Date in YYYY-MM-DD format')
    parser.add_argument('-e', '--end', type=str, default="2023-01-05", help='Start Date in YYYY-MM-DD format')
    parser.add_argument('-d', '--delay', type=int, default=0, help='Number of seconds to delay upload to S3')
    parser.add_argument('-P', '--push', type=bool, default=False, help='Push Files to S3 Bucket')

    args = parser.parse_args()

    #Load dataset into memory
    df = pd.read_csv("data/Crime_Data_from_2020_to_Present.csv")

    #Convert Fields to Date
    df['Date Rptd'] = pd.to_datetime(df['Date Rptd'])

    df["DateTime OCC"] = pd.to_datetime(df["DATE OCC"].str.slice(start=0, stop=10).str.strip() + " " +  df["TIME OCC"].apply(fix_time_occ))

    # Date range required
    start_date = datetime.strptime(args.start, "%Y-%m-%d")
    end_date = datetime.strptime(args.end, "%Y-%m-%d")


    while start_date < end_date:

        tmp_start = start_date
        tmp_end = tmp_start + timedelta(days=1)

        tmp_df = df[(df['Date Rptd'] >= pd.Timestamp(tmp_start.year, tmp_start.month, tmp_start.day)) & (df['Date Rptd'] < pd.Timestamp(tmp_end.year, tmp_end.month, tmp_end.day))]

        tmp_df.to_csv(f"data/output/la_crime-{tmp_start.year}-{tmp_start.month}-{tmp_start.day}.csv")
        tmp_start = tmp_start + timedelta(days=1)
        start_date = start_date + timedelta(days=1)


    if args.push:
        file_list = glob.glob("data/output/la_crime*")
        for f in file_list:
            push_to_s3(f, args.delay)






