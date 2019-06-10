#! usr/bin/env python3
import praw  # Reddit library
import csv
import json
from datetime import datetime, timedelta, date
from config import credentials
import boto3
import os

BUCKET_NAME = "cs-aware-data-collection"
FIELDS = ['subreddit', 'username', 'date', 'title', 'json']

# authentication
reddit = praw.Reddit(**credentials)

# we want data for the last 12 hour
today = datetime.today()
date_now = today - timedelta(hours=12)

# load accounts
def load_screen_names():
    with open('./users.json') as f:
        return json.load(f)

# comparison the time post and last 12 hours
def date_comparison(result):
    date_post = datetime.fromtimestamp(result.created_utc)
    return date_post >= date_now

def main():
    # create list of accounts
    list_users = load_screen_names()['user_to_follow']

    # create file CSV
    local_filename = "output_" + today.strftime("%Y%m%d_%H%M") + ".csv"
    with open(local_filename, mode='w') as csv_file:
        # create a filednames in csv
        
        # open write mode
        writer = csv.DictWriter(csv_file, fieldnames=FIELDS)
        writer.writeheader()
        for username in list_users:
            print(username)
            # limit is a number of post for
            for result in reddit.subreddit(username).new(limit=50):
                # comparison
                if date_comparison(result):
                    # write on file
                    writer.writerow({'subreddit': result.subreddit,  # name Subreddit
                                     'username': result.fullname,  # user_name (ID)
                                     'date': datetime.fromtimestamp(result.created_utc),
                                     # trasform date from timestamp to Datetime
                                     'title': result.title,  # Title post
                                     'json': reddit.request("GET", result.permalink)  # Complete Json
                                     })


    output_filename = "%d/%02d/%d/REDDIT/%s" % (today.year, today.month, today.day, local_filename)

    # Create connection and write file in Amazon S3
    print("upload")
    with open(local_filename, "rb") as f:
        s3 = boto3.resource('s3')
        s3.Object(BUCKET_NAME, output_filename).upload_fileobj(f)

    os.remove(local_filename)

if __name__ == "__main__":
    main()
