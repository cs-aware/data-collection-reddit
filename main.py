#! usr/bin/env python3
import praw  # Reddit library
import csv
import json
from datetime import datetime, timedelta, date
from config import credentials
import boto3
import os

from stix2 import Bundle, ObservedData, IPv4Address, UserAccount, Bundle
from stix2 import CustomObservable, properties


@CustomObservable('x-csaware-social', [
    ('source', properties.StringProperty()),
    ('title', properties.StringProperty()),
    ('text', properties.StringProperty()),
    ('subject', properties.StringProperty()),
])
class CSAwareSocial():
    pass


BUCKET_NAME = "cs-aware-data-collection"
USERS_FILE = './users.json'
FIELDS = ['subreddit', 'username', 'date', 'title', 'text', 'json']
PERIOD = 1  # Number of hours
POST_LIMIT = 50


# We want data from the last PERIOD hours
now = datetime.now()
date_from = now - timedelta(hours=PERIOD)


# Load accounts
def get_accounts(users_file):
    with open(users_file) as f:
        return json.load(f)


# Check if the post was made during the last PERIOD hours
def date_comparison(result):
    date_post = datetime.fromtimestamp(result.created_utc)
    return date_post >= date_from


def to_aws(local_filename):
    # Generate remote path
    remote_path = "%d/%02d/%02d/REDDIT/%s" % (now.year, now.month, now.day, local_filename)
    print("Uploading", remote_path)
    # Upload to AWS
    with open(local_filename, "rb") as f:
        s3 = boto3.resource('s3')
        s3.Object(BUCKET_NAME, remote_path).upload_fileobj(f)
    # Delete local copy
    os.remove(local_filename)


def main(users_file=USERS_FILE):
    # Authentication
    reddit = praw.Reddit(**credentials)

    # Load list of accounts
    subreddits = get_accounts(users_file)['user_to_follow']
    observed_data_list = []

    # CSV output file
    local_filename = "output_" + now.strftime("%Y%m%d_%H%M") + ".csv"
    with open(local_filename, mode='w') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=FIELDS)
        writer.writeheader()
        for subreddit in subreddits:
            print(subreddit)
            # We cannot get all posts, so we just get the most recent ones
            for result in reddit.subreddit(subreddit).new(limit=POST_LIMIT):
                if date_comparison(result):
                    post_date = datetime.fromtimestamp(result.created_utc)
                    writer.writerow({'subreddit': result.subreddit,  # Subreddit Name
                                     'username': result.fullname,  # Username (user ID)
                                     'date': post_date,
                                     'title': result.title,  # Post title
                                     'text': result.selftext,  # Post text (may be empty)
                                     'json': reddit.request("GET", result.permalink)  # Complete Post Json
                                    })

                    args = {
                        'source': 'reddit',
                        'title': result.title,
                        'text': result.selftext,
                        'subject': result.subreddit,
                    }
                    observed_user = UserAccount(type='user-account', user_id=result.fullname)
                    observed_object = CSAwareSocial(**args, allow_custom=True)
                    objects = {"0": observed_user, "1": observed_object}
                    observed_data = ObservedData(first_observed=post_date, last_observed=post_date, number_observed=1, objects=objects, allow_custom=True)
                    observed_data_list.append(observed_data)

    # STIX Conversion
    bundle = Bundle(observed_data_list)

    stix_filename = local_filename.replace('.csv', '.json')
    stix_output = open(stix_filename, 'w')
    stix_output.write(bundle.serialize(indent=4))
    stix_output.close()

    # Upload to AWS
    to_aws(local_filename)
    to_aws(stix_filename)


if __name__ == "__main__":
    main()
