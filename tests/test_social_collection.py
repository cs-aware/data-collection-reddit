import sys

sys.path.insert(0, '../')

from test_config import *
from main import main as reddit_main
from datetime import date


TEST_FILE = 'reddit.csv'


def main():
    reddit_main('../users.json')

    today = date.today()
    path = "%d/%02d/%02d/REDDIT/" % (today.year, today.month, today.day)

    s3 = open_aws_connection()
    for filename in aws_list(path, s3):
        file_content = aws_get(filename, TEST_FILE, TEST_OUTPUT_FOLDER, s3)
        if not file_content:
            return False

    return True


if __name__ == "__main__":
    result = main()
    print("Test passed:", result)
