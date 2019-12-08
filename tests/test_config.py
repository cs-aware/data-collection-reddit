import boto3
from boto3 import client
from botocore.config import Config
import os


BUCKET = 'cs-aware-data-collection'
TEST_DATA_FOLDER = 'test_data/'
TEST_OUTPUT_FOLDER = 'test_data/output/'
SENDER_MUNICIPALITY = "TEST_MUNICIPALITY"
SENDER_DEVICE = "TEST_SERVER"


def open_aws_connection():
    proxy_config_file = 'proxy-config.json'
    s3 = None
    if os.path.isfile(proxy_config_file):
        proxy_config = None
        with open(proxy_config_file) as f:
            proxy_config = json.load(f)
        proxy_string = '{0}:{1}'.format(proxy_config['https'],
                                        proxy_config['port'])
        # Open connection with Amazon S3 with proxy
        # print("proxy file found: {0}".format(proxy_string))
        s3 = boto3.resource('s3', config=Config(proxies={'https': proxy_string}))
    else:
        # Open connection with Amazon S3
        # print("proxy file not found")
        s3 = boto3.resource('s3')
    return s3


def aws_put(path, filename, content, s3=None):
    """
    Sends a specific file to a specific path on S3.
    :param path: base path of the destination folder on the server
    :param filename: name of the file to send
    :param content: content of the file to send
    """
    if not s3:
        s3 = open_aws_connection()
    s3.Object(BUCKET, path + filename).put(Body=content)


def aws_get(file_path, filename=None, dst_folder=None, s3=None):
    if not s3:
        s3 = open_aws_connection()
    new_content = s3.Object(BUCKET, file_path).get()["Body"].read()
    if dst_folder:
        with open(dst_folder+filename, 'wb') as f:
            f.write(new_content)
    return new_content


def aws_delete(filename, s3=None):
    if not s3:
        s3 = open_aws_connection()
    s3.Object(BUCKET, filename).delete()


def aws_list(path, s3=None):
    if not s3:
        s3 = open_aws_connection()
    bucket = s3.Bucket(BUCKET)
    obj_list = bucket.objects.filter(Prefix=path)
    return [item.key for item in obj_list]
