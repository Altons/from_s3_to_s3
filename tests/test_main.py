import os
import boto3
from moto import mock_s3
from datetime import datetime
import pytest

from app.main import check_key_exists, move_from_s3_to_s3, generate_name


S3_SRC_BUCKET = "mybucket"
S3_DEST_BUCKET = "targetbucket"
FILE_NAME = "pytest.ini"
FILE_LOCATION = FILE_NAME
yyyymmdd = datetime.today().strftime('%Y-%m-%d')

@pytest.fixture(scope='function')
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'
    os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'



@pytest.fixture(scope='function')
def s3(aws_credentials):
    with mock_s3():
        yield boto3.client("s3")



def test_key_exists(s3):
    s3.create_bucket(Bucket=S3_SRC_BUCKET)

    with open(FILE_LOCATION, 'rb') as data:
        s3.upload_fileobj(data, S3_SRC_BUCKET, FILE_NAME)

    assert check_key_exists(FILE_NAME,S3_SRC_BUCKET) == True
    assert check_key_exists('i_dont_exist',S3_SRC_BUCKET) == False

def test_name_generation():
    name = 'super_test.txt'
    new_name = generate_name(name)
    assert new_name == f'super_test_{yyyymmdd}.txt'

def test_move_file_to_another_bucket(s3):
    s3.create_bucket(Bucket=S3_SRC_BUCKET)
    s3.create_bucket(Bucket=S3_DEST_BUCKET)
    with open(FILE_LOCATION, 'rb') as data:
        s3.upload_fileobj(data, S3_SRC_BUCKET, FILE_NAME)

    move_from_s3_to_s3(FILE_NAME,S3_SRC_BUCKET,S3_DEST_BUCKET)
    assert  check_key_exists(f'pytest_{yyyymmdd}.ini',S3_DEST_BUCKET) == True


