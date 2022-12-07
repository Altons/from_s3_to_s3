import boto3
from datetime import datetime
import os



def aws_credentials():
    return {'AWS_ACCESS_KEY_ID':    os.environ['AWS_ACCESS_KEY_ID'],
            'AWS_SECRET_ACCESS_KEY':os.environ['AWS_SECRET_ACCESS_KEY'],
            'AWS_SECURITY_TOKEN':   os.environ['AWS_SECURITY_TOKEN'],
            'AWS_SECURITY_TOKEN':   os.environ['AWS_SESSION_TOKEN'],
            'AWS_DEFAULT_REGION':   os.environ['AWS_DEFAULT_REGION']}


def get_s3(aws_credentials: dict):
    return boto3.client(service_name='s3',
                region_name=aws_credentials['AWS_DEFAULT_REGION'],
                aws_access_key_id=aws_credentials['AWS_ACCESS_KEY_ID'],
                aws_secret_access_key=aws_credentials['AWS_SECRET_ACCESS_KEY'])


def check_key_exists(mykey, bucket):
    s3_client = get_s3(aws_credentials())
    try:
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=mykey)
        for obj in response['Contents']:
            if mykey == obj['Key']:
                return True
        return False  # no keys match
    except KeyError:
        return False  # no keys found
    except Exception as e:
        # Handle or log other exceptions such as bucket doesn't exist
        return e


def generate_name(input:str):
    # assumes that input file has an extension
    temp = input.split('.')
    yyyymmdd = datetime.today().strftime('%Y-%m-%d')
    return f'{temp[0]}_{yyyymmdd}.{temp[1]}'
    

def move_from_s3_to_s3(src_object_key:str,src_bucket:str,dest_bucket:str):
    s3_client = get_s3(aws_credentials())
    source = {'Bucket': src_bucket, 'Key': src_object_key}
    dest_object_key = generate_name(src_object_key)
    s3_client.copy_object(CopySource = source, Bucket = dest_bucket, Key = dest_object_key)
    
    

