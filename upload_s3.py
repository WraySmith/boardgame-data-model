import configparser
import logging
import os

import boto3
from botocore.exceptions import ClientError


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_s3_client(user="default", region=None, use_cfg=False):
    """
    Creates and returns an AWS client session. Has the ability to load
    an acces ID and key from a config file (if using in a notebook env
    such as Google colab).

    Args:
        user (str): AWS user (default="default")
        region (str): region to create S3 client in (default=None)
        use_cfg (bool): use cfg file for AWS access key id and
            secret access key

    Returns:
        AWS boto3 client session
    """
    if use_cfg:
        session = boto3.Session()
        s3_client = session.client(
            "s3",
            region_name=region,
            aws_access_key_id=config['AWS']['AWS_ACCESS_KEY_ID'],
            aws_secret_access_key=config['AWS']['AWS_SECRET_ACCESS_KEY']
        )
    else:
        session = boto3.Session(profile_name=user)
        s3_client = session.client(
            "s3",
            region_name=region,
        )

    return s3_client


def create_bucket(s3_client, bucket_name, region=None):
    """
    Create an S3 bucket in a specified region. If a region is not 
    specified, the bucket is created in the S3 default region.

    Args:
        s3_client: client created using `create_s3_client()`
        bucket_name (str): Bucket name to create
        region (str): region to create bucket in, e.g., 'us-west-2'
    Returns
        True if bucket created, else False
    """

    try:
        if region is None:
            s3_client.create_bucket(ACL="private", Bucket=bucket_name)
        else:
            location = {"LocationConstraint": region}
            s3_client.create_bucket(
                ACL="private",
                Bucket=bucket_name,
                CreateBucketConfiguration=location,
            )
    except ClientError as e:
        logging.error(e)
        return False
    return True


def upload_file(s3_client, file_name, bucket_name, object_name=None):
    """
    Upload a file to an S3 bucket

    Args:
        file_name (str): File to upload
        bucket_name (str): Bucket to upload to
        object_name (str): S3 object name. 
            If not specified then file_name is used
    Returns: 
        True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    try:
        response = s3_client.upload_file(file_name, bucket_name, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def setup_project(dirpath, bucket_name, region, user="default", use_cfg=False):
    """
    Setup the Board Game Dataset Model raw data and output directory in S3.

    Args:
        dirpath (str): Raw directory to upload to S3 bucket
        bucket_name (str): Bucket name to upload data to
        region (str): region to create bucket in, e.g., 'us-west-2'
        user (str): AWS user (default="default")
        use_cfg (bool): use cfg file for AWS access key id and
            secret access key
    Returns: 
        None. Creates buckets and uploads data to S3.
    """
    s3_client = create_s3_client(user=user, region=region, use_cfg=use_cfg)
    create_bucket(s3_client, bucket_name, region)
    s3_client.put_object(Bucket=bucket_name, Key="output/")

    root_dir = os.path.dirname(dirpath)
    for (root, dirs, files) in os.walk(dirpath, topdown=True):
        for file in files:
            filepath = os.path.join(root, file)
            new_root = root.replace(root_dir + "/", "")
            namepath = os.path.join(new_root, file)
            upload_file(s3_client, filepath, bucket_name, object_name=namepath)


if __name__ == "__main__":

    setup_project(
        "./data/raw",
        "data-model-test-project",
        "us-west-2",
        use_cfg=True
    )