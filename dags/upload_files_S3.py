import logging
import boto3
from botocore.exceptions import ClientError
import os
from airflow.models import Variable

AWS_ACCESS_KEY = Variable.get("AWS_ACCESS_KEY")
AWS_SECRET_KEY = Variable.get("AWS_SECRET_KEY")


def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
    )

    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


# # The following code was used to check if the files were in the S3 bucket
# s3 = boto3.resource("s3")
# my_bucket = s3.Bucket("cohorte-agosto-38d749a7")
# for file in my_bucket.objects.all():
#     print(file.key)
