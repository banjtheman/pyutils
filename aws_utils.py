import json
import logging
from typing import Type, Union, Dict, Any, List

import boto3
from botocore.exceptions import ClientError


s3 = boto3.client("s3")


def upload_file_to_s3(file_name: str, bucket: str, object_name: str = None) -> bool:
    """
    Purpose:
        Uploads a file to s3
    Args/Requests:
         file_name: name of file
         bucket: s3 bucket name
         object_name: s3 name of object
    Return:
        Status: True if uploaded, False if failure
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    try:
        response = s3.upload_file(file_name, bucket, object_name)
        logging.info(response)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def get_file_metadata_from_s3(bucket: str, prefix: str = None) -> Dict[str:Any]:
    """
    Purpose:
        gets metadata of files from s3 bucket
    Args/Requests:
         bucket: The bucket to use
         prefix: the prefix to use
    Return:
        json object with files
    """
    jsonResp = []

    paginator = s3.get_paginator("list_objects_v2")

    if prefix is not None:
        page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)
    else:
        page_iterator = paginator.paginate(Bucket=bucket)

    for bucket in page_iterator:
        for file in bucket["Contents"]:

            s3obj = {}
            # logging.info(file["Key"])
            s3obj["name"] = file["Key"].replace(prefix, "")
            try:
                metadata = s3.head_object(Bucket=bucket, Key=file["Key"])
                # logging.info(metadata)

                s3obj["content_type"] = metadata["ContentType"]
                s3obj["content_length"] = metadata["ContentLength"]
                s3obj["last_modified"] = metadata["LastModified"]

                jsonResp.append(s3obj)

            except:
                logging.error("Failed {}".format(file["Key"]))

    return jsonResp


def download_file_from_s3(
    file_name: str,
    bucket: str,
    s3_key: str,
) -> bool:
    """
    Purpose:
        Download file from s3
    Args/Requests:
         file_name: name to save file
         bucket: S3 bucket to download from
         s3_key: name of file in s3
    Return:
        status 0 if passed, -1 if fail
    """

    try:
        with open(file_name, "wb") as f:
            s3.download_fileobj(bucket, s3_key, f)
            return True
    except Exception as error:
        logging.error(error)
        return False