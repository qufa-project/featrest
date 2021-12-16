import os
import boto3
from botocore.exceptions import ClientError

from urllib.parse import urlparse

from featuretools.mkfeat.error import Error

from .errpage import ErrorSvc

_aws_s3 = boto3.client('s3')


def download(uri, fpath):
    bucket, objname = _parse_uri(uri)
    if isinstance(bucket, ErrorSvc):
        return bucket
    try:
        _aws_s3.download_file(bucket, objname, fpath)
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return Error.ERR_DATA_NOT_FOUND
        return Error.ERR_GENERAL

    return Error.OK


def upload(uri, fpath):
    bucket, objname = _parse_uri(uri)
    if isinstance(bucket, ErrorSvc):
        return bucket
    try:
        _aws_s3.upload_file(fpath, bucket, objname)
    except ClientError as e:
        return Error.ERR_GENERAL

    return Error.OK


def _parse_uri(uri):
    res = urlparse(uri)
    if res.scheme != 's3' or not res.netloc or res.path is None:
        return ErrorSvc.ERR_URI_FORMAT, None
    return res.netloc, res.path.strip(os.sep)
