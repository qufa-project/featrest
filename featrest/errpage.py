from flask import jsonify
from typing import Union
from enum import Enum

from featuretools.mkfeat.error import Error


class ErrorSvc(str, Enum):
    ERR_STOPPED = "ERR_STOPPED"
    ERR_COMPLETED = "ERR_COMPLETED"
    ERR_NO_TASK = "ERR_NO_TASK"
    ERR_URI_FORMAT = "ERR_URI_FORMAT"


def error_page(code: int, err: Union[ErrorSvc, Error], errmsg: str):
    response = jsonify({
        'errcode': err,
        'errmsg': errmsg,
    })
    response.status_code = code
    return response


def error_page_wrong_json():
    return error_page(400, Error.ERR_INVALID_ARG, "Wrong JSON body format")


def error_page_wrong_uri(uri):
    return error_page(400, Error.ERR_INVALID_ARG, "Wrong S3 URI format: {}".format(uri))


def error_page_column_count_mismatch():
    return error_page(400, Error.ERR_COLUMN_COUNT_MISMATCH, "The numbers of columns do not match")


def error_page_data_not_found():
    return error_page(400, Error.ERR_DATA_NOT_FOUND, "Data file not found")


def error_page_label_not_found():
    return error_page(400, Error.ERR_LABEL_NOT_FOUND, "Label file not found")


def error_page_no_task(tid: int):
    return error_page(400, ErrorSvc.ERR_NO_TASK, "There no task with tid: {}".format(tid))


def error_page_not_completed(tid: int):
    return error_page(400, Error.ERR_ONGOING, "task(tid: {}) has not been completed".format(tid))


def error_page_stopped(tid: int):
    return error_page(400, ErrorSvc.ERR_STOPPED, "task(tid: {}) was stopped".format(tid))


def error_page_already_completed(tid: int):
    return error_page(400, ErrorSvc.ERR_COMPLETED, "task(tid: {}) has been already completed".format(tid))


def error_page_stop_failed(tid: int):
    return error_page(500, Error.ERR_ONGOING, "failed to stop task(tid: {})".format(tid))


def error_page_column_type():
    return error_page(500, Error.ERR_COLUMN_TYPE,
                      "There exists a wrong column whose values are inconsistent with column type")


def error_page_data_label_count_mismatch():
    return error_page(500, Error.ERR_DATA_LABEL_COUNT_MISMATCH,
                      "The record count of data does not match with the count of the label file")


def error_page_unknown():
    return error_page(500, Error.ERR_GENERAL, "Unknown error")
