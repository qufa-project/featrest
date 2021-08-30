import typing

from flask import (request, jsonify)
from featuretools.mkfeat.feat_extractor import FeatureExtractor
from featuretools.mkfeat.error import Error

import s3
import util

from extractor import Extractor
from errpage import (error_page, error_page_wrong_json, error_page_data_not_found, error_page_no_task,
                     error_page_not_completed, error_page_already_completed, error_page_stopped, error_page_unknown,
                     error_page_column_count_mismatch, error_page_wrong_uri, error_page_column_type, ErrorSvc)
from tmp_fpath import get_tmp_fpath


extractors = []


def _reg_extractor(extractor):
    for tid, name in enumerate(extractors, start=1):
        if extractors[tid - 1] is None:
            extractors[tid - 1] = extractor
            return tid
    extractors.append(extractor)
    return len(extractors)


def _find_extractor(tid) -> typing.Optional[FeatureExtractor]:
    if tid > 0 and len(extractors) < tid:
        return None
    return extractors[tid - 1]


def _remove_extractor(tid):
    if 0 < tid <= len(extractors):
        extractors[tid - 1] = None


def start_task():
    json_in = request.json
    if json_in is None:
        return error_page_wrong_json()
    if 'data' not in json_in or 'operator' not in json_in:
        return error_page(400, Error.ERR_INVALID_ARG, "no 'data' object found in JSON request body")
    json_data = json_in['data']
    if 'uri' not in json_data or 'columns' not in json_data:
        return error_page(400, Error.ERR_INVALID_ARG, "no 'uri' or 'columns' found in data object")

    uri = json_data['uri']
    columns = json_data['columns']
    operators = json_in['operator']

    tmp_fpath = get_tmp_fpath()
    res = s3.download(uri, tmp_fpath)
    if res == Error.ERR_DATA_NOT_FOUND:
        return error_page_data_not_found()
    elif res == ErrorSvc.ERR_URI_FORMAT:
        return error_page_wrong_uri(uri)
    elif res != Error.OK:
        return error_page_unknown()

    extractor = Extractor(tmp_fpath, columns)
    err = extractor.start(operators)
    if err == Error.OK:
        tid = _reg_extractor(extractor)
        return {"tid": tid}

    if err == Error.ERR_DATA_NOT_FOUND:
        return error_page_data_not_found()
    elif err == Error.ERR_COLUMN_COUNT_MISMATCH:
        return error_page_column_count_mismatch()
    elif err == Error.ERR_COLUMN_HAS_NO_NAME_OR_TYPE:
        errmsg = "There exists a column which has no name or type"
    elif err == Error.ERR_COLUMN_BAD:
        errmsg = "Key column is not unique or wrong"
    elif err == Error.ERR_COLUMN_MULTI_KEY:
        errmsg = "There are multiple key columns"
    elif err == Error.ERR_COLUMN_MULTI_LABEL:
        errmsg = "There are multiple label columns"
    elif err == Error.ERR_COLUMN_KEY_AND_LABEL:
        errmsg = "There is a column which has key and label attribute"
    elif err == Error.ERR_COLUMN_TYPE:
        return error_page_column_type()
    else:
        return error_page_unknown()
    return error_page(400, err, errmsg)


def status_task(tid):
    extractor = _find_extractor(tid)
    if extractor is None:
        return error_page_no_task(tid)
    prog = extractor.get_progress()
    if prog is None:
        return error_page_stopped(tid)

    return {"progress": prog}


def get_featureinfo(tid):
    extractor = _find_extractor(tid)
    if extractor is None:
        return error_page_no_task(tid)

    infos = extractor.get_feature_info()
    if isinstance(infos, list):
        res = []
        for info in infos:
            res.append({"name": info[0], "type": info[1]})
        return jsonify(res)

    if infos is None:
        return error_page_stopped(tid)
    if infos == Error.ERR_ONGOING:
        return error_page_not_completed(tid)
    return error_page_unknown()


def save_task(tid):
    json_in = request.json
    if json_in is None:
        return error_page_wrong_json()
    if 'uri' not in json_in:
        return error_page(400, Error.ERR_INVALID_ARG, "no 'uri' found in JSON body")
    extractor = _find_extractor(tid)
    if extractor is None:
        return error_page_no_task(tid)
    else:
        tmp_fpath = get_tmp_fpath()
        err = extractor.save(tmp_fpath)
        if err == Error.OK:
            uri = json_in['uri']
            res = s3.upload(uri, tmp_fpath)
            util.remove(tmp_fpath)
            if res == ErrorSvc.ERR_URI_FORMAT:
                return error_page_wrong_uri(uri)
            elif res != Error.OK:
                return error_page_unknown()
            return ""
        util.remove(tmp_fpath)
        if err == ErrorSvc.ERR_STOPPED:
            return error_page_stopped(tid)
        if err == Error.ERR_ONGOING:
            return error_page_not_completed(tid)
        return error_page_unknown()


def stop_task(tid):
    extractor = _find_extractor(tid)
    if extractor is None:
        return error_page_no_task(tid)
    else:
        err = extractor.stop()
        if err == Error.OK:
            return ""
        if err == ErrorSvc.ERR_STOPPED:
            return error_page_stopped(tid)
        if err == ErrorSvc.ERR_COMPLETED:
            return error_page_already_completed(tid)
        return error_page_unknown()


def remove_task(tid):
    extractor = _find_extractor(tid)
    if extractor is None:
        return error_page_no_task(tid)
    else:
        err = extractor.cleanup()
        if err == Error.OK:
            _remove_extractor(tid)
            return ""
        if err == Error.ERR_ONGOING:
            return error_page_not_completed(tid)
        return error_page_unknown()
