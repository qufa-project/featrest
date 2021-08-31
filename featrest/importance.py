from flask import (request, jsonify)

from featuretools.mkfeat.error import Error
from analyzer import Analyzer
from errpage import (error_page, error_page_wrong_json, error_page_data_not_found, error_page_label_not_found,
                     error_page_no_task, error_page_not_completed, error_page_stopped, error_page_stop_failed,
                     error_page_unknown, error_page_wrong_uri, error_page_column_count_mismatch, error_page_column_type,
                     error_page_data_label_count_mismatch, ErrorSvc)

from tmp_fpath import get_tmp_fpath
import s3
import util

_analyzers = []


def _reg_analyzer(analyzer):
    for tid, name in enumerate(_analyzers, start=1):
        if _analyzers[tid - 1] is None:
            _analyzers[tid - 1] = analyzer
            return tid
    _analyzers.append(analyzer)
    return len(_analyzers)


def _find_analyzer(tid) -> Analyzer:
    if tid > 0 and len(_analyzers) < tid:
        return None
    return _analyzers[tid - 1]


def _remove_analyzer(tid):
    if 0 < tid <= len(_analyzers):
        _analyzers[tid - 1] = None


def start_task():
    json_in = request.json
    if json_in is None:
        return error_page_wrong_json()
    if 'data' not in json_in:
        return error_page(400, Error.ERR_INVALID_ARG, "no 'data' object found in JSON request body")
    json_data = json_in['data']
    if 'uri' not in json_data or 'columns' not in json_data:
        return error_page(400, Error.ERR_INVALID_ARG, "no 'uri' or 'columns' found in data object")

    uri_data = json_data['uri']
    columns_data = json_data['columns']

    tmp_fpath_data = get_tmp_fpath()
    tmp_fpath_label = None
    res = s3.download(uri_data, tmp_fpath_data)
    if res == Error.ERR_DATA_NOT_FOUND:
        return error_page_data_not_found()
    elif res == ErrorSvc.ERR_URI_FORMAT:
        return error_page_wrong_uri(uri_data)
    elif res != Error.OK:
        return error_page_unknown()

    columns_label = None
    if 'label' in json_in:
        json_label = json_in['label']
        if 'uri' not in json_label or 'columns' not in json_label:
            return error_page(400, Error.ERR_INVALID_ARG, "no 'uri' or 'columns' found in label object")
        uri_label = json_label['uri']
        tmp_fpath_label = get_tmp_fpath()
        res = s3.download(uri_label, tmp_fpath_label)
        if res == Error.ERR_DATA_NOT_FOUND:
            util.remove(tmp_fpath_data)
            return error_page_label_not_found()
        elif res == ErrorSvc.ERR_URI_FORMAT:
            util.remove(tmp_fpath_data)
            return error_page_wrong_uri(uri_data)
        elif res != Error.OK:
            util.remove(tmp_fpath_data)
            return error_page_unknown()

        columns_label = json_label['columns']

    analyzer = Analyzer(tmp_fpath_data, columns_data, tmp_fpath_label, columns_label)
    err = analyzer.start()
    if err == Error.OK:
        tid = _reg_analyzer(analyzer)
        return {"tid": tid}

    util.remove(tmp_fpath_data)
    util.remove(tmp_fpath_label)

    if err == Error.ERR_DATA_NOT_FOUND:
        return error_page_data_not_found()
    if err == Error.ERR_LABEL_NOT_FOUND:
        return error_page_label_not_found()
    if err == Error.ERR_COLUMN_COUNT_MISMATCH:
        return error_page_column_count_mismatch()
    if err == Error.ERR_COLUMN_TYPE:
        return error_page_column_type()
    if err == Error.ERR_DATA_LABEL_COUNT_MISMATCH:
        return error_page_data_label_count_mismatch()
    return error_page_unknown()


def status_task(tid):
    analyzer = _find_analyzer(tid)
    if analyzer is None:
        return error_page_no_task(tid)
    prog = analyzer.get_progress()
    if prog is None:
        return error_page_stopped(tid)

    return {"progress": prog}


def get_importance(tid):
    analyzer = _find_analyzer(tid)
    if analyzer is None:
        return error_page_no_task(tid)

    importance = analyzer.get_importance()
    if isinstance(importance, list):
        return jsonify(importance)

    if importance == Error.ERR_ONGOING:
        return error_page_not_completed(tid)
    elif importance == ErrorSvc.ERR_STOPPED:
        return error_page_stopped(tid)
    return error_page_unknown()


def stop_task(tid):
    analyzer = _find_analyzer(tid)
    if analyzer is None:
        return error_page_no_task(tid)
    err = analyzer.stop()
    if err == Error.OK:
        return ""
    if err == ErrorSvc.ERR_STOPPED:
        return error_page_stopped(tid)
    elif err == Error.ERR_ONGOING:
        return error_page_stop_failed(tid)
    return error_page_unknown()


def remove_task(tid):
    analyzer = _find_analyzer(tid)
    if analyzer is None:
        return error_page_no_task(tid)
    err = analyzer.cleanup()
    if err == Error.OK:
        _remove_analyzer(tid)
        return ""
    if err == Error.ERR_ONGOING:
        return error_page_not_completed(tid)
    return error_page_unknown()
