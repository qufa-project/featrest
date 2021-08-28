from flask import (request, jsonify)

from featuretools.mkfeat.error import Error
from analyzer import Analyzer
from errpage import (error_page, error_page_wrong_json, error_page_data_not_found, error_page_label_not_found,
                     error_page_no_task, error_page_not_completed, error_page_stopped, error_page_stop_failed,
                     error_page_unknown, ErrorSvc)


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

    path_data = json_data['uri']
    columns_data = json_data['columns']

    path_label = None
    columns_label = None
    if 'label' in json_in:
        json_label = json_in['label']
        if 'uri' not in json_label or 'columns' not in json_label:
            return error_page(400, Error.ERR_INVALID_ARG, "no 'uri' or 'columns' found in label object")
        path_label = json_label['uri']
        columns_label = json_label['columns']

    analyzer = Analyzer(path_data, columns_data, path_label, columns_label)
    err = analyzer.start()
    if err == Error.OK:
        tid = _reg_analyzer(analyzer)
        return {"tid": tid}

    if err == Error.ERR_DATA_NOT_FOUND:
        return error_page_data_not_found()
    if err == Error.ERR_LABEL_NOT_FOUND:
        return error_page_label_not_found()
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
