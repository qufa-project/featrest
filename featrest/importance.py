from flask import (request, abort, jsonify)

from featuretools.mkfeat.error import Error
from analyzer import Analyzer


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
    if tid > 0 and len(_analyzers) <= tid:
        _analyzers[tid - 1] = None


def start_task():
    path_data = request.args.get("data")
    path_label = request.args.get("label")
    if path_data is None or path_label is None:
        abort(400)

    analyzer = Analyzer()
    err = analyzer.start(path_data, path_label)
    if err == Error.OK:
        tid = _reg_analyzer(analyzer)
        return {"tid": tid}

    if err == Error.ERR_DATA_NOT_FOUND or err == Error.ERR_LABEL_NOT_FOUND:
        abort(401)
    abort(500)


def status_task(tid):
    analyzer = _find_analyzer(tid)
    if analyzer is None:
        abort(501)
    return {"progress": analyzer.get_progress()}


def get_importance(tid):
    analyzer = _find_analyzer(tid)
    if analyzer is None:
        abort(501)
    if not analyzer.is_completed():
        abort(503)

    importance = analyzer.get_importance()
    if isinstance(importance, list):
        return jsonify(importance)

    if importance == Error.ERR_ONGOING:
        abort(503)
    abort(500)


def stop_task(tid):
    analyzer = _find_analyzer(tid)
    if analyzer is None:
        abort(501)
    if not analyzer.is_running():
        abort(502)
    analyzer.stop()

    return ""


def remove_task(tid):
    analyzer = _find_analyzer(tid)
    if analyzer is None:
        abort(501)
    if analyzer.is_running():
        abort(503)
    analyzer.cleanup()
    _remove_analyzer(tid)

    return ""
