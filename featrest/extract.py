from flask import (request, abort, jsonify)
from featuretools.mkfeat.feat_extractor import FeatureExtractor
from featuretools.mkfeat.error import Error

from extractor import Extractor


extractors = []


def _reg_extractor(extractor):
    for tid, name in enumerate(extractors, start=1):
        if extractors[tid - 1] is None:
            extractors[tid - 1] = extractor
            return tid
    extractors.append(extractor)
    return len(extractors)


def _find_extractor(tid) -> FeatureExtractor:
    if tid > 0 and len(extractors) < tid:
        return None
    return extractors[tid - 1]


def _remove_extractor(tid):
    if tid > 0 and len(extractors) < tid:
        extractors[tid - 1] = None


def start_task():
    path = request.args.get("data")
    json_in = request.json
    if path is None or json_in is None:
        abort(400)
    if 'columns' not in json_in or 'operator' not in json_in:
        abort(400)
    columns = json_in['columns']
    operators = json_in['operator']

    extractor = Extractor()
    err = extractor.start(path, columns, operators)
    if err == Error.OK:
        tid = _reg_extractor(extractor)

        return {"tid": tid}
    if err == Error.ERR_DATA_NOT_FOUND:
        abort(401)
    abort(500)


def status_task(tid):
    extractor = _find_extractor(tid)
    if extractor is None:
        abort(501)
    return {"progress": extractor.get_progress()}


def get_featureinfo(tid):
    extractor = _find_extractor(tid)
    if extractor is None:
        abort(501)
    if not extractor.is_completed():
        abort(503)

    infos = extractor.get_feature_info()
    if isinstance(infos, list):
        res = []
        for info in infos:
            res.append({"name": info[0], "type": info[1]})
        return jsonify(res)

    if infos == Error.ERR_ONGOING:
        abort(503)
    abort(500)


def save_task(tid):
    extractor = _find_extractor(tid)
    if extractor is None:
        abort(501)
    if extractor.is_running():
        abort(503)
    if not extractor.is_completed():
        abort(502)

    json_in = request.json
    if json_in is None:
        abort(400)
    if 'id' not in json_in:
        abort(400)
    path = json_in['id']
    extractor.save(path)

    return ""


def stop_task(tid):
    extractor = _find_extractor(tid)
    if extractor is None:
        abort(501)
    if not extractor.is_running():
        abort(502)
    extractor.stop()

    return ""


def remove_task(tid):
    extractor = _find_extractor(tid)
    if extractor is None:
        abort(501)
    if extractor.is_running():
        abort(503)
    extractor.cleanup()
    _remove_extractor(tid)

    return ""
