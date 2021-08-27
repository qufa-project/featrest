import typing

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
        abort(400)
    if 'data' not in json_in or 'operator' not in json_in:
        abort(400)
    json_data = json_in['data']
    if 'uri' not in json_data or 'columns' not in json_data:
        abort(400)

    path = json_data['uri']
    columns = json_data['columns']
    operators = json_in['operator']

    extractor = Extractor(path, columns)
    err = extractor.start(operators)
    if err == Error.OK:
        tid = _reg_extractor(extractor)

        return {"tid": tid}
    if err == Error.ERR_DATA_NOT_FOUND:
        abort(501)
    elif err == Error.ERR_COLUMN_TYPE:
        abort(502)
    elif err == Error.ERR_COLUMN_COUNT_MISMATCH:
        abort(503)
    elif err == Error.ERR_COLUMN_HAS_NO_NAME_OR_TYPE:
        abort(504)
    elif err == Error.ERR_COLUMN_NO_KEY:
        abort(505)
    elif err == Error.ERR_COLUMN_MULTI_KEY:
        abort(506)
    elif err == Error.ERR_COLUMN_MULTI_LABEL:
        abort(507)
    elif err == Error.ERR_COLUMN_KEY_AND_LABEL:
        abort(508)
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

    infos = extractor.get_feature_info()
    if isinstance(infos, list):
        res = []
        for info in infos:
            res.append({"name": info[0], "type": info[1]})
        return jsonify(res)

    if infos == Error.ERR_STOPPED:
        abort(502)
    if infos == Error.ERR_ONGOING:
        abort(503)
    abort(500)


def save_task(tid):
    json_in = request.json
    if json_in is None:
        abort(400)
    if 'uri' not in json_in:
        abort(400)
    extractor = _find_extractor(tid)
    if extractor is None:
        abort(501)
    else:
        path = json_in['uri']
        err = extractor.save(path)
        if err == Error.OK:
            return ""
        if err == Error.ERR_STOPPED:
            abort(502)
        if err == Error.ERR_ONGOING:
            abort(503)
        abort(500)


def stop_task(tid):
    extractor = _find_extractor(tid)
    if extractor is None:
        abort(501)
    else:
        err = extractor.stop()
        if err == Error.OK:
            return ""
        if err == Error.ERR_STOPPED:
            abort(502)
        abort(500)


def remove_task(tid):
    extractor = _find_extractor(tid)
    if extractor is None:
        abort(501)
    else:
        err = extractor.cleanup()
        if err == Error.OK:
            _remove_extractor(tid)
            return ""
        if err == Error.ERR_ONGOING:
            abort(503)
        abort(500)
