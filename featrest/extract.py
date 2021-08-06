from flask import (request, abort)
from featuretools.mkfeat.feat_extractor import FeatureExtractor
from featuretools.mkfeat.error import Error


extractors = []


def _reg_extractor(extractor):
    for tid, name in enumerate(extractors, start=1):
        if extractors[tid - 1] is None:
            extractors[tid - 1] = extractor
            return tid
    extractors.append(extractor)
    return len(extractors)


def start_task():
    path = request.args.get("data")
    json_in = request.json
    if path is None or json_in is None:
        abort(400)
    if 'columns' not in json_in or 'operator' not in json_in:
        abort(400)
    columns = json_in['columns']
    operators = json_in['operator']

    extractor = FeatureExtractor()
    err = extractor.load(path, columns)
    if err == Error.OK:
        extractor.extract_features(operators)
        tid = _reg_extractor(extractor)

        return {"tid": tid}
    if err == Error.ERR_DATA_NOT_FOUND:
        abort(401)
    abort(500)
