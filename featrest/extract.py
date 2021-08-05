from flask import (request, abort)
from featuretools.mkfeat.feat_extractor import FeatureExtractor


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
    extractor.load(path, columns)

    extractor.extract_features(operators)
    tid = _reg_extractor(extractor)

    return {"tid": tid}

