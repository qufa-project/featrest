from flask import Flask
from . import extract
from . import importance


app = Flask(__name__)


@app.route("/extract", methods=['POST'])
def create_extract_task():
    return extract.start_task()


@app.route("/extract/<int:tid>/status", methods=['GET'])
def status_extract_task(tid):
    return extract.status_task(tid)


@app.route("/extract/<int:tid>/featureinfo", methods=['GET'])
def get_featureinfo(tid):
    return extract.get_featureinfo(tid)


@app.route("/extract/<int:tid>/save", methods=['PUT'])
def save_extract_task(tid):
    return extract.save_task(tid)


@app.route("/extract/<int:tid>/stop", methods=['PUT'])
def stop_extract_task(tid):
    return extract.stop_task(tid)


@app.route("/extract/<int:tid>", methods=['DELETE'])
def remove_extract_task(tid):
    return extract.remove_task(tid)


@app.route("/importance", methods=['POST'])
def create_importance_task():
    return importance.start_task()


@app.route("/importance/<int:tid>/status", methods=['GET'])
def status_importance_task(tid):
    return importance.status_task(tid)


@app.route("/importance/<int:tid>", methods=['GET'])
def get_importance(tid):
    return importance.get_importance(tid)


@app.route("/importance/<int:tid>/stop", methods=['PUT'])
def stop_importance_task(tid):
    return importance.stop_task(tid)


@app.route("/importance/<int:tid>", methods=['DELETE'])
def remove_importance_task(tid):
    return importance.remove_task(tid)
