from flask import Flask
import extract


app = Flask("featrest")


@app.route("/extract", methods=['POST'])
def create_extract_task():
    return extract.start_task()


@app.route("/extract/<int:tid>/status", methods=['GET'])
def status_extract_task(tid):
    return extract.status_task(tid)


@app.route("/extract/<int:tid>/featureinfo", methods=['GET'])
def get_featureinfo(tid):
    return extract.get_featureinfo(tid)


@app.route("/extract/<int:tid>", methods=['DELETE'])
def remove_extract_task(tid):
    return extract.remove_task(tid)