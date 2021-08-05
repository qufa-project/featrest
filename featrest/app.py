from flask import Flask
import extract


app = Flask("featrest")


@app.route("/extract", methods=['POST'])
def create_extract_task():
    return extract.start_task()
