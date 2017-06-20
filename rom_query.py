# coding=UTF-8
from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route('/query', methods=['POST'])
def query():
    if request.method == "POST":
        from query import Query
        if not request.json:
            abort(400)
        q = Query(request.json)
        resp = q.parse()
        print(resp)
        dset = q.execute()

    return jsonify(dset)

if __name__ == "__main__":
    app.run()
