from flask import Flask
from flask import send_file

app = Flask(__name__)


@app.route('/logo')
def downloadFile():
    path = "/opt/bst/bst.png"
    return send_file(path, as_attachment=True)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
