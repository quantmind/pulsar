from flask import Flask, url_for, render_template, request
from flask import make_response, jsonify, g
import logging
import datetime
import base64
import sys

app = Flask(__name__)
app.debug=False

# Get all loggers
print logging.Logger.manager.loggerDict

# Set up logging before any requests
@app.before_first_request
def setup_logging():
    if not app.debug:
        # In production mode, add log handler to sys.stderr.
        app.logger.addHandler(logging.StreamHandler())
        stdout = logging.StreamHandler(sys.stdout)
        logging.getLogger('access').setLevel(logging.DEBUG)
        logging.getLogger('pulsar').setLevel(logging.DEBUG)
        logging.getLogger('pulsar.wsgi').setLevel(logging.DEBUG)

'''
# Hack to log requests
@app.after_request
def access_log(response):
    access = request.remote_addr + " " + \
            request.method + " " + request.url + " " + str(response.status_code) + " " + str(response.content_length)
    logging.getLogger('access').info(access)
    #print dir(request)
    #print dir(response)
    return response
'''

@app.errorhandler(404)
def not_found(e):
    return make_response("404 Page", 404)

@app.errorhandler(500)
def not_found(e):
    return make_response("500 Page", 500)


@app.route('/', methods=['GET'])
def add_org():
    return "Example"

if __name__ == '__main__':
        app.run()
