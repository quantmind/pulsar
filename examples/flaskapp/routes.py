from flask import Flask, url_for, render_template, request
from flask import make_response, jsonify, g
import logging
import datetime
import base64
import sys


def app():
    app = Flask(__name__)

    @app.errorhandler(404)
    def not_found(e):
        return make_response("404 Page", 404)

    @app.errorhandler(500)
    def not_found(e):
        return make_response("500 Page", 500)

    @app.route('/', methods=['GET'])
    def add_org():
        return "Flask Example"

    return app


if __name__ == '__main__':
    app().run()
