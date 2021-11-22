from main import *
from tools import *

from flask import Flask, request, jsonify
from json import dumps
from flask_restful import Resource, Api, reqparse, abort
from flask_httpauth import HTTPBasicAuth

auth = HTTPBasicAuth()

# ===============================================================================================
#
# ===============================================================================================
@app.route('/api_patient', methods = ['POST'])
@auth.login_required
def process_api_patient():
    username = request.authorization.username
    if request.method == 'POST':
        log_to_kafka("patient", request.get_json())
        return jsonify("Thank You")
    else:
        return jsonify("ERROR")

# ===============================================================================================
#
# ===============================================================================================
@auth.verify_password
def verify_password(username, password):
	return val_user(username, password,'api_patient')
