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
@app.route('/api_admissions', methods = ['POST'])
@auth.login_required
def process_api_admissions():
    username = request.authorization.username
    if request.method == 'POST':
        log_to_kafka("admissions", request.get_json())        
        return jsonify("Thank You")
    else:
        return jsonify("ERROR")

# ===============================================================================================
#
# ===============================================================================================
@auth.verify_password
def verify_password(username, password):
	return val_user(username, password,'api_admissions')
