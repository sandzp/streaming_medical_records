from flask import Flask
from flask_restful import Resource, Api, reqparse, abort

app = Flask(__name__)
api = Api(app)

from api_patient 	    import *
from api_lab_results 	import *
from api_admissions 	import *
from api_admissions_details 	import *

app.config.from_object('config')

if __name__ == '__main__':
	#app.run(host='0.0.0.0', ssl_context='adhoc')
	app.run(host='0.0.0.0',debug=True, port=5000)
