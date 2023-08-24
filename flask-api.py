import flask

import os

import subprocess

 

# create the Flask app

app = flask.Flask(__name__)

 

# router

@app.route('/', methods = ['GET'])

def index():

    subprocess.run(["airflow", "dags", 'test', 'dbt_dag'])

 

if __name__ == '__main__':

    app.run(host="0.0.0.0", debug=True, port=5555)
