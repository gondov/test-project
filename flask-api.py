import flask
import subprocess
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

@dag(
        schedule = '0 1 * * *',
        start_date = days_ago(1),
        catchup=False,
        tags=["Example"],

)
def FLASK():
    @task()
    def make_flusk():
        # create the Flask app
        app = flask.Flask(__name__)
        # router
        @app.route('/', methods = ['GET'])
        def index():
            subprocess.run(["airflow", "dags", 'test', 'dbt_dag'])

        app.run(host="0.0.0.0", debug=True, port=5555)

    make_flusk()

FLASK()
