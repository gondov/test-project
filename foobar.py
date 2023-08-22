import psycopg2
import datetime
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
 

@dag(       
        start_date=datetime.datetime(2021, 1, 1), schedule="@daily"
)
def TEST_ETL():

    @task
    def test_db_conn():

        # connect to the DB
        conn = psycopg2.connect(
            host="172.16.1.18",
            port=5432,
            database="etl",
            user="postgres",
            password="postgres"
        )
        cur = conn.cursor()

        # fetch data
        fetch_all = "SELECT * FROM etl2_cdm.cdm__person;"
        results = cur.execute(fetch_all)
        print(results)
        return(results)
    
    # set variables
    data = test_db_conn()

TEST_ETL()
