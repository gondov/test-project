import psycopg2
import datetime
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
 

@dag(
        schedule = '0 1 * * *',
        
        catchup=False,
        tags=["Example"],
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
        fetch_all = "SELECT * FROM omop54.care_site"
        results = cur.execute(fetch_all)
        print(results)
        return(results)
    
    # set variables
    data = test_db_conn()

TEST_ETL()
