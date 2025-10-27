from airflow.sdk import dag, task
from pendulum import datetime,duration
from ct_gov.include.etl.extract import Extractor


@dag(
    dag_id="process_ct_gov",
    start_date=datetime(2025, 10, 27),
    catchup=False,
    schedule=None,
    tags=["ctgov"]
)

def process_ct_gov(**context):
    @task
    def extract():
        def dextract():
            e=Extractor(context=context)
            for i in range(e.pages_to_load):
                e.make_request()
        return dextract


    extract = extract()


process_ct_gov()