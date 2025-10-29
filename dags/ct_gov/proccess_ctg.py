from airflow.sdk import dag, task
from pendulum import datetime,duration
from airflow.sdk.definitions.context import get_current_context
from ct_gov.include.etl.extract import Extractor


@dag(
    dag_id="process_ct_gov",
    start_date=datetime(2025, 10, 27),
    catchup=False,
    schedule=None,
    tags=["ctgov"]
)

def process_ct_gov():
    @task
    def extract():
        context = get_current_context()

        e = Extractor(context=context)
        return e.make_requests()


    extract_task = extract()


process_ct_gov()