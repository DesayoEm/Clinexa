from datetime import datetime, date
from airflow.utils.log.logging_mixin import LoggingMixin
import requests
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from typing import Dict

from ct_gov.include.config import config
from ct_gov.include.services.middleware import persist_state_before_failure, persist_state_before_exit
from ct_gov.include.services.rate_limit_handler import RateLimiterHandler
from ct_gov.include.tests import FailureGenerator
from ct_gov.include.utilities.exceptions import RequestTimeoutError

log = LoggingMixin().log


class Extractor:
    def __init__(self, context: Dict, timeout: int = 30, max_retries: int = 3):

        self.rate_limit_handler = RateLimiterHandler()
        self.context = context
        self.last_saved_page = self.determine_starting_point(context)['last_saved_page']
        self.current_page = self.last_saved_page if self.last_saved_page else 0
        # technically current page should be  last saved + 1 but the val is incremented
        # by one in the make_requests func so no need to do it here

        self.last_saved_token = self.determine_starting_point(context)['last_saved_token']
        self.next_page_url = self.determine_starting_point(context)['next_page_url']

        self.timeout = timeout
        self.max_retries = max_retries
        self.failure_generator = FailureGenerator(True, 0.5)


        log.info(
            f"Initializing Extractor... \n"
            f"\n Last saved is page {self.last_saved_page}"
            f"\n URL to extract is {self.next_page_url}"

            )



    @staticmethod
    def determine_starting_point(context: Dict):
        log.info(f"Determining starting point for extractor...")

        ti = context['task_instance']
        if not ti:
            log.warning("No task instance found in context, starting fresh")
            return {
                'last_saved_page': 0,
                'last_saved_token': None,
                'next_page_url': config.FIRST_PAGE_URL
            }

        states = ti.xcom_pull(
            task_ids=ti.task_id,
            key="previous_states",
            include_prior_dates=True,

        )

        if states and isinstance(states, list):
            state = states[0]
            log.info(f"Existing context found from last run: {state} \n \n")

            return {
                'last_saved_page': state['pages_loaded'],
                'last_saved_token': state['last_saved_token'],
                'next_page_url': f"{config.BASE_URL}{state['last_saved_token']}"
            }

        else:
            log.info(f"No existing context found from previous run... \n \n")
            return {
                'last_saved_page': 0,
                'last_saved_token': None,
                'next_page_url': config.FIRST_PAGE_URL
            }




    def make_requests(self):
        while self.current_page < 20:
            try:
                url = self.next_page_url

                self.current_page += 1
                log.info(f"Starting from page {self.current_page}")

                self.rate_limit_handler.wait_if_needed()

                for attempt in range(self.max_retries):
                    response = requests.get(url, timeout=self.timeout)
                    attempt += 1
                    if response.status_code == 200:
                        data = response.json()
                        next_page_token = data.get("nextPageToken")
                        break
                    elif attempt >= self.max_retries and response.status_code != 200:
                        log.error(
                            f"Request exception FAILED AFTER 3 attempts on page {self.current_page}"
                        )

                        persist_state_before_failure(
                            error=RequestTimeoutError,
                            context=self.context,
                            metadata={
                                'pages_loaded': self.last_saved_page,
                                'last_saved_token': self.last_saved_token,
                                'next_page_url': self.next_page_url,
                            }

                        )

                if not next_page_token:
                    log.info(
                    f"Next page not found on page {self.current_page}"
                    f"Check metadata for the token for this page")

                    metadata = {
                        'pages_loaded': self.last_saved_page,
                        'last_saved_token': self.last_saved_token,
                        'next_page_url': self.next_page_url,
                    }
                    persist_state_before_exit(self.context, metadata)


                if self.current_page == 10:
                    self.failure_generator.maybe_fail_extraction(self.current_page)


                self.last_saved_token = next_page_token
                self.next_page_url = f"{config.BASE_URL}{next_page_token}"
                self.last_saved_page += 1

                log.info(
                    f'Successfully made request to {self.next_page_url} \n Last loaded page is page {self.current_page}'
                     f'\n Next page is {self.next_page_url}'
                    )

                # self.save_response(data)

            except Exception as e:
                persist_state_before_failure(
                    error = e,
                    context=self.context,
                    metadata={
                        'last_saved_page': self.last_saved_page,
                        'last_saved_token': self.last_saved_token,
                        'next_page_url': self.next_page_url,
                        },
                    )
        metadata = {
            'pages_loaded': self.last_saved_page,
            'last_saved_token': self.last_saved_token,
            'next_page_url': self.next_page_url,
        }
        persist_state_before_exit(self.context, metadata)



    def save_response(self, data: Dict):
        df = pd.DataFrame(data)
        table = pa.Table.from_pandas(df)

        file_date = datetime.today().strftime("%Y-%m-%d")

        output_dir = f"{config.SHARD_STORAGE_DIR}/{file_date}"
        os.makedirs(output_dir, exist_ok=True)

        with open("etl/states/last_shard_path.py", "w") as f:
            f.write(
                f'shard_path = "{output_dir}"\n'
            )
        page_number = self.current_page
        file_to_write = f"{output_dir}/{page_number}.parquet"

        pq.write_table(table, file_to_write)

        self.last_saved_page += 1


        log.info(
            f"Successfully saved page {self.current_page} at {file_to_write}"
        )



    @staticmethod
    def compact_shards(path_to_read: str, path_to_write: str):
        try:
            files = os.listdir(path_to_read)

            file_name = f"studies - {date.today().strftime("%Y-%m-%d")}"
            file_to_write = f"{path_to_write}/{file_name}.parquet"

            parquet_shards = [f for f in files if f.endswith(".parquet")]
            num_of_files = len(parquet_shards)

            if num_of_files == 0:
                log.info("No parquet files to compact.")
                return

            writer = None
            try:
                for file in parquet_shards:
                    table = pq.read_table(f"{path_to_read}/{file}")

                    if writer is None:
                        writer = pq.ParquetWriter(file_to_write, table.schema)

                    if table.schema != writer.schema:
                        table = table.cast(writer.schema)

                    writer.write_table(table)

            except Exception as e:
                if writer and writer.is_open:
                    writer.close()
                log.error(f"Compaction failed. Shards preserved at: {path_to_read}\n Error: {str(e)}")

            finally:
                if writer and writer.is_open:
                    writer.close()

            log.info(
                f"{num_of_files} pages compacted at {file_to_write}"
            )
        except Exception as e:
            raise



