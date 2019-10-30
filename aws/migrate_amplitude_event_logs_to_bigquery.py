import json
import logging
import sys
import time

from datetime import datetime, timedelta
from google.cloud import bigquery


def main():
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    logging.info('process start!!!!')

    try:
        client = bigquery.Client()
        dataset_id = 'event_log_dataset'
        table_id = 'event_log'
        filename = '/Users/ray.chou/Documents/Amplitude/138162/log_json/138162_2019-10-21_6#90.json'

        dataset_ref = client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)
        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        job_config.skip_leading_rows = 1
        job_config.autodetect = True

        with open(filename, "rb") as stream:
            logging.info('stream: %s', stream)
            line = stream.readline()
            while line:
                eventLog = json.loads(line)
                job = client.load_table_from_json(
                    eventLog,
                    table_ref,
                    job_config=job_config)

                job.result()  # Waits for table load to complete.
                print("Loaded {} rows into {}:{}.".format(
                    job.output_rows, dataset_id, table_id))

                line = stream.readline()

            stream.close()

        print('processing')
    finally:
        logging.info('process done!!!!')


if __name__ == "__main__":
    main()
