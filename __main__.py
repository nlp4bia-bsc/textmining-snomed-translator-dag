import airflow
import datetime
import json
from airflow import DAG
from airflow.operators.python import PythonOperator

from snomed_automatic_translation.scripts.spreadsheet_translator import translate_snomedid

default_args = {
    "spreadsheet_id": "1GM17jnZop0eHSYaWKccVhp4pdbX58IEuSLXsiYg4GUQ",
    "sheet_id": "0",
    "column": "SNOMEDID NUMBER"
}


dag = DAG(
    dag_id="snomed_automatic_translate",
    start_date=datetime.datetime(2024, 5, 10),
    schedule_interval=None,
    default_args=default_args,
    description='Automatic translate of SNOMED ids',
    user_defined_macros={
        'default_params': json.dumps(default_args)
    }
)

translate_snomedid_task = PythonOperator(
    task_id='translate_snomedid',
    provide_context=True,
    python_callable=translate_snomedid,
    dag=dag
)

translate_snomedid_task
