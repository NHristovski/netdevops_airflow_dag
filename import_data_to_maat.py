from airflow.sdk import dag, task
from datetime import datetime
from operators.maat_api_operator import (
    MaatResourceOperator
)


@dag(
    dag_id='import_data_to_maat',
    start_date=datetime(2025, 12, 1),
    schedule=None,
    catchup=False,
    doc_md="""
    # Maat Setup

    This DAG sets up initial data in Maat Management API.

    Version: 1.0
    """,
    tags=['maat', 'setup']
)
def import_data_to_maat():

    list_resources = MaatResourceOperator(
        task_id='list_all_resources',
        operation='list',
        query_params={
            'offset': 0,
            'limit': 10,
        }
    )

    find_resource = MaatResourceOperator(
        task_id='list_all_resources',
        operation='retrieve',
        resource_id='srlinux-leaf1',
    )
