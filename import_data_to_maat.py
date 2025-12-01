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

    # Task to find or handle missing resource
    @task
    def find_or_handle_resource(**context):
        """
        Try to find a resource, handle 404 errors gracefully.
        """
        from operators.maat_api_operator import MaatResourceOperator
        from airflow.exceptions import AirflowException

        # Create an instance of the operator
        operator = MaatResourceOperator(
            task_id='find_resource_op',
            operation='retrieve',
            resource_id='srlinux-leaf1',
        )

        try:
            # Execute the operator
            result = operator.execute(context)
            print("I GOT RESULT:", result)
            return result
        except AirflowException as e:
            # Check if it's a 404 error
            error_msg = str(e)
            if '404' in error_msg:
                print("ITS 404 ERROR - RESOURCE NOT FOUND")
                return {'status': 'not_found', 'resource_id': 'srlinux-leaf1'}
            else:
                # Re-raise if it's not a 404
                raise e

    find_resource = find_or_handle_resource()


    # Define task dependencies
    list_resources >> find_resource

# Instantiate the DAG
dag_instance = import_data_to_maat()
