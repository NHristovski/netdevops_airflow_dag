"""
Simple DAG to test Maat API connectivity.

This DAG performs basic operations to verify the operator is working correctly.
"""

from airflow.sdk import dag, Param
from datetime import datetime
from operators.maat_api_operator import (
    MaatAPIOperator,
    MaatServiceOperator,
    HTTPMethod,
    OperationType
)


@dag(
    dag_id='maat_api_test',
    start_date=datetime(2025, 11, 3),
    schedule=None,
    catchup=False,
    doc_md="Simple test DAG for Maat API Operator - Version: 1.0",
    tags=['maat', 'api', 'test', 'v1.0'],
    params={
        'endpoint': Param(
            default='/serviceInventoryManagement/v4.0.0/service',
            type='string',
            description='API endpoint to test'
        ),
        'method': Param(
            default=HTTPMethod.GET,
            type='string',
            enum=[m.value for m in HTTPMethod],
            description='HTTP method'
        )
    }
)
def maat_api_test_dag():
    """
    Simple test DAG for Maat API operations.
    """

    # Test 1: Simple GET request to list services
    test_get_services = MaatAPIOperator(
        task_id='test_get_services',
        endpoint='{{ params.endpoint }}',
        method='{{ params.method }}',
        query_params={
            'offset': 0,
            'limit': 5,
        }
    )

    # Test 2: Using specialized operator
    test_list_services = MaatServiceOperator(
        task_id='test_list_services',
        operation=OperationType.LIST,
        query_params={
            'offset': 0,
            'limit': 10,
        }
    )

    test_get_services >> test_list_services


# Instantiate the DAG
dag_instance = maat_api_test_dag()

