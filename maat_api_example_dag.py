"""
Example DAG demonstrating the use of MaatAPIOperator.

This DAG shows various operations with the Maat Management API:
- List services
- Create a service
- Retrieve a service
- Update a service
- Delete a service
- Manage resources and listeners
"""

from airflow.sdk import dag, task
from datetime import datetime
from operators.maat_api_operator import (
    MaatAPIOperator,
    MaatServiceOperator,
    MaatResourceOperator
)


@dag(
    dag_id='maat_api_example',
    start_date=datetime(2025, 11, 3),
    schedule=None,
    catchup=False,
    doc_md="""
    # Maat API Integration Example

    This DAG demonstrates how to use the custom MaatAPIOperator to interact with the Maat Management API.

    ## Operations included:
    - List all services
    - Create a new service
    - Retrieve service by ID
    - Update service
    - Delete service
    - Manage resources and listeners

    Version: 1.0
    """,
    tags=['maat', 'api', 'example', 'v1.0'],
)
def maat_api_example_dag():
    """
    Example DAG for Maat API operations.
    """

    # Example 1: List all services using the base operator
    list_services = MaatAPIOperator(
        task_id='list_all_services',
        endpoint='/serviceInventoryManagement/v4.0.0/service',
        method='GET',
        query_params={
            'offset': 0,
            'limit': 10,
        }
    )

    # Example 2: Create a new service using specialized operator
    create_service = MaatServiceOperator(
        task_id='create_service',
        operation='create',
        service_data={
            'category': 'network.core.sap',
            'description': 'Service created by Airflow',
            'name': 'airflow_example_service',
            'serviceCharacteristic': [
                {
                    'name': 'inet',
                    'value': '192.168.100.1/28'
                }
            ],
            '@schemaLocation': 'https://bitbucket.software.geant.org/projects/OSSBSS/repos/maat-schema/raw/TMF638-ServiceInventory-v4-pionier.json',
            '@type': 'Service'
        }
    )

    # Example 3: Retrieve a specific service
    # Note: In a real scenario, you'd use XCom to get the ID from create_service
    retrieve_service = MaatServiceOperator(
        task_id='retrieve_service',
        operation='retrieve',
        service_id='{{ ti.xcom_pull(task_ids="create_service")["id"] }}',  # Get ID from previous task
        query_params={}
    )

    # Example 4: Update a service
    update_service = MaatServiceOperator(
        task_id='update_service',
        operation='update',
        service_id='{{ ti.xcom_pull(task_ids="create_service")["id"] }}',
        service_data={
            'description': 'Service updated by Airflow'
        }
    )

    # Example 5: List all resources
    list_resources = MaatResourceOperator(
        task_id='list_all_resources',
        operation='list',
        query_params={
            'offset': 0,
            'limit': 10,
        }
    )

    # Example 6: Create a resource
    create_resource = MaatResourceOperator(
        task_id='create_resource',
        operation='create',
        resource_data={
            'category': 'device.switch',
            'description': 'Resource created by Airflow',
            'name': 'airflow-test-switch',
            'resourceCharacteristic': [
                {
                    'name': 'interface',
                    'value': 'ae1'
                }
            ],
            '@schemaLocation': 'https://bitbucket.software.geant.org/projects/OSSBSS/repos/maat-schema/raw/TMF639-ResourceInventory-v4-pionier.json',
            '@type': 'PhysicalResource',
            'serialNumber': 'AFL-12345'
        }
    )

    # Example 9: Custom endpoint using base operator
    custom_api_call = MaatAPIOperator(
        task_id='custom_api_call',
        endpoint='/serviceInventoryManagement/v4.0.0/service',
        method='GET',
        query_params={
            'limit': 5,
        },
        headers={
            'X-Custom-Header': 'custom-value'
        }
    )

    # Example 10: Delete service (cleanup)
    delete_service = MaatServiceOperator(
        task_id='delete_service',
        operation='delete',
        service_id='{{ ti.xcom_pull(task_ids="create_service")["id"] }}'
    )

    # Define task dependencies
    # First, list existing services
    list_services >> create_service

    # After creating, retrieve and update
    create_service >> retrieve_service >> update_service

    # In parallel, work with resources
    list_services >> list_resources >> create_resource

    # Custom call can run independently
    list_services >> custom_api_call

    # Finally, cleanup by deleting the service
    [update_service, custom_api_call] >> delete_service


# Instantiate the DAG
dag_instance = maat_api_example_dag()

