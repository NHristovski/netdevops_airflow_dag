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

    retrive_resources = MaatResourceOperator(
        task_id='retrieve_resource',
        operation='retrieve',
        resource_id='srlinux-leaf1'
    )

    # # Task to find or handle missing resource
    # @task
    # def retrieve_resource(**context):
    #     """
    #     Try to find a resource, handle 404 errors gracefully.
    #     """
    #     from operators.maat_api_operator import MaatResourceOperator
    #
    #     # Create an instance of the operator
    #     operator = MaatResourceOperator(
    #         task_id='find_resource_op',
    #         operation='retrieve',
    #         resource_id='srlinux-leaf1',
    #     )
    #
    #     # Execute the operator
    #     result = operator.execute(context)
    #
    #     # Pull the HTTP status code from XCom (pushed by the operator in the same task context)
    #     ti = context['ti']
    #     http_status_code = ti.xcom_pull(key='http_status_code')
    #
    #     print(f"HTTP Status Code: {http_status_code}")
    #     print(f"Result: {result}")
    #
    #     # Check if it's a 404 error (resource not found)
    #     if http_status_code == 404:
    #         return {'status': 'not_found', 'resource_id': 'srlinux-leaf1', 'http_status_code': http_status_code}
    #
    #     # Return the result with status code
    #     if isinstance(result, dict):
    #         result['http_status_code'] = http_status_code
    #
    #     return result
    #
    # find_resource = retrieve_resource()

    # Branching task to decide whether to create resource or skip
    @task.branch
    def check_resource_status(**context):
        """
        Check if resource was found or not (404).
        If 404, proceed to create_resource.
        Otherwise, skip to end_task.
        """
        ti = context['ti']
        result = ti.xcom_pull(task_ids='retrieve_resource')

        print('The result is: ', result)

         # Check if the result indicates a 404 error
        if result and result.get('status') == 'not_found':
            print("Resource not found (404), will create it")
            return 'create_resource'
        else:
            print("Resource found, skipping creation")
            return 'skip_creation'

    check_status = check_resource_status()

    # Task to create the resource (only runs if 404)
    create_resource = MaatResourceOperator(
        task_id='create_resource',
        operation='create',
        data={
            "category": "device.router",
            "description": "Nokia SRLinux Router - leaf1",
            "id": "srlinux-leaf1",
            "name": "srlinux-leaf1",
            "serialNumber": "SRL-7220-IXR-D2L-2024-001",
            "operationalState": "enabled",
            "resourceCharacteristic": [
                {
                    "name": "vendor",
                    "value": "Nokia"
                },
                {
                    "name": "model",
                    "value": "7220 IXR-D2L"
                },
                {
                    "name": "os-version",
                    "value": "24.10.1"
                },
                {
                    "name": "management-ip",
                    "value": "172.80.80.11"
                },
                {
                    "name": "site",
                    "value": "Site-A"
                },
                {
                    "name": "tunnel-interface",
                    "value": "ethernet-1/1"
                },
                {
                    "name": "tunnel-ip",
                    "value": "10.0.1.1/30"
                }
            ],
            "resourceRelationship": [
                {
                    "relationshipType": "connects_to",
                    "resource": {
                        "id": "srlinux-leaf2",
                        "name": "srlinux-leaf2",
                        "@referredType": "PhysicalResource"
                    }
                }
            ],
            "@type": "PhysicalResource",
            "@schemaLocation": "https://bitbucket.software.geant.org/projects/OSSBSS/repos/maat-schema/raw/TMF639-ResourceInventory-v4-pionier.json"
        }

    )

    # Empty task for the skip branch
    @task
    def skip_creation():
        """
        Placeholder task when resource already exists.
        """
        print("Resource already exists, skipping creation")
        return {'status': 'skipped'}

    skip_task = skip_creation()

    # Define task dependencies
    list_resources >> find_resource >> check_status
    check_status >> create_resource
    check_status >> skip_task

# Instantiate the DAG
dag_instance = import_data_to_maat()
