from airflow.sdk import dag, task
from datetime import datetime
from operators.maat_api_operator import (
    MaatResourceOperator
)
import logging

# Suppress the secrets masker warning for short values
logging.getLogger('airflow.sdk._shared.secrets_masker.secrets_masker').setLevel(logging.ERROR)

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

    retrieve_resource = MaatResourceOperator(
        task_id='retrieve_resource',
        operation='get_by_name',
        resource_name='srlinux-leaf1'
    )

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

        print('Retrieved resource result:', result)

        http_status_code = result.get('http_status_code') if result else None

        print(f'HTTP Status Code: {http_status_code}')

        # Check if it's a 404 error
        if http_status_code == 404:
            print("Resource not found (404), will create it")
            return 'create_resource'
        else:
            print("Resource found, skipping creation")
            return 'skip_first_creation'

    check_status = check_resource_status()

    # Task to create the resource (only runs if 404)
    create_resource = MaatResourceOperator(
        task_id='create_resource',
        operation='create',
        resource_data={
            "category": "device.router",
            "description": "Nokia SRLinux Router - leaf1",
            "name": "srlinux-leaf1",
            "serialNumber": "SRL-7220-IXR-D2L-2024-001",
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
                }
            ],
            "@type": "PhysicalResource",
            "@schemaLocation": "https://bitbucket.software.geant.org/projects/OSSBSS/repos/maat-schema/raw/TMF639-ResourceInventory-v4-pionier.json"
        }

    )

    retrieve_second_resource = MaatResourceOperator(
        task_id='retrieve_second_resource',
        operation='retrieve',
        resource_id='srlinux-leaf2'
    )

    @task.branch
    def check_second_resource_status(**context):
        """
        Check if the second resource was found or not (404).
        If 404, proceed to create_second_resource.
        Otherwise, skip to end_task.
        """
        ti = context['ti']
        result = ti.xcom_pull(task_ids='retrieve_second_resource')

        print(f'Full response is: {result.get('response')}')

        print(f'response size is: {len(result.get('response'))}')

        http_status_code = result.get('http_status_code') if result else None

        print(f'HTTP Status Code: {http_status_code}')

        # Check if it's a 404 error
        if http_status_code == 404:
            print("Resource not found (404), will create it")
            return 'create_second_resource'
        else:
            print("Resource found, skipping creation")
            return 'skip_second_creation'

    second_check_status = check_second_resource_status()

    create_second_resource = MaatResourceOperator(
        task_id='create_second_resource',
        operation='create',
        resource_data={
            "category": "device.router",
            "description": "Nokia SRLinux Router - leaf2",
            "name": "srlinux-leaf2",
            "serialNumber": "SRL-7220-IXR-D2L-2024-002",
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
                    "value": "172.80.80.12"
                },
                {
                    "name": "site",
                    "value": "Site-B"
                }
            ],
            "@type": "PhysicalResource",
            "@schemaLocation": "https://bitbucket.software.geant.org/projects/OSSBSS/repos/maat-schema/raw/TMF639-ResourceInventory-v4-pionier.json"
        }
    )

    # Empty task for the first skip branch
    @task
    def skip_first_creation():
        """
        Placeholder task when first resource already exists.
        """
        print("First resource already exists, skipping creation")
        return {'status': 'skipped'}

    skip_first_task = skip_first_creation()

    # Empty task for the second skip branch
    @task
    def skip_second_creation():
        """
        Placeholder task when second resource already exists.
        """
        print("Second resource already exists, skipping creation")
        return {'status': 'skipped'}

    skip_second_task = skip_second_creation()

    # Define task dependencies
    list_resources >> retrieve_resource >> check_status
    check_status >> create_resource >> retrieve_second_resource
    check_status >> skip_first_task >> retrieve_second_resource

    retrieve_second_resource >> second_check_status
    second_check_status >> create_second_resource
    second_check_status >> skip_second_task

# Instantiate the DAG
dag_instance = import_data_to_maat()
