from airflow.sdk import dag, task
from datetime import datetime
from operators.maat_api_operator import (
    MaatResourceOperator,
    OperationType
)
import logging

# Suppress the secrets masker warning for short values
logging.getLogger('airflow.sdk._shared.secrets_masker.secrets_masker').setLevel(logging.ERROR)

@dag(
    dag_id='initial_maat_setup',
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
def initial_maat_setup():

    retrieve_first_router = MaatResourceOperator(
        task_id='retrieve_first_router',
        operation=OperationType.GET_BY_NAME,
        resource_name='srlinux-leaf1'
    )

    # Branching task to decide whether to create resource or skip
    @task.branch
    def check_first_router_status(**context):
        """
        Check if resource was found or not.
        If response list is empty, proceed to create_first_router.
        Otherwise, skip to skip_first_router_creation.
        """
        ti = context['ti']
        result = ti.xcom_pull(task_ids='retrieve_first_router')

        print('Result: ', result)

        # Check if result exists and has a 'response' key
        if result and 'response' in result:
            response_list = result.get('response')
            if response_list is not None:
                if len(response_list) == 1:
                    print("Resource found, skipping creation")
                    return 'skip_first_router_creation'

        print("Resource not found (response list is empty or missing), will create it")
        return 'create_first_router'

    check_first_router = check_first_router_status()

    # Task to create the resource (only runs if 404)
    create_first_router = MaatResourceOperator(
        task_id='create_first_router',
        operation=OperationType.CREATE,
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
                },
                {
                    "name": "interface-ethernet-1/1",
                    "value": "up"
                },
                {
                    "name": "interface-ethernet-1/49",
                    "value": "up"
                }
            ],
            "@type": "PhysicalResource",
            "@schemaLocation": "https://bitbucket.software.geant.org/projects/OSSBSS/repos/maat-schema/raw/TMF639-ResourceInventory-v4-pionier.json"
        }

    )

    retrieve_second_router = MaatResourceOperator(
        task_id='retrieve_second_router',
        operation=OperationType.GET_BY_NAME,
        resource_name='srlinux-leaf2'
    )

    @task.branch
    def check_second_router_status(**context):
        """
        Check if the second resource was found or not.
        If response list is empty, proceed to create_second_router.
        Otherwise, skip to skip_second_router_creation.
        """
        ti = context['ti']
        result = ti.xcom_pull(task_ids='retrieve_second_router')

        print('Result: ', result)

        if result and 'response' in result:
            response_list = result.get('response')
            if response_list is not None:
                if len(response_list) == 1:
                    print("Resource found (response list has 1 item), skipping creation")
                    return 'skip_second_router_creation'

        print("Resource not found (response list is empty or missing), will create it")
        return 'create_second_router'

    check_second_router = check_second_router_status()

    create_second_router = MaatResourceOperator(
        task_id='create_second_router',
        operation=OperationType.CREATE,
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
                },
                {
                    "name": "interface-ethernet-1/1",
                    "value": "up"
                },
                {
                    "name": "interface-ethernet-1/49",
                    "value": "up"
                }
            ],
            "@type": "PhysicalResource",
            "@schemaLocation": "https://bitbucket.software.geant.org/projects/OSSBSS/repos/maat-schema/raw/TMF639-ResourceInventory-v4-pionier.json"
        }
    )

    # Empty task for the first skip branch
    @task
    def skip_first_router_creation():
        """
        Placeholder task when first router already exists.
        """
        print("First router already exists, skipping creation")
        return {'status': 'skipped'}

    skip_first_router = skip_first_router_creation()

    # Empty task for the second skip branch
    @task
    def skip_second_router_creation():
        """
        Placeholder task when second router already exists.
        """
        print("Second router already exists, skipping creation")
        return {'status': 'skipped'}

    skip_second_router = skip_second_router_creation()

    # Define task dependencies
    # First router branch
    retrieve_first_router >> check_first_router
    check_first_router >> create_first_router
    check_first_router >> skip_first_router

    # Second router branch
    retrieve_second_router >> check_second_router
    check_second_router >> create_second_router
    check_second_router >> skip_second_router

# Instantiate the DAG
dag_instance = initial_maat_setup()
