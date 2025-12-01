from airflow.sdk import dag, task, Param
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime
from operators.maat_api_operator import (
    MaatResourceOperator,
    OperationType
)


@dag(
    dag_id='ssh_ansible_configure_interface',
    start_date=datetime(2025, 11, 3),
    schedule=None,
    catchup=False,
    doc_md="Version: 1.0",
    tags=['ssh', 'ansible'],
    params={
        'router': Param(
            default='srlinux-leaf1',
            type='string',
            enum=['srlinux-leaf1', 'srlinux-leaf2'],
            description='Router to configure'
        ),
        'interface': Param(
            default='ethernet-1/1',
            type='string',
            enum=['ethernet-1/1', 'ethernet-1/49'],
            description='Interface to configure'
        ),
        'state': Param(
            default='up',
            type='string',
            enum=['up', 'down'],
            description='Interface state: up (enable) or down (disable)'
        )
    }
)
def ssh_remote_ansible_dag():
    """
    DAG that executes ansible on remote VM and prints the output.
    """
    retrieve_router_info = MaatResourceOperator(
        task_id='retrieve_router_info',
        operation=OperationType.GET_BY_NAME,
        resource_name='{{ params.router }}'
    )

    # Branching task to decide whether to create resource or skip
    @task.branch
    def check_router_status(**context):
        """
        Check if resource was found or not.
        If response list is empty, proceed to create_first_router.
        Otherwise, skip to skip_first_router_creation.
        """
        ti = context['ti']
        result = ti.xcom_pull(task_ids='retrieve_router_info')

        print('Result: ', result)

        # Check if result exists and has a 'response' key
        if result and 'response' in result:
            response_list = result.get('response')
            if response_list is not None:
                if len(response_list) == 1:
                    print("Resource found: ", response_list)
                    return 'run_remote_command'

        print("Resource not found! Will not proceed to run command.")
        return 'show_error'

    check_router = check_router_status()

    # SSHOperator doesn't have a TaskFlow decorator, so we use it directly
    run_remote_command = SSHOperator(
        task_id='run_remote_command',
        ssh_conn_id='ansible-ssh',
        command='ansible-playbook /home/ubuntu/ansible/playbooks/nokia_change_interface_state.yaml -i /home/ubuntu/ansible/inventory/hosts.ini -e "router={{ params.router }} interface={{ params.interface }} state={{ params.state }}"',
    )

    @task
    def show_error(**context):
        """
        Display error message when router is not found in Maat.
        """
        router_name = context['params']['router']
        print(f"ERROR: Router '{router_name}' not found in Maat!")
        raise Exception(f"Router '{router_name}' not found in Maat inventory")

    error_task = show_error()

    @task
    def update_router_interface(**context):
        """
        Update the router in Maat with the new interface state after successful Ansible execution.
        """
        ti = context['ti']
        # Get the router information from retrieve_router_info task
        result = ti.xcom_pull(task_ids='retrieve_router_info')

        # Extract router ID from the response
        if result and 'response' in result:
            response_list = result.get('response')
            if response_list and len(response_list) == 1:
                router = response_list[0]
                router_id = router.get('id')
                router_name = router.get('name')

                print(f"Updating router '{router_name}' (ID: {router_id}) in Maat...")

                # Get current parameters
                interface_name = context['params']['interface']
                interface_state = context['params']['state']

                print(f"Setting interface {interface_name} to {interface_state}")

                # Get existing resource characteristics
                resource_characteristics = router.get('resourceCharacteristic', [])

                # Create the characteristic name for this interface
                interface_char_name = f"interface-{interface_name}"

                # Update or add the interface characteristic
                interface_found = False
                for char in resource_characteristics:
                    if char.get('name') == interface_char_name:
                        # Update existing interface state
                        char['value'] = interface_state
                        interface_found = True
                        print(f"Updated existing interface characteristic: {interface_char_name} = {interface_state}")
                        break

                if not interface_found:
                    # Add new interface characteristic
                    resource_characteristics.append({
                        "name": interface_char_name,
                        "value": interface_state
                    })
                    print(f"Added new interface characteristic: {interface_char_name} = {interface_state}")

                # Now update the router in Maat using the operator
                from operators.maat_api_operator import MaatResourceOperator

                update_operator = MaatResourceOperator(
                    task_id='update_router_in_maat',
                    operation=OperationType.UPDATE,
                    resource_id=router_id,
                    resource_data={
                        "resourceCharacteristic": resource_characteristics
                    }
                )

                # Execute the update
                update_result = update_operator.execute(context)
                print(f"✅ Successfully updated router in Maat")
                print(f"Updated characteristic: {interface_char_name} = {interface_state}")

                return {
                    'router_id': router_id,
                    'router_name': router_name,
                    'interface': interface_name,
                    'new_state': interface_state,
                    'updated': True
                }
            else:
                print("Error: Could not extract router information from response")
                return {'updated': False, 'error': 'Invalid response format'}
        else:
            print("Error: No router information available")
            return {'updated': False, 'error': 'No router data'}

    update_task = update_router_interface()

    # Define task dependencies
    retrieve_router_info >> check_router
    check_router >> run_remote_command >> update_task
    check_router >> error_task


# Instantiate the DAG
dag_instance = ssh_remote_ansible_dag()
