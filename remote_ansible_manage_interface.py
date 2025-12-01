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
        Check if resource was found and verify interface exists with correct state.
        """
        ti = context['ti']
        result = ti.xcom_pull(task_ids='retrieve_router_info')

        print('Result: ', result)

        # Check if result exists and has a 'response' key
        if result and 'response' in result:
            response_list = result.get('response')
            if response_list is not None and len(response_list) == 1:
                router = response_list[0]
                print("Router found: ", router.get('name'))

                # Get parameters
                interface_name = context['params']['interface']
                desired_state = context['params']['state']

                # Get resource characteristics
                resource_characteristics = router.get('resourceCharacteristic', [])

                # Create the characteristic name for this interface
                interface_char_name = f"interface-{interface_name}"

                # Check if interface exists in the router characteristics
                interface_found = False
                current_state = None

                for char in resource_characteristics:
                    if char.get('name') == interface_char_name:
                        interface_found = True
                        current_state = char.get('value')
                        break

                if not interface_found:
                    print(f"ERROR: Interface {interface_name} does not exist on router {router.get('name')}")
                    # Store error info in XCom for show_error task
                    ti.xcom_push(key='error_type', value='interface_not_found')
                    ti.xcom_push(key='interface_name', value=interface_name)
                    return 'show_error'

                # Interface exists, check if state change is needed
                print(f"Interface {interface_name} current state: {current_state}")
                print(f"Desired state: {desired_state}")

                if current_state == desired_state:
                    print(f"Interface {interface_name} is already {desired_state}")
                    return 'interface_already_configured'

                print(f"State change needed: {current_state} -> {desired_state}")
                return 'run_remote_command'

        print("Resource not found! Will not proceed to run command.")
        ti.xcom_push(key='error_type', value='router_not_found')
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
        Display error message based on error type.
        """
        ti = context['ti']
        error_type = ti.xcom_pull(key='error_type', task_ids='check_router_status')

        if error_type == 'interface_not_found':
            interface_name = ti.xcom_pull(key='interface_name', task_ids='check_router_status')
            router_name = context['params']['router']
            print(f"ERROR: Interface '{interface_name}' does not exist on router '{router_name}'!")
            print(f"Available interfaces must be configured in Maat first.")
            raise Exception(f"Interface '{interface_name}' does not exist on router '{router_name}'")
        else:  # router_not_found
            router_name = context['params']['router']
            print(f"ERROR: Router '{router_name}' not found in Maat!")
            print(f"Please ensure the router exists in Maat before trying to configure it.")
            raise Exception(f"Router '{router_name}' not found in Maat inventory")

    error_task = show_error()

    @task
    def interface_already_configured(**context):
        """
        Display message when interface is already in the desired state.
        """
        interface_name = context['params']['interface']
        state = context['params']['state']
        router_name = context['params']['router']

        print(f"Interface '{interface_name}' on router '{router_name}' is already {state}")
        print(f"No configuration change needed.")

        return {
            'router': router_name,
            'interface': interface_name,
            'state': state,
            'changed': False,
            'message': f"Interface already {state}"
        }

    already_configured_task = interface_already_configured()

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
                print(f"Successfully updated router in Maat")
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

    update_maat_task = update_router_interface()

    @task(trigger_rule='one_failed')
    def run_remote_command_rollback(**context):
        """
        Rollback task - revert interface to original state if Maat update fails.
        """
        from airflow.providers.ssh.hooks.ssh import SSHHook

        ti = context['ti']

        # Get the original state (opposite of what was requested)
        requested_state = context['params']['state']
        rollback_state = 'down' if requested_state == 'up' else 'up'

        interface_name = context['params']['interface']
        router_name = context['params']['router']

        print(f"️ROLLBACK: Maat update failed!")
        print(f"Reverting interface {interface_name} on {router_name} from {requested_state} back to {rollback_state}")

        rollback_command = (
            f'ansible-playbook /home/ubuntu/ansible/playbooks/nokia_change_interface_state.yaml '
            f'-i /home/ubuntu/ansible/inventory/hosts.ini '
            f'-e "router={router_name} interface={interface_name} state={rollback_state}"'
        )

        print(f"Executing rollback command: {rollback_command}")

        # Execute the rollback via SSH
        ssh_hook = SSHHook(ssh_conn_id='ansible-ssh')

        try:
            with ssh_hook.get_conn() as ssh_client:
                stdin, stdout, stderr = ssh_client.exec_command(rollback_command)
                exit_status = stdout.channel.recv_exit_status()

                output = stdout.read().decode('utf-8')
                error_output = stderr.read().decode('utf-8')


                if exit_status == 0:
                    print(f"Successfully rolled back interface {interface_name} to {rollback_state}")
                else:
                    print(f"Rollback command failed with exit status {exit_status}")
                    print(f"Error output:\n{error_output}")
                    raise Exception(f"Rollback failed with exit status {exit_status}")

                return {
                    'rolled_back': True,
                    'interface': interface_name,
                    'router': router_name,
                    'original_state': rollback_state,
                    'failed_state': requested_state
                }
        except Exception as e:
            print(f"Rollback execution failed: {str(e)}")
            raise Exception(f"Critical: Both Maat update and rollback failed! Manual intervention required. Error: {str(e)}")

    rollback_task = run_remote_command_rollback()

    # Define task dependencies
    retrieve_router_info >> check_router
    check_router >> run_remote_command >> update_maat_task
    check_router >> error_task
    check_router >> already_configured_task

    # Add rollback trigger: if update_maat_task fails, execute rollback
    update_maat_task >> rollback_task


# Instantiate the DAG
dag_instance = ssh_remote_ansible_dag()
