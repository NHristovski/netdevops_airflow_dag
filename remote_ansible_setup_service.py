from datetime import datetime

from airflow.exceptions import AirflowException
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.sdk import dag, task, Param

from operators.maat_api_operator import (
    MaatResourceOperator,
    MaatServiceOperator,
    OperationType
)


@dag(
    dag_id='ssh_ansible_setup_service',
    start_date=datetime(2025, 12, 15),
    schedule=None,
    catchup=False,
    doc_md="Version: 1.0",
    tags=['ssh', 'ansible', 'setup', 'maat'],
    params={
        'service_name': Param(
            default='MAC-VRF',
            type='string',
            enum=['MAC-VRF', 'SOME-OTHER-SERVICE'],
            description='Name of the service'
        ),
        'first_router': Param(
            default='srlinux-leaf1',
            type='string',
            enum=['srlinux-leaf1', 'srlinux-leaf2'],
            description='First router'
        ),
        'second_router': Param(
            default='srlinux-leaf1',
            type='string',
            enum=['srlinux-leaf1', 'srlinux-leaf2'],
            description='Second router'
        ),
    }
)
def remote_ansible_setup_service_dag():
    """
    DAG that executes ansible on remote VM to configure service between two routers and prints the output.
    """

    @task
    def validate_params(**context):
        """
        Validate that:
        1. Service with the same name doesn't already exist in Maat
        2. first_router and second_router are not equal
        3. Both routers exist in Maat
        """
        service_name = context['params']['service_name']
        first_router = context['params']['first_router']
        second_router = context['params']['second_router']

        print(f"Checking if service '{service_name}' already exists in Maat...")
        service_check_operator = MaatServiceOperator(
            task_id='check_service_exists',
            operation=OperationType.RETRIEVE_BY_NAME,
            service_name=service_name
        )
        service_result = service_check_operator.execute(context)

        # Check if service exists
        if service_result and 'response' in service_result:
            service_response_list = service_result.get('response')
            if service_response_list and len(service_response_list) > 0:
                raise AirflowException(
                    f"Service '{service_name}' already exists in Maat. Please configure a different service or delete the existing service first."
                )

        print(f"Service '{service_name}' does not exist in Maat - validation passed")

        if first_router == second_router:
            raise AirflowException(
                f"First router cannot be equal to second router. Both are set to '{first_router}'"
            )

        # Step 3: Check if both routers exist in Maat
        print(f"Checking if first router '{first_router}' exists in Maat...")
        first_router_operator = MaatResourceOperator(
            task_id='check_first_router_exists',
            operation=OperationType.RETRIEVE_BY_NAME,
            resource_name=first_router
        )
        first_router_result = first_router_operator.execute(context)

        if not first_router_result or 'response' not in first_router_result:
            raise AirflowException(
                f"First router '{first_router}' not found in Maat inventory"
            )

        first_response_list = first_router_result.get('response')
        if not first_response_list or len(first_response_list) == 0:
            raise AirflowException(
                f"First router '{first_router}' not found in Maat inventory"
            )

        print(f"First router '{first_router}' found in Maat")

        print(f"Checking if second router '{second_router}' exists in Maat...")
        second_router_operator = MaatResourceOperator(
            task_id='check_second_router_exists',
            operation=OperationType.RETRIEVE_BY_NAME,
            resource_name=second_router
        )
        second_router_result = second_router_operator.execute(context)

        if not second_router_result or 'response' not in second_router_result:
            raise AirflowException(
                f"Second router '{second_router}' not found in Maat inventory"
            )

        second_response_list = second_router_result.get('response')
        if not second_response_list or len(second_response_list) == 0:
            raise AirflowException(
                f"Second router '{second_router}' not found in Maat inventory"
            )

        print(f"Second router '{second_router}' found in Maat")
        print(f"Validations passed")

        # Extract router IDs
        first_router_id = first_response_list[0].get('id')
        second_router_id = second_response_list[0].get('id')

        print(f"First router ID: {first_router_id}")
        print(f"Second router ID: {second_router_id}")

        return {
            'first_router': first_router,
            'second_router': second_router,
            'first_router_id': first_router_id,
            'second_router_id': second_router_id,
            'first_router_data': first_response_list[0],
            'second_router_data': second_response_list[0]
        }

    validate_routers_task = validate_params()

    @task
    def run_remote_command(**context):
        """
        Execute Ansible playbook via SSH.
        """
        from airflow.providers.ssh.hooks.ssh import SSHHook

        first_router = context['params']['first_router']
        second_router = context['params']['second_router']

        command = (
            f'ansible-playbook /home/ubuntu/ansible/playbooks/nokia_setup_macvrf_tunnel.yaml '
            f'-i /home/ubuntu/ansible/inventory/hosts.ini '
            f'-e "router1_name={first_router} router2_name={second_router}"'
        )

        print(f"Executing Ansible command: {command}")

        ssh_hook = SSHHook(ssh_conn_id='ansible-ssh')

        try:
            with ssh_hook.get_conn() as ssh_client:
                stdin, stdout, stderr = ssh_client.exec_command(command)
                exit_status = stdout.channel.recv_exit_status()

                output = stdout.read().decode('utf-8')
                error_output = stderr.read().decode('utf-8')

                print(f"Command output:\n{output}")

                if exit_status == 0:
                    print(f"Ansible playbook executed successfully")
                    return {
                        'success': True,
                        'exit_status': exit_status,
                    }
                else:
                    print(f"Ansible playbook failed with exit status {exit_status}")
                    print(f"Error output:\n{error_output}")
                    return {
                        'success': False,
                        'exit_status': exit_status,
                        'error': error_output,
                    }
        except Exception as e:
            print(f"SSH execution failed: {str(e)}")
            return {
                'success': False,
                'error': str(e),
            }

    run_remote_command_task = run_remote_command()

    @task
    def create_service_in_maat(**context):
        """
        Create the service in Maat after successful Ansible execution.
        """
        ti = context['ti']

        # Get service name from params
        service_name = context['params']['service_name']

        # Get router IDs from validation task
        validation_result = ti.xcom_pull(task_ids='validate_params')
        first_router_id = validation_result.get('first_router_id')
        second_router_id = validation_result.get('second_router_id')
        first_router_name = validation_result.get('first_router')
        second_router_name = validation_result.get('second_router')

        print(f"Creating service '{service_name}' in Maat...")
        print(f"First router: {first_router_name} (ID: {first_router_id})")
        print(f"Second router: {second_router_name} (ID: {second_router_id})")

        # Create service data
        service_data = {
            "category": "network.tunnel",
            "description": "MAC-VRF Service",
            "name": service_name,
            "serviceCharacteristic": [
                {
                    "name": "router1_tunnel_ip",
                    "value": "10.10.10.1"
                },
                {
                    "name": "router2_tunnel_ip",
                    "value": "10.10.10.2"
                }
            ],
            "supportingResource": [
                {
                    "id": first_router_id,
                    "name": first_router_name,
                    "@referredType": "PhysicalResource"
                },
                {
                    "id": second_router_id,
                    "name": second_router_name,
                    "@referredType": "PhysicalResource"
                }
            ],
            "@type": "Service",
            "@schemaLocation": "https://bitbucket.software.geant.org/projects/OSSBSS/repos/maat-schema/raw/TMF638-ServiceInventory-v4-pionier.json"
        }

        # Create service using MaatServiceOperator
        service_operator = MaatServiceOperator(
            task_id='create_service_operator',
            operation=OperationType.CREATE,
            service_data=service_data
        )

        result = service_operator.execute(context)

        if result and 'response' in result:
            print(f"Service '{service_name}' created successfully in Maat")
            return {
                'created': True,
                'service_name': service_name,
                'service_data': result.get('response')
            }
        else:
            print(f"Failed to create service '{service_name}' in Maat")
            return {
                'created': False,
                'service_name': service_name
            }

    update_maat_task = create_service_in_maat()

    @task.branch(trigger_rule='all_done')
    def check_update_result(**context):
        """
        Check if Ansible command and Maat update were successful.
        If run_remote_command failed, go directly to end (no rollback needed).
        If update failed or result is missing, trigger rollback.
        Otherwise, end gracefully.
        This task runs regardless of whether update_maat_task succeeds or fails.
        """
        ti = context['ti']

        ansible_result = ti.xcom_pull(task_ids='run_remote_command')

        print(f"Ansible result: {ansible_result}")

        if not ansible_result or ansible_result.get('success') is False:
            print("Ansible command failed or result missing - going directly to end (no rollback needed)")
            return 'end_task'

        print("Ansible command successful - checking Maat update result")

        update_result = ti.xcom_pull(task_ids='create_service_in_maat')

        print(f"Update result: {update_result}")

        if not update_result:
            print("Update result is missing - triggering rollback")
            return 'run_remote_command_rollback'

        if update_result.get('created') is False:
            print("Update failed - triggering rollback")
            return 'run_remote_command_rollback'

        print("Update successful - proceeding to end task")
        return 'end_task'

    check_update = check_update_result()

    @task()
    def run_remote_command_rollback(**context):
        """
        Rollback task - revert interface to original state if Maat update fails.
        """
        from airflow.providers.ssh.hooks.ssh import SSHHook

        first_router = context['params']['first_router']
        second_router = context['params']['second_router']

        print(f"ROLLBACK: Maat update failed!")

        rollback_command = (
            f'ansible-playbook /home/ubuntu/ansible/playbooks/nokia_rollback_macvrf_tunnel.yaml '
            f'-i /home/ubuntu/ansible/inventory/hosts.ini '
            f'-e "router1_name={first_router} router2_name={second_router}"'
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
                    print(f"Successfully rolled back")
                    return {
                        'rolled_back': True,
                    }
                else:
                    print(f"Rollback command failed with exit status {exit_status}")
                    print(f"Error output:\n{error_output}")
                    raise Exception(f"Rollback failed with exit status {exit_status}")

        except Exception as e:
            print(f"Rollback execution failed: {str(e)}")
            raise Exception(f"Critical: Both Maat update and rollback failed! Manual intervention required. Error: {str(e)}")

    rollback_task = run_remote_command_rollback()

    @task
    def end_task():
        """
        End task - does nothing, just marks successful completion.
        """
        return {
            'status': 'completed'
        }

    end = end_task()

    # Define task dependencies
    validate_routers_task >> run_remote_command_task >> update_maat_task >> check_update

    # Branching after update check
    check_update >> rollback_task
    check_update >> end


# Instantiate the DAG
dag_instance = remote_ansible_setup_service_dag()
