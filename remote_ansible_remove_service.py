from datetime import datetime

from airflow.exceptions import AirflowException
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.sdk import dag, task, Param

from operators.maat_api_operator import (
    MaatServiceOperator,
    OperationType
)


@dag(
    dag_id='ssh_ansible_remove_service',
    start_date=datetime(2025, 12, 15),
    schedule=None,
    catchup=False,
    doc_md="Version: 1.0",
    tags=['ssh', 'ansible', 'deletion', 'maat'],
    params={
        'service_id': Param(
            type='string',
            description='ID of the service to be removed'
        ),
    }
)
def remote_ansible_remove_service_dag():
    """
    DAG that executes ansible on remote VM to remove service between two routers and prints the output.
    """

    @task
    def validate_params(**context):
        service_id = context['params']['service_id']

        print(f"Checking if service with id '{service_id}' exists in Maat...")

        service_check_operator = MaatServiceOperator(
            task_id='check_service_exists',
            operation=OperationType.RETRIEVE,
            service_id=service_id
        )

        service_result = service_check_operator.execute(context)

        # Check if service exists
        if service_result and service_result.get('http_status_code') == 200:
            print(f"Service '{service_id}' exists in Maat")

            # Extract router information from supportingResource
            supporting_resources = service_result.get('supportingResource', [])

            if len(supporting_resources) < 2:
                raise AirflowException(
                    f"Service '{service_id}' does not have enough supporting resources (routers). Expected 2, found {len(supporting_resources)}"
                )

            first_router_name = supporting_resources[0].get('name')
            first_router_id = supporting_resources[0].get('id')
            second_router_name = supporting_resources[1].get('name')
            second_router_id = supporting_resources[1].get('id')

            print(f"First router: {first_router_name} (ID: {first_router_id})")
            print(f"Second router: {second_router_name} (ID: {second_router_id})")

            return {
                'service_id': service_id,
                'first_router': first_router_name,
                'second_router': second_router_name,
                'first_router_id': first_router_id,
                'second_router_id': second_router_id
            }
        else:
            raise AirflowException(
                f"Failed to retrieve service '{service_id}' from Maat."
            )

    validate_service_task = validate_params()

    @task
    def run_remote_command(**context):
        """
        Execute Ansible playbook via SSH.
        """
        from airflow.providers.ssh.hooks.ssh import SSHHook

        ti = context['ti']

        # Get router names from validation task
        validation_result = ti.xcom_pull(task_ids='validate_params')
        first_router = validation_result.get('first_router')
        second_router = validation_result.get('second_router')

        command = (
            f'ansible-playbook /home/ubuntu/ansible/playbooks/nokia_rollback_macvrf_tunnel.yaml '
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
    def delete_service_in_maat(**context):
        """
        Delete the service from Maat after successful Ansible rollback execution.
        """
        ti = context['ti']

        # Get service ID from validation task
        validation_result = ti.xcom_pull(task_ids='validate_params')
        service_id = validation_result.get('service_id')

        print(f"Deleting service with ID '{service_id}' from Maat...")

        # Delete service using MaatServiceOperator
        service_operator = MaatServiceOperator(
            task_id='delete_service_operator',
            operation=OperationType.DELETE,
            service_id=service_id
        )

        result = service_operator.execute(context)

        print(f"Delete result: {result}")

        if result and result.get('http_status_code') == 204:
            print(f"Service '{service_id}' deleted successfully from Maat")
            return {
                'deleted': True,
                'service_id': service_id
            }
        else:
            print(f"Failed to delete service '{service_id}' from Maat")
            return {
                'deleted': False,
                'service_id': service_id
            }

    delete_maat_task = delete_service_in_maat()


    @task.branch(trigger_rule='all_done')
    def check_delete_result(**context):
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

        delete_result = ti.xcom_pull(task_ids='delete_service_in_maat')

        print(f"Update result: {delete_result}")

        if not delete_result:
            print("Delete result is missing - triggering rollback")
            return 'run_remote_command_rollback'

        if delete_result.get('deleted') is False:
            print("Delete failed - triggering rollback")
            return 'run_remote_command_rollback'

        print("Delete successful - proceeding to end task")
        return 'end_task'


    check_delete = check_delete_result()


    @task()
    def run_remote_command_rollback(**context):
        """
        Rollback task - creates the service again via Ansible if deletion failed.
        """
        from airflow.providers.ssh.hooks.ssh import SSHHook

        # Get router names from validation task
        validation_result = ti.xcom_pull(task_ids='validate_params')
        first_router = validation_result.get('first_router')
        second_router = validation_result.get('second_router')

        print(f"ROLLBACK: Maat update failed!")

        rollback_command = (
            f'ansible-playbook /home/ubuntu/ansible/playbooks/nokia_setup_macvrf_tunnel.yaml '
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
            raise Exception(
                f"Critical: Both Maat update and rollback failed! Manual intervention required. Error: {str(e)}")


    rollback_task = run_remote_command_rollback()


    @task
    def end_task():
        """
        End task - does nothing, just marks successful completion.
        """
        return {
            'status': 'completed'
        }


    success = end_task()

    # Define task dependencies
    validate_service_task >> run_remote_command_task >> delete_maat_task >> check_delete_result

    # Branching after delete check
    check_delete_result >> rollback_task
    check_delete_result >> success

# Instantiate the DAG
dag_instance = remote_ansible_remove_service_dag()
