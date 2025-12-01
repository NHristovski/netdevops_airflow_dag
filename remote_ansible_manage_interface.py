from airflow.sdk import dag, task, Param
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime


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

    # SSHOperator doesn't have a TaskFlow decorator, so we use it directly
    run_remote_command = SSHOperator(
        task_id='execute_ansible_on_remote_vm',
        ssh_conn_id='ansible-ssh',
        command='ansible-playbook /home/ubuntu/ansible/playbooks/nokia_change_interface_state.yaml -i /home/ubuntu/ansible/inventory/hosts.ini -e "router={{ params.router }} interface={{ params.interface }} state={{ params.state }}"',
    )


# Instantiate the DAG
dag_instance = ssh_remote_ansible_dag()
