# NetDevOps Airflow DAG

Apache Airflow DAGs for automating network service provisioning and management on Nokia SR Linux routers using Ansible and MAAT API integration.

## DAGs

- **import_data_to_maat**: Imports router data into MAAT inventory
- **remote_ansible_setup_service**: Provisions MAC-VRF tunnel service between two routers via Ansible and registers it in MAAT inventory
- **remote_ansible_remove_service**: Removes MAC-VRF tunnel service from routers and deletes it from MAAT inventory
- **remote_ansible_manage_interface**: Manages router interface state (up/down) via Ansible and updates MAAT resource inventory

## MAAT Operator

Custom Airflow operators for interacting with MAAT API:

- **MaatResourceOperator**: Manages physical/logical resources (routers, interfaces) - supports LIST, CREATE, RETRIEVE, RETRIEVE_BY_NAME, UPDATE, DELETE
- **MaatServiceOperator**: Manages network services with supporting resources - supports LIST, CREATE, RETRIEVE, RETRIEVE_BY_NAME, UPDATE, DELETE

