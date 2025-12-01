"""
Airflow DAG to create SRLinux routers and VPN service.

This DAG demonstrates:
1. Creating two SRLinux router resources (Site A and Site B)
2. Creating a VPN service connecting the two routers
3. Verifying the created resources and service
"""

from datetime import datetime, timedelta
import json

from airflow.sdk import dag, task
from operators.maat_api_operator import (
    MaatAPIOperator,
    MaatResourceOperator,
    MaatServiceOperator
)


default_args = {
    'owner': 'network-admin',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


@dag(
    dag_id='srlinux_vpn_setup',
    default_args=default_args,
    description='Create SRLinux routers and configure VPN service',
    schedule=None,  # Manual trigger only
    start_date=datetime(2025, 11, 25),
    catchup=False,
    tags=['srlinux', 'vpn', 'network', 'routers'],
)
def srlinux_vpn_dag():
    """DAG to provision SRLinux routers and VPN service."""

    # Step 1: Create SRLinux Router at Site A
    create_router_site_a = MaatResourceOperator(
        task_id='create_srlinux_router_site_a',
        operation='create',
        resource_data={
            "category": "device.router",
            "description": "Nokia SRLinux Router - Site A",
            "name": "srlinux-site-a",
            "serialNumber": "SRL-7220-IXR-D3-2024-001",
            "resourceCharacteristic": [
                {"name": "vendor", "value": "Nokia"},
                {"name": "model", "value": "7220 IXR-D3"},
                {"name": "os-version", "value": "24.10.1"},
                {"name": "management-ip", "value": "192.168.10.1"},
                {"name": "site", "value": "Site-A"},
                {"name": "tunnel-interface", "value": "ethernet-1/1"},
                {"name": "tunnel-ip", "value": "10.0.1.1/30"}
            ],
            "@type": "PhysicalResource",
            "@schemaLocation": "https://bitbucket.software.geant.org/projects/OSSBSS/repos/maat-schema/raw/TMF639-ResourceInventory-v4-pionier.json"
        }
    )

    # Step 2: Create SRLinux Router at Site B
    create_router_site_b = MaatResourceOperator(
        task_id='create_srlinux_router_site_b',
        operation='create',
        resource_data={
            "category": "device.router",
            "description": "Nokia SRLinux Router - Site B",
            "name": "srlinux-site-b",
            "serialNumber": "SRL-7220-IXR-D3-2024-002",
            "resourceCharacteristic": [
                {"name": "vendor", "value": "Nokia"},
                {"name": "model", "value": "7220 IXR-D3"},
                {"name": "os-version", "value": "24.10.1"},
                {"name": "management-ip", "value": "192.168.20.1"},
                {"name": "site", "value": "Site-B"},
                {"name": "tunnel-interface", "value": "ethernet-1/1"},
                {"name": "tunnel-ip", "value": "10.0.1.2/30"}
            ],
            "@type": "PhysicalResource",
            "@schemaLocation": "https://bitbucket.software.geant.org/projects/OSSBSS/repos/maat-schema/raw/TMF639-ResourceInventory-v4-pionier.json"
        }
    )

    # Step 3: Update Router A to add relationship to Router B
    @task
    def update_router_a_relationship(**context):
        """Update Router A to add connection to Router B."""
        router_b_id = context['ti'].xcom_pull(task_ids='create_srlinux_router_site_b')['id']
        return {
            "resourceRelationship": [
                {
                    "relationshipType": "connects_to",
                    "resource": {
                        "id": router_b_id,
                        "name": "srlinux-site-b",
                        "@referredType": "PhysicalResource"
                    }
                }
            ]
        }

    update_router_a = MaatResourceOperator(
        task_id='update_router_a_add_relationship',
        operation='update',
        resource_id='{{ ti.xcom_pull(task_ids="create_srlinux_router_site_a")["id"] }}',
        resource_data='{{ ti.xcom_pull(task_ids="update_router_a_relationship") }}'
    )

    # Step 4: Update Router B to add relationship to Router A
    @task
    def update_router_b_relationship(**context):
        """Update Router B to add connection to Router A."""
        router_a_id = context['ti'].xcom_pull(task_ids='create_srlinux_router_site_a')['id']
        return {
            "resourceRelationship": [
                {
                    "relationshipType": "connects_to",
                    "resource": {
                        "id": router_a_id,
                        "name": "srlinux-site-a",
                        "@referredType": "PhysicalResource"
                    }
                }
            ]
        }

    update_router_b = MaatResourceOperator(
        task_id='update_router_b_add_relationship',
        operation='update',
        resource_id='{{ ti.xcom_pull(task_ids="create_srlinux_router_site_b")["id"] }}',
        resource_data='{{ ti.xcom_pull(task_ids="update_router_b_relationship") }}'
    )

    # Step 5: Create VPN Service between the two routers
    @task
    def prepare_vpn_service_data(**context):
        """Prepare VPN service data with router IDs."""
        router_a_id = context['ti'].xcom_pull(task_ids='create_srlinux_router_site_a')['id']
        router_b_id = context['ti'].xcom_pull(task_ids='create_srlinux_router_site_b')['id']

        return {
            "category": "network.vpn.ipsec",
            "description": "IPsec VPN Service between Site A and Site B SRLinux routers",
            "name": "vpn-siteA-siteB",
            "state": "active",
            "isServiceEnabled": True,
            "serviceCharacteristic": [
                {"name": "vpn-type", "value": "IPsec"},
                {"name": "encryption-algorithm", "value": "AES-256-GCM"},
                {"name": "ike-version", "value": "IKEv2"},
                {"name": "tunnel-mode", "value": "tunnel"},
                {"name": "local-network", "value": "10.100.1.0/24"},
                {"name": "remote-network", "value": "10.100.2.0/24"},
                {"name": "local-endpoint", "value": "192.168.10.1"},
                {"name": "remote-endpoint", "value": "192.168.20.1"},
                {"name": "pfs-group", "value": "group14"},
                {"name": "lifetime", "value": "3600"}
            ],
            "supportingResource": [
                {
                    "id": router_a_id,
                    "name": "srlinux-site-a",
                    "@referredType": "PhysicalResource"
                },
                {
                    "id": router_b_id,
                    "name": "srlinux-site-b",
                    "@referredType": "PhysicalResource"
                }
            ],
            "place": [
                {"id": "site-a", "name": "Data Center Site A", "role": "local site"},
                {"id": "site-b", "name": "Data Center Site B", "role": "remote site"}
            ],
            "@type": "Service",
            "@schemaLocation": "https://bitbucket.software.geant.org/projects/OSSBSS/repos/maat-schema/raw/TMF638-ServiceInventory-v4-pionier.json"
        }

    create_vpn_service = MaatServiceOperator(
        task_id='create_vpn_service',
        operation='create',
        service_data='{{ ti.xcom_pull(task_ids="prepare_vpn_service_data") }}'
    )

    # Step 6: Verify Router A
    verify_router_a = MaatResourceOperator(
        task_id='verify_router_site_a',
        operation='retrieve',
        resource_id='{{ ti.xcom_pull(task_ids="create_srlinux_router_site_a")["id"] }}'
    )

    # Step 7: Verify Router B
    verify_router_b = MaatResourceOperator(
        task_id='verify_router_site_b',
        operation='retrieve',
        resource_id='{{ ti.xcom_pull(task_ids="create_srlinux_router_site_b")["id"] }}'
    )

    # Step 8: Verify VPN Service
    verify_vpn = MaatServiceOperator(
        task_id='verify_vpn_service',
        operation='retrieve',
        service_id='{{ ti.xcom_pull(task_ids="create_vpn_service")["id"] }}'
    )

    # Step 9: Print summary
    @task
    def print_summary(**context):
        """Print summary of created resources and service."""
        router_a = context['ti'].xcom_pull(task_ids='verify_router_site_a')
        router_b = context['ti'].xcom_pull(task_ids='verify_router_site_b')
        vpn = context['ti'].xcom_pull(task_ids='verify_vpn_service')

        print("="*70)
        print("SRLINUX VPN SETUP SUMMARY")
        print("="*70)
        print(f"\n✓ Router Site A:")
        print(f"  ID: {router_a.get('id')}")
        print(f"  Name: {router_a.get('name')}")
        print(f"  Management IP: {[c['value'] for c in router_a.get('resourceCharacteristic', []) if c['name'] == 'management-ip'][0]}")

        print(f"\n✓ Router Site B:")
        print(f"  ID: {router_b.get('id')}")
        print(f"  Name: {router_b.get('name')}")
        print(f"  Management IP: {[c['value'] for c in router_b.get('resourceCharacteristic', []) if c['name'] == 'management-ip'][0]}")

        print(f"\n✓ VPN Service:")
        print(f"  ID: {vpn.get('id')}")
        print(f"  Name: {vpn.get('name')}")
        print(f"  State: {vpn.get('state')}")
        print(f"  Encryption: {[c['value'] for c in vpn.get('serviceCharacteristic', []) if c['name'] == 'encryption-algorithm'][0]}")
        print("\n" + "="*70)

    # Define task dependencies
    router_a_rel = update_router_a_relationship()
    router_b_rel = update_router_b_relationship()
    vpn_data = prepare_vpn_service_data()
    summary = print_summary()

    # Create routers in parallel
    [create_router_site_a, create_router_site_b] >> router_a_rel >> update_router_a
    [create_router_site_a, create_router_site_b] >> router_b_rel >> update_router_b

    # Create VPN service after both routers are updated
    [update_router_a, update_router_b] >> vpn_data >> create_vpn_service

    # Verify everything
    create_vpn_service >> [verify_router_a, verify_router_b, verify_vpn] >> summary


# Instantiate the DAG
dag_instance = srlinux_vpn_dag()

