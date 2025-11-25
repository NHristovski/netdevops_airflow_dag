"""
DAG that prints numbers 1-10 in new lines
"""
from datetime import datetime, timedelta
from airflow.sdk import dag, task


@dag(
    dag_id='print_numbers_1_to_10',
    description='A simple DAG that prints numbers from 1 to 10',
    schedule=None,  # Manual trigger only
    start_date=datetime(2025, 11, 2),
    catchup=False,
    tags=['example', 'tutorial'],
)
def print_numbers_dag():
    """
    This DAG prints numbers from 1 to 10, each on a new line.
    """

    @task
    def print_numbers():
        """
        Task that prints numbers 1-10
        """
        print("Starting to print numbers from 1 to 10:")
        print("-" * 40)
        for i in range(1, 11):
            print(f"{i}")
        print("-" * 40)
        print("Finished printing numbers!")
        return "Task completed successfully"

    # Execute the task
    print_numbers()


# Instantiate the DAG
dag_instance = print_numbers_dag()

