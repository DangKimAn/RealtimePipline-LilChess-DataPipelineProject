from datetime import datetime
from airflow.decorators import dag, task

@dag(
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["example"],
)
def my_python_workflow():
    
    @task
    def extract_data():
        return {"data": [1, 2, 3, 4, 5]}

    @task
    def process_data(input_dict):
        data = input_dict["data"]
        squared = [x**2 for x in data]
        return squared

    # Setting up the task dependency
    raw_data = extract_data()
    process_data(raw_data)

# Instantiate the DAG
my_python_workflow()
