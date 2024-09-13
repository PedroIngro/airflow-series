from airflow import DAG
from datetime import datetime
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator

# dag = DAG(
#             dag_id = "01_first_dag",
#             start_date = datetime(2024, 7, 21)
#         )
# task1 = EmptyOperator(task_id = "extract", dag = dag)
# task2 = EmptyOperator(task_id = "load", dag = dag)
# task3 = EmptyOperator(task_id = "transform", dag = dag)

# task1 >> task2 >> task3

with DAG(
            dag_id = "01_first_dag",
            start_date = datetime(2024, 7, 21),
            schedule_interval= "@weekly"
        ) as dag:
    task1 = EmptyOperator(task_id = "extract", dag = dag)
    task2 = EmptyOperator(task_id = "load", dag = dag)
    task3 = EmptyOperator(task_id = "transform", dag = dag)

    task1 >> task2 >> task3

# @DAG(
#         start_date = datetime(2024, 7, 21)
#     )