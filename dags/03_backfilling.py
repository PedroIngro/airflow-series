from airflow import DAG
from airflow.operators.empty import EmptyOperator

from datetime import datetime

# BackFilling é a capacidade do airflow de executar os dias que se passaram desde a start_date até a data atual

with DAG(
          dag_id = "03_backfilling", 
          start_date = datetime(2023, 12, 25), 
          schedule= "@daily",
          catchup = False #True por default
        ) as dag:
  task1 = EmptyOperator(task_id = "first_task")
  task2 = EmptyOperator(task_id = "second_task")
  task3 = EmptyOperator(task_id = "thrid_task")


  task1 >> [task2, task3]