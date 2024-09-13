from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime

def example_function(id:str):
  print(f"Testando o Python Decorator. {id}")

with DAG (
            dag_id = "02_python_decorator_example", 
            start_date = datetime(2024, 7, 21), 
            schedule = None
          )as dag:
  task1 = PythonOperator(
                            task_id = "testing_python_decorator",
                            python_callable = example_function,
                            op_args = ["Teste de Parâmetro"]
                        )
  task2 = PythonOperator(
                          task_id = "testing_kwargs_decorator",
                          python_callable = example_function,
                          op_kwargs= {"id": "Teste de Kwargs"}
                      )
  
  task1 >> task2