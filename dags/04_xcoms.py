from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json





def ingest(ti): #TASK INSTANCE (Representa a instância de uma task dentro do backend do airflow)
  
  import pandas as pd
  from unidecode import unidecode

  url = "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/vendas-gasolina-c-m3-2020.csv"

  df = pd.read_csv(url, delimiter = ";", encoding = "utf-8")
  df.columns = [unidecode(column.strip()) for column in df.columns]
  df = df.fillna(0)

  for col in df.columns:
      if df[col].dtype == float or col == 'TOTAL':
          df[col] = df[col].astype(str).str.replace('.', '', regex=False).astype(float)
  
  id_vars = ["COMBUSTIVEL", "ANO", "REGIAO", "ESTADO", "UNIDADE", "TOTAL"]
  value_vars = [column for column in df.columns if column not in id_vars]

  df_melted = pd.melt(df, id_vars = id_vars, value_vars = value_vars)
  df_melted["contribution"] = df_melted["value"] / df_melted["TOTAL"]
  df_melted["contribution_pct"] = round((df_melted["value"] / df_melted["TOTAL"]) * 100, 3)

  max_contribution = df_melted["contribution"].max()
  min_contribution = df_melted[df_melted["contribution"] != 0]["contribution"].min()

  # ti.xcom_push(key = "max_contribution", value = max_contribution)
  # ti.xcom_push(key = "min_contribution", value = min_contribution)
  return {"max_contribution" : max_contribution, "min_contribution": min_contribution}
# A importância de fazer um return dict, é que você cria apenas um resgistro e faz apenas uma requisição tbm.
  

def consume(ti):
   max_contribution = ti.xcom_pull(key = "max_contribution", task_ids = "ingest")
   min_contribution = ti.xcom_pull(key = "min_contribution", task_ids = "ingest")
   print(f"max_contribution: {max_contribution}")
   print(f"min_contribution: {min_contribution}")

def consume_return (ti):
   return_value = ti.xcom_pull(key = "return_value", task_ids = "ingest")
   max_contribution = return_value["max_contribution"]
   min_contribution = return_value["min_contribution"]
   print(f"max_contribution: {max_contribution}")
   print(f"min_contribution: {min_contribution}")

def consume_gingar(xcom): #Nome do parâmetro não importa ao contrário do TI (Reservado)
   return_value = json.loads(xcom.replace("'", '"'))
   print(return_value)
   print(f"Tipo do Dado Recebido {type(return_value)} ")
   max_contribution = return_value["max_contribution"]
   min_contribution = return_value["min_contribution"]
   print(f"max_contribution: {max_contribution}")
   print(f"min_contribution: {min_contribution}")
   
# def ingest_v2(**kwargs): #TASK INSTANCE (Representa a instância de uma task dentro do backend do airflow)
  
#   import pandas as pd
#   from unidecode import unidecode

#   url = "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/vendas-gasolina-c-m3-2020.csv"

#   df = pd.read_csv(url, delimiter = ";", encoding = "utf-8")
#   df.columns = [unidecode(column.strip()) for column in df.columns]
#   df = df.fillna(0)

#   for col in df.columns:
#       if df[col].dtype == float or col == 'TOTAL':
#           df[col] = df[col].astype(str).str.replace('.', '', regex=False).astype(float)
  
#   id_vars = ["COMBUSTIVEL", "ANO", "REGIAO", "ESTADO", "UNIDADE", "TOTAL"]
#   value_vars = [column for column in df.columns if column not in id_vars]

#   df_melted = pd.melt(df, id_vars = id_vars, value_vars = value_vars)
#   df_melted["contribution"] = df_melted["value"] / df_melted["TOTAL"]
#   df_melted["contribution_pct"] = round((df_melted["value"] / df_melted["TOTAL"]) * 100, 3)

#   max_contribution = df_melted["contribution"].max()
#   min_contribution = df_melted[df_melted["contribution"] != 0]["contribution"].min()

#   ti = kwargs["ti"]
#   ti.xcom_push(key = "max_contribution", value = max_contribution)
#   ti.xcom_push(key = "min_contribution", value = min_contribution)

# def ingest_v3(**context): #TASK INSTANCE (Representa a instância de uma task dentro do backend do airflow)
  
#   import pandas as pd
#   from unidecode import unidecode

#   url = "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/vendas-gasolina-c-m3-2020.csv"

#   df = pd.read_csv(url, delimiter = ";", encoding = "utf-8")
#   df.columns = [unidecode(column.strip()) for column in df.columns]
#   df = df.fillna(0)

#   for col in df.columns:
#       if df[col].dtype == float or col == 'TOTAL':
#           df[col] = df[col].astype(str).str.replace('.', '', regex=False).astype(float)
  
#   id_vars = ["COMBUSTIVEL", "ANO", "REGIAO", "ESTADO", "UNIDADE", "TOTAL"]
#   value_vars = [column for column in df.columns if column not in id_vars]

#   df_melted = pd.melt(df, id_vars = id_vars, value_vars = value_vars)
#   df_melted["contribution"] = df_melted["value"] / df_melted["TOTAL"]
#   df_melted["contribution_pct"] = round((df_melted["value"] / df_melted["TOTAL"]) * 100, 3)

#   max_contribution = df_melted["contribution"].max()
#   min_contribution = df_melted[df_melted["contribution"] != 0]["contribution"].min()

#   ti = context["ti"]
#   ti.xcom_push(key = "max_contribution", value = max_contribution)
#   ti.xcom_push(key = "min_contribution", value = min_contribution)

def consume(ti):
   max_contribution = ti.xcom_pull(key = "max_contribution", task_ids = "ingest")
   min_contribution = ti.xcom_pull(key = "min_contribution", task_ids = "ingest")
   print(f"max_contribution: {max_contribution}")
   print(f"min_contribution: {min_contribution}")

with DAG(
          dag_id = "04_xcoms",
          start_date = datetime.today(),
          schedule = None
        ) as dag:
  
  task1 = PythonOperator(
                            task_id = "ingest",
                            python_callable = ingest

                        )
  task2 = PythonOperator(
                             task_id = "consume",
                             python_callable = consume_gingar,
                             op_kwargs = {
                                            "xcom": """{{ti.xcom_pull(key="return_value", task_ids="ingest")}}"""
                                        }
                                        #A utilização do gingar template aproveita a abertura de conexão com o banco quando está fazendo o parse da DAG.
                                        # Se for utilizado dentro da task (TI), ele terá que fazer uma segunda conexão para poder pegar os parâmetros
                        )
  task1 >> task2

# Eu posso colocar o xcom em um servidor tipo S3 ao invés de um banco tradicional. Custom Backends
# É recomendado utilizar os xcoms apenas para transporte de metadados.
# Não é recomendado passar datasets completos pelo xcom