# imports
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.sqlite.operators.sqlite import SqliteOperator


from datetime import datetime
import sqlite3
import pandas as pd


# DAG default arguments 
default_args = {
    'owner': 'supersanzy',
    'start_date': datetime(2025,5,1)
}


def load_data():
# sqlite3 connection
    connection = sqlite3.connect('health.db')
    df = pd.read_csv('smoking_health_data.csv')
    df.to_sql('smoking_health_data', con=connection, if_exists='replace', index=False)
    connection.close()


# DAG definitions
with DAG(dag_id='etl_project', description='attempting etl with airflow', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    # task definitions
    download_file = BashOperator(
        task_id="download_file",
        bash_command='curl -L -o ~/project_python/smokers-health-data.zip \
                    https://www.kaggle.com/api/v1/datasets/download/jaceprater/smokers-health-data'      
    )

    unzip_file = BashOperator(
        task_id='unzip_file',
        bash_command='unzip /home/supersanzy/project_python/smokers-health-data.zip -d /home/supersanzy/project_python/'
    )

    rename_file = BashOperator(
        task_id= 'rename_file',
        bash_command= 'mv /home/supersanzy/project_python/smoking_health_data_final.csv /home/supersanzy/project_python/smoking_health_data.csv'
    )

    create_table = SqliteOperator(
        task_id='create_table',
        sqlite_conn_id='db_sqlite',
        sql='''
                CREATE TABLE IF NOT EXISTS smoking_health_data (
                    age INTEGER,
                    sex TEXT,
                    curreny_smoker TEXT,
                    heart_rate INTEGER,
                    blood_pressure TEXT,
                    cigs_per_day REAL,
                    chol REAL
                );
            '''
    )

    load = PythonOperator(
        task_id= 'loading_data',
        python_callable=load_data
    )


# task_pipeline
download_file >> unzip_file >> rename_file >> create_table >> load