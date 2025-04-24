# dags/full_pipeline_dag.py
from airflow import DAG
from airflow.operators.bash import BashOperator  # Nouvelle syntaxe pour Airflow 2.x
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator  # Nouvelle syntaxe pour Airflow 2.x
import datetime
import os

# Configuration pour Docker Airflow
# Utiliser le chemin des DAGs qui est déjà monté dans Docker
# Tous les fichiers du projet sont copiés dans le dossier dags
PROJECT_PATH = "/opt/airflow/dags"  # Utiliser le dossier dags déjà monté

default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG(
    'full_pipeline',
    default_args=default_args,
    schedule_interval="@hourly",
    catchup=False,
    tags=['nyc_taxi', 'weather'],
) as dag:
    
    # Tâche pour extraire les données Yellow Taxi - utilise un script Python dans le conteneur
    extract_yellow = BashOperator(
        task_id='extract_yellow',
        bash_command='python {{ params.script_path }}',
        params={'script_path': '/opt/airflow/dags/extract/fetch_yellow_trip.py'},
        env={'PYTHONPATH': '/opt/airflow/dags'},
    )
    
    # Tâche pour lancer le traitement batch via PySpark
    spark_transform = BashOperator(
        task_id='spark_transform',
        bash_command='python {{ params.script_path }}',
        params={'script_path': '/opt/airflow/dags/process/spark_batch.py'},
        env={'PYTHONPATH': '/opt/airflow/dags'},
    )
    
    # Tâche pour charger les données taxi dans PostgreSQL
    load_to_postgres = BashOperator(
        task_id='load_to_postgres',
        bash_command='python {{ params.script_path }}',
        params={'script_path': '/opt/airflow/dags/process/load_to_postgres.py'},
        env={'PYTHONPATH': '/opt/airflow/dags'},
    )
    
    # Tâche pour extraire les données météo (streaming simulé)
    extract_weather = BashOperator(
        task_id='extract_weather',
        bash_command='python {{ params.script_path }}',
        params={'script_path': '/opt/airflow/dags/extract/fetch_weather_stream.py'},
        env={'PYTHONPATH': '/opt/airflow/dags'},
    )
    
    # Tâche pour lancer le job PyFlink (simulé) pour traiter les fichiers JSON météo
    flink_transform = BashOperator(
        task_id='flink_transform',
        bash_command='python {{ params.script_path }}',
        params={'script_path': '/opt/airflow/dags/process/flink_stream.py'},
        env={'PYTHONPATH': '/opt/airflow/dags'},
    )
    
    # Tâche pour exécuter les transformations DBT
    run_dbt = BashOperator(
        task_id='run_dbt',
        bash_command='cd /opt/airflow/dags/dbt && dbt run',
        env={'DBT_PROFILES_DIR': '/opt/airflow/dags/dbt'},
    )
    
    # Définition du flux de travail
    extract_yellow >> spark_transform >> load_to_postgres
    extract_weather >> flink_transform
    [load_to_postgres, flink_transform] >> run_dbt
