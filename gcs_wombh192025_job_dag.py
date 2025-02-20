from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator


# Defino los parámetros básicos del DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Declaro variables
BUCKET_NAME = 'bucketwombh192025'
FILE_NAME = 'hired_employees.csv'
BQ_DATASET = 'projectwombh192025.DS_proyectoWOMBH192025'

# Creo el DAG 
with DAG(
    'gcs_wombh192025_job_dag',
    default_args=default_args,
    description='Carga datos desde GCS a BigQuery',
    schedule_interval=None,  
    start_date=datetime(2023, 12, 1),
    catchup=False,
) as dag:
    
    # task 1 para cargar datos desde GCS a BigQuery
    load_hired_employees = GCSToBigQueryOperator(
        task_id='load_hired_employees',
        bucket=BUCKET_NAME,
        source_objects=FILE_NAME,
        destination_project_dataset_table=f'{BQ_DATASET}.HIRED_EMPLOYEES',
        source_format='CSV',
        skip_leading_rows=1,  # Omitimos el encabezado del archivo CSV
        write_disposition='WRITE_TRUNCATE',  # Sobrescribe la tabla si ya existe
        create_disposition='CREATE_IF_NEEDED',  # Crea la tabla si no existe
        field_delimiter=',',  # Especificamos que el delimitador es coma
        schema_fields=[{"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "datetime", "type": "STRING", "mode": "NULLABLE"},
        {"name": "department_id", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "job_id", "type": "INTEGER", "mode": "NULLABLE"},]
    )
    
    transform_query = BigQueryExecuteQueryOperator(
    task_id='transform_query',
    sql='''CREATE OR REPLACE TABLE projectwombh192025.DS_proyectoWOMBH192025.HIRED_EMPLOYEES_TRF AS
           SELECT name,datetime,ifnull(department_id,0) as department_id 
           FROM projectwombh192025.DS_proyectoWOMBH192025.HIRED_EMPLOYEES;''',
    use_legacy_sql=False,
    dag=dag
    )
	
    load_hired_employees >> transform_query