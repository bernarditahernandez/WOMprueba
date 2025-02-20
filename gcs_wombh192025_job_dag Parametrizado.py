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
BUCKET_NAME = 'BUCKET PRUEBA'
FILE_NAME = 'TABLA PRUEBA.csv'
BQ_DATASET = 'DATA SET PRUEBA'

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
        schema_fields=[{"name": "id", "type": "INTEGER", "mode": "REQUIRED"}, #SE PUEDE REUTILIZAR LA TABLA DE EJEMPLO DE EMPLEADOS
        {"name": "name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "datetime", "type": "STRING", "mode": "NULLABLE"},
        {"name": "department_id", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "job_id", "type": "INTEGER", "mode": "NULLABLE"},]
    )
    
    transform_query = BigQueryExecuteQueryOperator( #VERIFIAR QUE LOS PARAMETROS CONSUERDEN 
    task_id='transform_query',
    sql='''CREATE OR REPLACE TABLE PROYECTO.DATASET.TABLA_TRANSFORMADA AS
           SELECT NOMBRE COLUMNA,NOMBRE COLUMNA2,ifnull(NOMBRE COLUMAN3 NUMERICA,0) as department_id 
           FROM PROYECTO.DATASET.TABLA;''',
    use_legacy_sql=False,
    dag=dag
    )
	
    load_hired_employees >> transform_query