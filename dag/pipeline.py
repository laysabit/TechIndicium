import pandas as pd
import datetime as dt
import pathlib
import os
import sys
import airflow
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

root_path = str( pathlib.Path(__file__).parent.parent.resolve() ) + "/data/csv"

def get_names_all_tables():
    sql_stmt = "SELECT * FROM information_schema.tables;"
    pg_hook = PostgresHook(
        postgres_conn_id='northwind_conn',
    )
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    cursor.execute(sql_stmt)
    results = cursor.fetchall()

    table_names = [result[2] for result in results if result[1]=='public']
    
    return table_names
    

def get_tables_data(ti):
    
    names = ti.xcom_pull(task_ids='get_names_of_all_tables')
    
    pg_hook = PostgresHook(
        postgres_conn_id='northwind_conn',
    )
    
    for table_name in names:
        
        pg_conn = pg_hook.get_conn()
        cursor = pg_conn.cursor()
        
        cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = '%s'; " %( table_name ) ) 
        rows = cursor.fetchall()
        columns_names = [ row[0] for row in rows ]
        
        cursor.execute( "SELECT * FROM " + table_name + " ; " )
        rows.clear()
        rows = cursor.fetchall()

        table_name_df = pd.DataFrame(
            data=rows,
            columns=columns_names
        )

        # Mantendo propriedades do df    
        df2 = table_name_df.copy()
        df2.index = df2.index + 1
        df2.sort_index(inplace=True)
        
        path_to_create = os.path.join( root_path, table_name ) + ".csv"
        
        df2.to_csv(path_to_create, index=False, header=False, sep="|")

def insert_in_output_tables(ti):
    names = ti.xcom_pull(task_ids='get_names_of_all_tables')
    
    for table_name in names:
        
        pg_hook1 = PostgresHook(
            postgres_conn_id='northwind_conn',
        )
        
        pg_conn1 = pg_hook1.get_conn()
        cursor1 = pg_conn1.cursor()
        
        cursor1.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = '%s'; " %( table_name ) ) 
        rows = cursor1.fetchall()
        columns_names = [ row[0] for row in rows ]
        
        pg_hook = PostgresHook(
            postgres_conn_id='db_results_conn',
        )
        
        pg_conn = pg_hook.get_conn()
        cursor = pg_conn.cursor()
        
        path = os.path.join( root_path, table_name ) + ".csv"
        
        with open(path, 'r') as f:
            cursor.copy_from(f, table_name, sep='|' , null = '\\\\N', columns = columns_names)
        
        pg_conn.commit()
        
    
default_args = {
        'owner': 'Laysa',    
        'start_date': airflow.utils.dates.days_ago(1),
        'depends_on_past': True,
        'retries': 1,
        'retry_delay': dt.timedelta(minutes=5),
        }

with DAG(
    dag_id='Challenge',
    default_args=default_args,
    schedule_interval='@daily',
    description='It is the whole pipeline',
) as dag:
    
    task_get_names_all_tables = PythonOperator(
        task_id='get_names_of_all_tables',
        python_callable=get_names_all_tables,
        do_xcom_push=True,
    )
    
    task_get_all_tables_data = PythonOperator(
        task_id='get_all_tables_data',
        python_callable=get_tables_data,
    )
    
    task_create_output_tables = PostgresOperator(
        task_id = "create_output_table",
        postgres_conn_id = "db_results_conn",
        sql = "/airflow_results_db.sql",
    )
    
    task_insert_in_output_tables = PythonOperator(
        task_id='insert_in_output_tables',
        python_callable=insert_in_output_tables,
    )
    
    task_get_names_all_tables >> task_get_all_tables_data >> task_create_output_tables >> task_insert_in_output_tables
