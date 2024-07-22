from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.operators.bash import BashOperator


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 6, 27),
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG('bronze_to_silver_dag', default_args=default_args, description='bronze to silver', schedule_interval='@daily', catchup=False) as dag:
    datagen_credits_bronze = SSHOperator(
    task_id='bronze_credits',  
    ssh_conn_id='spark_ssh_conn',  # SSH bağlantısı için belirtilen bağlantı kimliği
    conn_timeout = None,
    cmd_timeout = None,
    command=""" cd /dataops/data-generator && \
    source datagen/bin/activate && \
    python /data-generator/dataframe_to_s3.py \
    -buc tmdb-bronze \
    -k credits/credits_part \
    -aki yourkey -sac yourpass \
    -eu http://minio:9000 \
    -i /dataops/tmdb_5000_credits.csv \
    -ofp True -z 500 -b 0.00001 -oh True""")

    
    datagen_movies_bronze = SSHOperator(task_id='bronze_movies',ssh_conn_id='spark_ssh_conn', 
    conn_timeout = None,
    cmd_timeout = None,
    command=""" cd /dataops/data-generator && \
    source datagen/bin/activate && \
    python /data-generator/dataframe_to_s3.py \
    -buc tmdb-bronze \
    -k movies/movies_part \
    -aki yourkey -sac yourpass \
    -eu http://minio:9000 \
    -i /dataops/tmdb_5000_movies.csv \
    -ofp True -z 500 -b 0.00001 -oh True""")


    # schema.py dosyasını çalıştıran görev
    credit_task_schema = SSHOperator(
        task_id='schema_credits',
        ssh_conn_id='spark_ssh_conn',
        conn_timeout=None,
        cmd_timeout=None,
        command="""cd /dataops/airflowenv && \
                   source bin/activate && \
                   python /dataops/schema_credits.py"""
    )
    
    # schema.py dosyasını çalıştıran görev
    movies_task_schema = SSHOperator(
        task_id='schema_movies',
        ssh_conn_id='spark_ssh_conn',
        conn_timeout=None,
        cmd_timeout=None,
        command="""cd /dataops/airflowenv && \
                   source bin/activate && \
                   python /dataops/schema_movies.py"""
    )

    # silver_movies görevini tanımla
    movies_s3_silver_task = SSHOperator(
        task_id='silver_movies',
        ssh_conn_id='spark_ssh_conn',
        conn_timeout=None,
        cmd_timeout=None,
        command="""cd /dataops/airflowenv && \
                   source bin/activate && \
                   python /dataops/movies_to_s3.py"""
    )

    # silver_credits görevini tanımla
    credits_s3_silver_task = SSHOperator(
        task_id='silver_credits',
        ssh_conn_id='spark_ssh_conn',
        conn_timeout=None,
        cmd_timeout=None,
        command="""cd /dataops/airflowenv && \
                   source bin/activate && \
                   python /dataops/credits_to_s3.py"""
    )

    # Görevleri sıralı olarak çalıştırma
    datagen_credits_bronze >> credit_task_schema >> credits_s3_silver_task
    datagen_movies_bronze >> movies_task_schema >> movies_s3_silver_task

    
    