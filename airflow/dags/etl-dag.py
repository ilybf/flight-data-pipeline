from datetime import datetime
from datetime import timedelta
import subprocess
from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

def call_procedure(procedure_name):
    hook = PostgresHook(postgres_conn_id="depi_db")
    conn = hook.get_conn()
    cursor = conn.cursor()

    try: 
        print(f"Starting {procedure_name} procedure execution...")
        cursor.execute(f"CALL {procedure_name};")

        conn.commit()
        print(f"Procedure {procedure_name} execution is done...")
    except Exception as e:
        conn.rollback()
        print(f"Procedure {procedure_name} failed: {e}")
        raise

    finally: 
        cursor.close()
        conn.close()

def run_talend_job():
    result = subprocess.run([
        '/bin/bash', '-c', 
        'cd /opt/airflow/talend_jobs/parent_job && PATH=/usr/bin:$PATH ./parent_job_run.sh'
    ], capture_output=True, text=True)
    
    
    if result.returncode != 0:
        raise Exception(f"Talend job failed: {result.stderr}")


with DAG(
    dag_id="flight_etl",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:
    
    copy_spark_script = BashOperator(
        task_id='copy_spark_script',
        bash_command="""
            # 1. Create directory on Spark Master
            docker exec spark-master mkdir -p /tmp/spark_drivers/ && \
            
            # 2. Cleanup old file if it exists
            docker exec spark-master rm -rf /tmp/spark_drivers/batch.py && \
            
            # 3. Copy the script
            docker cp /opt/airflow/scripts/pyspark/batch.py spark-master:/tmp/spark_drivers/batch.py
        """
    )

    copy_jdbc_jar = BashOperator(
        task_id='copy_jdbc_jar',
        bash_command="""
            # 1. Create directory on Spark Master (Internal Spark Jars folder)
            docker exec spark-master mkdir -p /spark/jars/ && \

            # 2. Remove old JAR to ensure no conflicts
            docker exec spark-master rm -f /spark/jars/postgresql-42.7.7.jar && \

            # 3. Copy the JAR file
            # Source path is based on your tree: drivers/folder/file.jar
            docker cp /opt/airflow/drivers/postgresql-42.7.7.jar/postgresql-42.7.7.jar spark-master:/spark/jars/postgresql-42.7.7.jar && \
            
            # 4. Fix permissions so the 'spark' user can read the file
            docker exec --user root spark-master chmod 644 /spark/jars/postgresql-42.7.7.jar
        """
    )

    spark_batch_processor = BashOperator(
        task_id='spark_batch_processor',
        bash_command="""
        docker exec spark-master /spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --jars /spark/jars/postgresql-42.7.7.jar \
        --driver-class-path /spark/jars/postgresql-42.7.7.jar \
        --conf spark.pyspark.python=/usr/bin/python3 \
        /tmp/spark_drivers/batch.py
        """
    )
    call_bronze_procedure = PythonOperator(
        task_id="call_bronze_procedure",
        python_callable=call_procedure,
        op_args=['bronze_layer()']
    )

    call_silver_procedure = PythonOperator(
        task_id="call_silver_procedure",
        python_callable=call_procedure,
        op_args=['silver_layer()']
    )

    call_gold_procedure = PythonOperator(
        task_id="call_gold_procedure",
        python_callable=call_procedure,
        op_args=['gold_layer()']
    )

    call_ml_procedure = PythonOperator(
        task_id="call_ml_procedure",
        python_callable=call_procedure,
        op_args=['machine_learning()']
    )
    debug_check_spark_files = BashOperator(
        task_id='debug_check_spark_files',
        bash_command="""
            
            # Use docker exec to run ls inside the Spark Master container
            docker exec spark-master ls -l /tmp/spark_drivers/
            
            echo "--- Debug check complete ---"
        """
    )

    debug_cd = BashOperator(
        task_id = 'debug_check_dir',
        bash_command='pwd' # Print current directory
    )

    run_python_script = BashOperator(
    task_id = 'python_etl_pipeline',
    bash_command="""
        echo "Starting ETL pipeline execution..." && \
        cd /opt/airflow/etl_scripts && \
        echo "Current directory is: $(pwd)" && \
        echo "Installing requirements..." && \
        /usr/local/bin/pip install --ignore-installed -r requirements.txt && \
        rm -rf ~/.kaggle/kagglehub/datasets/usdot/flight-delays/
        echo "Starting main.py script..." && \
        python main.py
    """
)
    


    run_talend_etl = PythonOperator(
        task_id="talend_etl_pipeline",
        python_callable=run_talend_job,
    )

    run_machine_learning_load = BashOperator(
        task_id="run_machine_learning_load",
        bash_command="""
        cd /opt/airflow/etl_scripts && \
        python machine_learning.py
    """
    )

    run_machine_learning_prediction = BashOperator(
        task_id="run_machine_learning_prediction",
        bash_command="""
        cd /opt/airflow/etl_scripts && \
        python ml_job.py
    """
    )
    call_bronze_procedure >> call_silver_procedure >> call_gold_procedure

    call_bronze_procedure >> run_python_script

    run_python_script >> run_talend_etl

    (
    run_talend_etl
    >> run_machine_learning_load
    >> run_machine_learning_prediction
    >> copy_spark_script
    >> copy_jdbc_jar
    >> debug_check_spark_files
    >> spark_batch_processor
)

    