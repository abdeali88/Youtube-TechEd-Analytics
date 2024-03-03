from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from youtube_extraction import fetch_video_data

# topics
topics = ["Machine Learning", "Cyber Security", "Python", "Data Science", "Data Engineering", "Data Analytics", "Java", "Cloud Computing", "DevOps", "Blockchain", "Web Development", "Mobile App Development", "Data Structures", "Algorithms"]

# Default DAG args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialize DAG
dag = DAG(
    'youtube_extraction_dag',
    default_args=default_args,
    description='DAG to extract YouTube videos',
    schedule_interval='@daily',  # Set to run daily for each topic
)


start = DummyOperator(
    task_id='start',
    dag=dag,
)

end = DummyOperator(
    task_id='end',
    dag=dag,
)

# Define tasks for each topic
for topic in topics:
    task_id = f'extract_{topic.replace(" ", "_")}'
    extract_task = PythonOperator(
        task_id=task_id,
        python_callable=fetch_video_data,
        op_args=[topic],
        provide_context=True,
        dag=dag,
    )

    start >> extract_task >> end

if __name__ == "__main__":
    dag.cli()
