"""
Dummy spark job to test airflow
"""

from datetime import datetime, timedelta

from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator

# from utils.spark import SparkOperator

# pylint: disable=C0103 # Disable warning about uppercase var names
# pylint: disable=W0104 # Disable warning about non-effect statements on tasks

default_args = {
    "owner": "foo",
    "depends_on_past": False,
    "start_date": datetime(2015, 6, 1),
    "email": ["foo@bar.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG("test-dag", default_args=default_args, schedule_interval=timedelta(1))

# t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = BashOperator(task_id="print_date", bash_command="date", dag=dag)

t2 = BashOperator(task_id="sleep", bash_command="sleep 5", retries=3, dag=dag)

templated_command = """
    {% for i in range(5) %}
        echo "{{ ds }}"
        echo "{{ macros.ds_add(ds, 7)}}"
        echo "{{ params.my_param }}"
    {% endfor %}
"""

t3 = BashOperator(
    task_id="templated",
    bash_command=templated_command,
    params={"my_param": "Parameter I passed in"},
    dag=dag,
)

t2.set_upstream(t1)
t3.set_upstream(t1)
