from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import (
	PythonOperator, 
	PythonVirtualenvOperator,
	BranchPythonOperator
)

def gen_emp(id, rule='one_success'):
	op = EmptyOperator(task_id=id, trigger_rule=rule)
	return op

with DAG(
    'pyspark_movie',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3),
    },
    max_active_runs=1,
    max_active_tasks=3,
    description='movie data with pyspark and airflow',
    schedule="10 4 * * * ",
    start_date=datetime(2024, 8, 8),
    catchup=True,
    tags=['pyspark', 'spark',  'movies'],
) as dag:
	REQ=[
            "git+https://github.com/rlaehgus97/spark_airflow.git",
    ]

#Task

    re_partition = PythonVirtualenvOperator(
        task_id='re.partition',
        requirements=REQ,
        system_site_packages=False,
        python_callable=get_parq,
        op_kwargs = {'date_start':datetime(2018,12,31), 'date_lim':'20190530'}
        )

    join_df = BashOperator(
        task_id='join.df'
        )

    agg = BashOperator(
        task_id='agg.df',
        )

	task_start = gen_emp('start')
	task_end = gen_emp('end','all_done')

#Graph
task_start >> re_partition >> join_df >> agg >> task_end
