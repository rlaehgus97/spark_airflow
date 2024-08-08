from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonVirtualenvOperator, BranchPythonOperator

with DAG(
    'pyspark_movie',
    default_args={
        'depends_on_past': True,
        'retries': 1,
        'retry_delay': timedelta(seconds=3),
    },
    description='movie data with pyspark and airflow',
    schedule="10 4 * * * ",
    start_date=datetime(2015, 1, 1),
    end_date=datetime(2015, 1, 30),
    catchup=True,
    tags=['pyspark', 'spark',  'movies'],
) as dag:

    def repartition(ds_nodash):
        from spark_air.repartition import re_partition

        repartition(ds_nodash)

    def path_exist(ds_nodash)
        import os

        home=os.path.expanduser("~")
        if(os.path.exists(f"{home}/data/movie/reparition/load_dt={ds_nodash}")):
            return "rm.dir"
        else:
            return "re.partition"
#Task
    re_partition = PythonVirtualenvOperator(
        task_id='re.partition',
        requirements=["git+https://github.com/rlaehgus97/spark_airflow.git"],
        system_site_packages=False,
        python_callable=repartition,
        trigger_rule="none_failed"
    )

    join_df = BashOperator(
        task_id='join.df',
        bash_command="""
            $SPARK_HOME/bin/spark-submit ~/airflow_pyspark/py/join_df.py
        """
    )

    agg = BashOperator(
        task_id='agg.df',
        bash_command="""
            $SPARK_HOME/bin/spark-submit ~/airflow_pyspark/py/agg.py
        """
    )
    
    rm_dir = BashOperator(
        task_id="rm.dir",
        bash_command="""
            rm -rf ~/data/movie/repartition/load_dt={{ds_nodash}}
        """
    )

    task_branch = BranchPythonOperator(
        task_id="branch.op"
        python_callable=path_exist,
    )

    task_start = gen_emp('start')
    task_end = gen_emp('end','all_done')

#Graph
    task_start >> task_branch >> [re_partition, rm_dir]
    re_partition >> join_df >> agg >> task_end
    rm_dir >> re_partition
