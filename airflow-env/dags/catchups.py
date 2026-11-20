"""
catchup dag
"""

import textwrap
from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

default_args={
        "depends_on_past": False,
        "email": ["email@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(seconds=15),
        # 'queue': 'bash_queue',x``
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2026, 11, 18),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        'execution_timeout': timedelta(seconds=3),
        # 'on_failure_callback': some_function, # or list of functions
        # 'on_success_callback': some_other_function, # or list of functions
        # 'on_retry_callback': another_function, # or list of functions
        # 'sla_miss_callback': yet_another_function, # or list of functions
        # 'trigger_rule': 'all_success'
        'owner': 'Casimodo',
    }

with DAG(
    "catchup-dag",
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args=default_args,
    description="catchup DAG",
    #schedule=timedelta(days=1),
    schedule="0 10 * * 1-5",
    start_date=datetime(2026, 11, 15),
    catchup=False,
    tags=["catchup", "alan" ],
) as dag:

    t1 = BashOperator(
        task_id="print_exec_date",
        bash_command="echo 'Exec for {{ ts }}'",
        retries=3,   
    ) 

    t2 = BashOperator(
        task_id="fail", 
        bash_command='sleep 5',
        retries=3
    )


    
    dag.doc_md = __doc__
