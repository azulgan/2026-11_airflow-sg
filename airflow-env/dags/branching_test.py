"""
branching test DAG
"""

import pandas as pd
import textwrap
from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.models import Variable
#from airflow.models.taskinstance import TaskInstance
from airflow.operators.python import BranchPythonOperator
import random

#the_operator = Variable.get("the_operator")

def calculate_volume():
    data = pd.read_csv("/tmp/flower.csv")
    data["volume"] = data["sepal_length"] * data["sepal_width"]
    data = data.sort_values(by="volume", ascending=False)
    print(data.head(5))
    print("Which branch ?")
    print(decide_branch())
    data.to_csv("/tmp/enriched.csv")

#def decide_branch(seconds):
#    # Si le jour est un jour de semaine (lundi à vendredi)
#    if seconds % 2 == 0:
#        return 'travailler'
#    else:
#        return 'et_voler_c_est_pas_beau'


def decide_branch():
    return random.choice(['travailler_c_trop_dur', 'et_voler_c_est_pas_beau'])

default_args={
        "depends_on_past": False,
        "email": ["{{operator_email}}"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(seconds=60),
        "start_date": datetime(2025, 3, 20),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2026, 11, 18),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function, # or list of functions
        # 'on_success_callback': some_other_function, # or list of functions
        # 'on_retry_callback': another_function, # or list of functions
        # 'sla_miss_callback': yet_another_function, # or list of functions
        # 'trigger_rule': 'all_success'
    }

with DAG(
    "branching-test-dag",
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args=default_args,
    description="Branching Test DAG",
    schedule=timedelta(days=1),
    start_date=datetime(2026, 11, 18),
    catchup=False,
    tags=["crash test", "Alan"],
) as dag:

    t1 = BashOperator(
        task_id="getFile",
        bash_command="curl -o /tmp/flower.csv https://raw.githubusercontent.com/CourseMaterial/DataWrangling/main/flowerdataset.csv",
    )

    exists = BashOperator(
        task_id="exists_flower",
        depends_on_past=False,
        bash_command="if [ -f /tmp/flower.csv ]; then echo EXISTS; else exit 1; fi",
        retries=3,
        retry_delay=timedelta(seconds=5),
    )
    t1.doc_md = textwrap.dedent(
        """\
    #### Task Documentation
    You can document your task using the attributes `doc_md` (markdown),
    `doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
    rendered in the UI's Task Instance Details page.
    ![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)
    **Image Credit:** Randall Munroe, [XKCD](https://xkcd.com/license.html)
    """
    )

    calculacion = PythonOperator(
        task_id = "calculacion",
        python_callable = calculate_volume,
    )
    
    sleep = BashOperator(
        task_id="sleep",
        depends_on_past=False,
        bash_command="sleep 5",
        retries=1,
    )

    sleep2 = BashOperator(
        task_id="sleep2",
        depends_on_past=False,
        bash_command="sleep 5",
        retries=1,
    )

    display = BashOperator(
        task_id = "display_full_results",
        depends_on_past=False,
        bash_command="cat /tmp/enriched.csv"
    )
    
    travailler = BashOperator(
        task_id="travailler_c_trop_dur",
        bash_command="echo travailler c'est trop dur",
    )

    et_voler_c_est_pas_beau = BashOperator(
        task_id="et_voler_c_est_pas_beau",
        bash_command="echo et_voler_c_est_pas_beau",
    )

    branching = BranchPythonOperator(
        task_id = "branching_task",
        python_callable=decide_branch,
        #provide_context=True, 
        #op_args = [ context['execution_date'].seconds() ]
    )

    branching >> [travailler, et_voler_c_est_pas_beau]
    [t1, sleep, exists] >> calculacion >> sleep2 >> display

    #t1 >> t4
    dag.doc_md = __doc__
