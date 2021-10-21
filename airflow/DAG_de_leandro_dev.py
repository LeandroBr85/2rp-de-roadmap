from datetime import timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from custom_operators.tworp_spark_submit_operator import TwoRPSparkSubmitOperator

default_args = {
    'owner': '2rp-leandro',
    'start_date': days_ago(0),
    'depend_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes = 1),
    'run_as_user':'2rp-leandro',
    'proxy_user':'2rp-leandro'
}

with DAG(dag_id='trilha_dag', schedule_interval=None, default_args=default_args, catchup=False) as dag:

    task1 = DummyOperator(
    task_id='task1',
    dag=dag,
    )


    knit = BashOperator(
       task_id='knit',
       bash_command='kinit -kt /home/2rp-leandro/2rp-leandro.keytab 2rp-leandro'
    )



    shell_task = BashOperator(
        task_id='shell_task',
        bash_command='sh /home/2rp-leandro/shell-script/executar.sh /home/2rp-leandro/teste-shell mensagem_shell_task'
    )


    pokemon = TwoRPSparkSubmitOperator(
    task_id='pokemon',
    name='pokemon',
    conn_id="spark_conn",
    application='home/2rp-leandro/pokemons_oldschool.py',
    conf={'spark.yarn.queue':'root.users.2rp-leandro','spark.driver.memory':'20g'},
    principal='2rp-leandro',
    keytab='home/2rp-leandro/2rp-leandro.keytab',
    proxy_user=None,
    verbose=True,
)


task1 >> knit >> shell_task >> pokemon


