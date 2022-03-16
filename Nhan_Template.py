from utils.df_handle import *
import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


local_tz = pendulum.timezone("Asia/Bangkok")

name='bar_nhan_updated'
prefix='foo'
csv_path = '/usr/local/airflow/plugins'+'/'

dag_params = {
    'owner': 'nhanvo',
    "depends_on_past": False,
    'start_date': datetime(2021, 10, 1, tzinfo=local_tz),
    'email_on_failure': True,
    'email_on_retry': False,
    'email':['duyvq@merapgroup.com', 'vanquangduy10@gmail.com'],
    'do_xcom_push': False,
    'execution_timeout':timedelta(seconds=300)
    # 'retries': 3,
    # 'retry_delay': timedelta(minutes=10),
}


dag = DAG(prefix+name,
          catchup=False,
          default_args=dag_params,
          # https://crontab.guru/
          schedule_interval= '0 10,17 * * *',
          tags=[prefix+name, 'Nhan', 'Daily', '60mins']
)


def python1():
    pass

# transform

def python2():
    pass

def truncate():
    pass


def insert():
    pass



dummy_start = DummyOperator(task_id="dummy_start", dag=dag)

python1 = PythonOperator(task_id="update_table", python_callable=python1, dag=dag)

python2 = PythonOperator(task_id="etl_to_postgres", python_callable=python2, dag=dag)

truncate = PythonOperator(task_id="truncate", python_callable=truncate, dag=dag, execution_timeout=timedelta(seconds=30))

insert = PythonOperator(task_id="insert", python_callable=insert, dag=dag)

dummy_start >> python1 >> python2 >> truncate >> insert
# >> tab_refresh
