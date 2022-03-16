from utils.df_handle import *
from google_service import get_service
import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


local_tz = pendulum.timezone("Asia/Bangkok")

name='Gdocs'
prefix='SC_'
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

service = get_service()
spreadsheets_id = '13Gg1pzdNlsocvt0-Ws41mVoeUUDEsUBOrt5O8J-7aLM'

dag = DAG(prefix+name,
          catchup=False,
          default_args=dag_params,
          # https://crontab.guru/
          # @once
          schedule_interval= '@once',
          tags=[prefix+name, 'Nhan', 'Daily', '60mins']
)


def python1():
    rangeAll = '{0}!A:AA'.format('Historical Data')
    body = {}
    service.spreadsheets().values().clear( spreadsheetId=spreadsheets_id, range=rangeAll,body=body ).execute()

# transform

def python2():
    df1 = get_ps_df("")
    service.spreadsheets().values().append(
    spreadsheetId=spreadsheets_id,
    valueInputOption='RAW',
    range='Historical Data!A1',
    body=dict(
        majorDimension='ROWS',
        values=df1.T.reset_index().T.values.tolist())
    ).execute()


dummy_start = DummyOperator(task_id="dummy_start", dag=dag)

python1 = PythonOperator(task_id="python1", python_callable=python1, dag=dag)

python2 = PythonOperator(task_id="python2", python_callable=python2, dag=dag)

dummy_start >> python1 >> python2
