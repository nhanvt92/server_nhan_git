import psycopg2 
import schedule
from psycopg2 import connect
import io
import httplib2
import os
import pandas as pd
import numpy as np
import unidecode
from datetime import datetime
from google_service import get_service
# now = datetime.now() # current date and time
# TOKEN='2084125145:AAHRq8qiO_Ije0YE_0rJnI3EVltkT_tEusY' #TokenID
# Test='-1001621918495' #GroupID
database = 'biteam'
host = '171.235.26.161'
username = 'biteam'
password = '123biteam'
# date_time = now.strftime("%m-%d-%Y")
# b=''

os.chdir("D://ipynb//forecast_sc")

conn = psycopg2.connect(
    host= host,
    database= database,
    user= username,
    password= password)

service = get_service()


spreadsheets_id = '13Gg1pzdNlsocvt0-Ws41mVoeUUDEsUBOrt5O8J-7aLM'
rangeAll = '{0}!A:AA'.format('Historical Data')
body = {}


def run_sql():
    sql_template=''
    with io.open('doanhsoquakhu.txt', "r", encoding="utf-8") as f:
         sql_template = f.read()#  .replace('\n', '')
    df = pd.read_sql_query(sql_template,conn)
    df1 =pd.DataFrame(df)
    return(df1)

def clear_data():
    resultClear = service.spreadsheets().values().clear( spreadsheetId=spreadsheets_id, range=rangeAll,body=body ).execute()

def insert_data():
    df1 = run_sql()
    response_date = service.spreadsheets().values().append(
        spreadsheetId=spreadsheets_id,
        valueInputOption='RAW',
        range='Historical Data!A1',
        body=dict(
            majorDimension='ROWS',
            values=df1.T.reset_index().T.values.tolist())
    ).execute()


def main():
    clear_data()
    insert_data()

if __name__ == "__main__":
    main()
