import os
from datetime import datetime, timedelta
from pprint import pprint

import psycopg2 as db
import pandas.io.sql as pandas_sql
from psycopg2.extensions import STATUS_BEGIN

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def print_context(ds, **swargs):
    pprint(swargs)
    print(ds)
    return "whatever you return gets printed in the logs"

def query_to_dataframe(conn, query:str):
    df_result = pandas_sql.read_sql(query, conn)
    return df_result

def single_insert(conn, insert_req):
    """ Execute a single INSERT request """
    cursor = conn.cursor()
    try:
        cursor.execute(insert_req)
        conn.commit()
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
        cursor.close()
        return 1
    cursor.close()

def run_transformation():
    query = "SELECT * FROM dsp_sauter.twitchdata"
    try:
        conn_string = "dbname = 'postgres' host = 'host.docker.internal' user = 'postgres' password = 'admin'"
        conn = db.connect(conn_string)
        dataframe = query_to_dataframe(conn, query = query)
        for row in dataframe.iterrows():
            query = f"INSERT into dsp_sauter.twitchdata_copy(channel,watch_time,stream_time,peak_viewrs,average_viewers,followers,partnered,mature,lang) VALUES ('{row['channel']}',{row['watch_time']},{row['stream_time']},{row['peak_viewrs']},{row['average_viewers']},{row['followers']},{row['partnered']},{row['mature']},'{row['lang']}')"
            result = single_insert(conn, query)
            if result == 1:
                print("Erro no insert")
    except Exception as e:
        raise e
    pass

default_args = {
    "owner": "DSP",
    "depends_on_past": False,
    "start_date": datetime(2019,11,26),
    "email": ["sauter_ds@sauter.digital"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

dag = DAG("move_table", default_args = default_args, schedule_interval = "0 22 * * *", catchup = False)

t_set_context = PythonOperator(
    task_id = "t_set_context", 
    provide_context= True, 
    python_callable=print_context, 
    dag = dag)

t_move_table = PythonOperator(
    task_id = "moveTable", 
    python_callable = run_transformation, 
    dag = dag)

t_set_context >> t_move_table