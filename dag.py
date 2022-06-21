# coding=utf-8

from datetime import datetime, timedelta
import pandas as pd
from io import StringIO
import requests
import pandahouse

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


# Функция для CH
def ch_get_df(query='Select 1', host='https://clickhouse.lab.karpov.courses', user='student', password='dpo_python_2020'):
    r = requests.post(host, data=query.encode("utf-8"), auth=(user, password), verify=False)
    result = pd.read_csv(StringIO(r.text), sep='\t')
    return result


# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'skvortsov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 3, 10),
}

# Интервал запуска DAG
schedule_interval = '0 19 * * *'


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_sim_skv():

    @task()
    def extract_feed():
        query = """SELECT user_id, 
                          today()-1 as event_date,
                          any(gender) as gender,
                          any(age) as age,
                          any(os) as os,
                          countIf(user_id, action='like') as likes, 
                          countIf(user_id, action='view') as views
                   FROM simulator_20220520.feed_actions  
                        WHERE
                        toDate(time) = today()-1 
                        GROUP BY
                        user_id
                        format TSVWithNames"""
        df_cube_feed = ch_get_df(query=query)
        return df_cube_feed
    @task()
    def extract_message():
        query = """Select user as user_id,
                          messages_sent,
                          users_sent,
                          messages_received,
                          users_received,
                          gender,
                          os,
                          age,
                          event_date
                         
                    from
                (SELECT user_id as user, count() as messages_sent, uniqExact(reciever_id) as users_sent, any(gender) as gender, any(os) as os, any(age) as age,  any(toDate(time)) as event_date
                   FROM simulator_20220520.message_actions  
                        where 
                        toDate(time) = today()-1 
                        group by user_id) as t1
                 FULL OUTER JOIN                       

                (SELECT reciever_id as user, count() as messages_received, uniqExact(user_id) as users_received
                   FROM simulator_20220520.message_actions  
                        where 
                        toDate(time) = today()-1 
                        group by user) as t2
                using user
                order by user
                format TSVWithNames"""
        df_cube_message = ch_get_df(query=query)
        return df_cube_message

    @task()
    def transfrom_join(df_cube_feed, df_cube_message):
        df_joined = df_cube_feed.merge(df_cube_message, how='outer', on=['user_id', 'gender', 'os', 'age', 'event_date']).fillna(0).reset_index(drop=True)
        df_joined['gender']= df_joined.gender.astype(int)
        df_joined['age']= df_joined.age.astype(int)
        df_joined['likes']= df_joined.likes.astype(int)
        df_joined['views']= df_joined.views.astype(int)
        df_joined['messages_sent']= df_joined.messages_sent.astype(int)
        df_joined['users_sent']= df_joined.users_sent.astype(int)
        df_joined['messages_received']= df_joined.messages_received.astype(int)
        df_joined['users_received']= df_joined.users_received.astype(int)
        
        
        df_joined  = df_joined [['user_id',
                        'event_date',
                        'gender', 
                        'age',
                        'os',
                        'likes',
                        'views', 
                        'messages_sent',
                        'users_sent',
                        'messages_received',
                        'users_received'
                        ]]

        return df_joined

    @task()
    def transfrom_gender(df_joined):
        df_cube_gender = df_joined[['event_date', 'gender', 'likes',  'views',  'messages_sent', 'users_sent',  'messages_received',  'users_received']]\
             .groupby(['event_date', 'gender'])\
             .sum()\
            .reset_index()
        print(df_cube_gender.to_csv(index=False, sep='\t'))
       # return df_cube_gender
    
    @task()
    def transfrom_os(df_joined):
        df_cube_os = df_joined[['event_date', 'os', 'likes',  'views',  'messages_sent', 'users_sent',  'messages_received',  'users_received']]\
            .groupby(['event_date', 'os'])\
            .sum()\
            .reset_index()
        print(df_cube_os.to_csv(index=False, sep='\t'))
       # return df_cube_os
    
    @task()
    def transfrom_age(df_joined):
        df_cube_age = df_joined[['event_date', 'age', 'likes',  'views',  'messages_sent', 'users_sent',  'messages_received',  'users_received']]\
            .groupby(['event_date', 'age'])\
            .sum()\
            .reset_index()
        print(df_cube_age.to_csv(index=False, sep='\t'))
      #  return df_cube_age
    @task()     
    def load(df_joined):
        connection_new = {
        'host': 'https://clickhouse.lab.karpov.courses',
        'password': '656e2b0c9c',
        'user': 'student-rw',
        'database': 'test'
        }

        create = '''CREATE TABLE IF NOT EXISTS test.skvortsov_8
            (user_id String,
             event_date String,
             gender UInt64,
             age UInt64,
             os String,
             likes UInt64,
             views UInt64,
             messages_sent UInt64,
             users_sent UInt64,
             messages_received UInt64,
             users_received UInt64
            ) ENGINE = Log()'''
        pandahouse.execute(query=create, connection=connection_new)
        pandahouse.to_clickhouse(df=df_joined, table='skvortsov_8', index=False, connection=connection_new)



    df_cube_feed = extract_feed()
    df_cube_message = extract_message()
    df_joined = transfrom_join(df_cube_feed, df_cube_message)
    df_cube_gender=transfrom_gender(df_joined)
    df_cube_os=transfrom_os(df_joined)
    df_cube_age=transfrom_age(df_joined)
    load(df_joined)
    
    

dag_sim_skv = dag_sim_skv()
