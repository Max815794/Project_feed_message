import telegram
import pandas as pd
from datetime import date, timedelta
import pandahouse
import matplotlib.pyplot as plt
import seaborn as sns
import io
import sys
import os
from read_db.CH import Getch

def check_anomaly(df, metric, a=4, n=4):
    
    df['q25'] = df[metric].shift(1).rolling(n).quantile(0.25)
    df['q75'] = df[metric].shift(1).rolling(n).quantile(0.75)
    df['iqr'] = df['q75'] -  df['q25']
    df['up'] =  df['q75'] + a*df['iqr']
    df['low'] = df['q25'] - a*df['iqr']
    
    df['up'] = df['up'].rolling(n*2, center=True, min_periods=1).mean()
    df['low'] = df['low'].rolling(n*2, center=True, min_periods=1).mean()
    
    if df[metric].iloc[-1] < df['low'].iloc[-1] or df[metric].iloc[-1] > df['up'].iloc[-1]:
        is_alert = 1
    else:
         is_alert = 0
            
    return is_alert, df

def check_anomaly_cr(df, metric, a=3, n=7):
    
    df['q25'] = df[metric].shift(1).rolling(n).quantile(0.25)
    df['q75'] = df[metric].shift(1).rolling(n).quantile(0.75)
    df['iqr'] = df['q75'] -  df['q25']
    df['up'] =  df['q75'] + a*df['iqr']
    df['low'] = df['q25'] - a*df['iqr']
    
    df['up'] = df['up'].rolling(n*4, center=True, min_periods=1).mean()
    df['low'] = df['low'].rolling(n*4, center=True, min_periods=1).mean()
    
    if df[metric].iloc[-1] < df['low'].iloc[-1] or df[metric].iloc[-1] > df['up'].iloc[-1]:
        is_alert = 1
    else:
         is_alert = 0
            
    return is_alert, df


def run_alerts(chat=None):
    
    chat_id = chat or -652068442
    bot = telegram.Bot(token=os.environ.get("REPORT_BOT_TOKEN"))
    
    data_feed = Getch(''' SELECT toStartOfFifteenMinutes(time) as ts,
                           toDate(time) as date,
                            formatDateTime(ts, '%R') as hm,
                            uniqExact(user_id) as user_feed,
                            countIf(user_id, action='view') as views,
                            countIf(user_id, action='like') as likes,
                            countIf(user_id, action='like')/countIf(user_id, action='view') as cr
                        FROM simulator_20220520.feed_actions 
                        WHERE time >= today()-1 AND time < toStartOfFifteenMinutes(now())
                        GROUP BY ts, date, hm
                        ORDER BY ts''').df
    
    data_message = Getch(''' SELECT toStartOfFifteenMinutes(time) as ts,
                            toDate(time) as date,
                            formatDateTime(ts, '%R') as hm,
                            uniqExact(user_id) as user_message,
                             COUNT(user_id) as message
                        FROM simulator_20220520.message_actions 
                        WHERE time >= today()-1 AND time < toStartOfFifteenMinutes(now())
                        GROUP BY ts, date, hm
                        ORDER BY ts''').df                   
    
    data_feed[['user_message', 'message']]=data_message[['user_message','message']]
    
    metrics_list=['user_feed', 'views', 'likes','cr', 'user_message','message']
    for metric in metrics_list:
        df = data_feed[['ts', 'date', 'hm', metric]].copy()
        if metric=='cr':
            is_alert, df = check_anomaly_cr(df, metric)
        else:
            is_alert, df = check_anomaly(df, metric)
        
        if is_alert==1:
            msg='''Метрика {metric}:\n текущее значение {current_val:.2f}\nотклонение от предыдущего значения {last_val_diff:.2%}\nhttps://superset.lab.karpov.courses/superset/dashboard/907/\n@max815794'''.format(metric=metric, current_val=df[metric].iloc[-1], last_val_diff=1 - (df[metric].iloc[-1]/df[metric].iloc[-2]))
                                                                                                                                       
            sns.set(rc={'figure.figsize':(16,10)})                                                                                   
            plt.tight_layout()
                                                                                                                                            
            ax=sns.lineplot(x=df['ts'], y=df[metric], label=metric)  
            ax=sns.lineplot(x=df['ts'], y=df['up'], label='up')                                                                                                                   
            ax=sns.lineplot(x=df['ts'], y=df['low'], label='low')    
            
            #for ind, label in enumerate(ax.get_xticklabels()):
            #    if ind % 1 == 0:
            #        label.set_visible(True)
            #    else:
            #        label.set_visible(False)
            
            ax.set(xlabel='time')
            ax.set(ylabel=metric)
            
            ax.set_title(metric)
            ax.set(ylim=(0, None))
            
            plot_object=io.BytesIO()
            ax.figure.savefig(plot_object)
            plot_object.seek(0)
            plot_object.name="{0}.png".format(metric)
            plt.close()
            
            bot.sendMessage(chat_id=chat_id, text=msg)
            
            bot.sendPhoto(chat_id=chat_id, photo=plot_object)
            
    return 

run_alerts()
