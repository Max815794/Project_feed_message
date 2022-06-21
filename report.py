import telegram
import pandas
from datetime import date, timedelta
import pandahouse
import matplotlib.pyplot as plt
import seaborn as sns
import io
import os

bot = telegram.Bot(token=os.environ.get("REPORT_BOT_TOKEN"))

chat_id=-715060805
#chat_id=155379601
connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': 'dpo_python_2020',
    'user': 'student',
    'database': 'simulator_20220520'
}

#Текстовый отчет

DAU_mes_q = 'SELECT COUNT(DISTINCT user_id) FROM {db}.message_actions where toDate(time) = today()-1 '
DAU_mes_wa_q = 'SELECT COUNT(DISTINCT user_id) FROM {db}.message_actions where toDate(time) = today()-8 '
DAU_feed_q = 'SELECT COUNT(DISTINCT user_id) FROM {db}.feed_actions where toDate(time) = today()-1 '
DAU_feed_wa_q = 'SELECT COUNT(DISTINCT user_id) FROM {db}.feed_actions where toDate(time) = today()-8 '

like_q = "SELECT COUNT(user_id) FROM {db}.feed_actions where toDate(time) = today()-1 AND action='like' "
like_wa_q = "SELECT COUNT(user_id) FROM {db}.feed_actions where toDate(time) = today()-8 AND action='like' "
view_q = "SELECT COUNT(user_id) FROM {db}.feed_actions where toDate(time) = today()-1 AND action='view' "
view_wa_q = "SELECT COUNT(user_id) FROM {db}.feed_actions where toDate(time) = today()-8 AND action='view' "

message_q = "SELECT COUNT(user_id) FROM {db}.message_actions where toDate(time) = today()-1 "
message_wa_q = "SELECT COUNT(user_id) FROM {db}.message_actions where toDate(time) = today()-8 "

df_DAU_mes = pandahouse.read_clickhouse(DAU_mes_q, connection=connection)
df_DAU_mes_wa = pandahouse.read_clickhouse(DAU_mes_wa_q, connection=connection)
df_DAU_feed = pandahouse.read_clickhouse(DAU_feed_q, connection=connection)
df_DAU_feed_wa = pandahouse.read_clickhouse(DAU_feed_wa_q, connection=connection)

df_like = pandahouse.read_clickhouse(like_q, connection=connection)
df_like_wa = pandahouse.read_clickhouse(like_wa_q, connection=connection)
df_view = pandahouse.read_clickhouse(view_q, connection=connection)
df_view_wa = pandahouse.read_clickhouse(view_wa_q, connection=connection)

df_message = pandahouse.read_clickhouse(message_q, connection=connection)
df_message_wa = pandahouse.read_clickhouse(message_wa_q, connection=connection)



df_DAU_mes = df_DAU_mes.rename(columns={"uniqExact(user_id)":"unique_users"})
df_DAU_mes_wa = df_DAU_mes_wa.rename(columns={"uniqExact(user_id)":"unique_users"})
df_DAU_feed = df_DAU_feed.rename(columns={"uniqExact(user_id)":"unique_users"})
df_DAU_feed_wa = df_DAU_feed_wa.rename(columns={"uniqExact(user_id)":"unique_users"})

df_like = df_like.rename(columns=({"count(user_id)":"like"}))
df_like_wa = df_like_wa.rename(columns=({"count(user_id)":"like"}))
df_view = df_view.rename(columns=({"count(user_id)":"view"}))
df_view_wa = df_view_wa.rename(columns=({"count(user_id)":"view"}))

df_message = df_message.rename(columns=({"count(user_id)":"message"}))
df_message_wa = df_message_wa.rename(columns=({"count(user_id)":"message"}))

DAU_mes = df_DAU_mes.unique_users[0]
DAU_mes_wa = df_DAU_mes_wa.unique_users[0]
DAU_feed = df_DAU_feed.unique_users[0]
DAU_feed_wa = df_DAU_feed_wa.unique_users[0]

like = df_like.like[0]
like_wa = df_like_wa.like[0]
view = df_view.view[0]
view_wa = df_view_wa.view[0]

message = df_message.message[0]
message_wa = df_message_wa.message[0]

cr=round(like/view,2)
cr_wa=round(like_wa/view_wa,2)

DAU_mes_text = "DAU мессенджера: " + str(DAU_mes) + " (" + str(DAU_mes_wa) +")\n"
DAU_feed_text = "DAU ленты новостей: " + str(DAU_feed) + " (" + str(DAU_feed_wa) +")\n"
like_text = "Лайки: " + str(like) + " (" + str(like_wa) +")\n"
view_text = "Просмотры: " + str(view) + " (" + str(view_wa) +")\n"
message_text="Сообщения: " + str(message) + " (" + str(message_wa) +")\n"
cr_text="CR: " + str(cr) + " ("+ str(cr_wa) +")\n"

yesterday_date=date.today()- timedelta(days=1)

report_text= "Отчет за " + str(yesterday_date) +": \nВ скобках значения метрик на неделю ранее.\n\n" + DAU_mes_text + DAU_feed_text + like_text + view_text + cr_text + message_text

bot.sendMessage(chat_id=chat_id, text=report_text)



#График DAU

DAU_feed_q = 'SELECT COUNT(DISTINCT user_id),toDate(time)  FROM {db}.feed_actions where toDate(time) BETWEEN today()-7 AND  today()-1 group by toDate(time)'
DAU_mes_q = 'SELECT COUNT(DISTINCT user_id) FROM {db}.message_actions where toDate(time) BETWEEN today()-7 AND  today()-1 group by toDate(time)'

df_DAU_feed = pandahouse.read_clickhouse(DAU_feed_q, connection=connection)
df_DAU_mes = pandahouse.read_clickhouse(DAU_mes_q, connection=connection)

df_DAU_feed = df_DAU_feed.rename(columns={"uniqExact(user_id)":"DAU ленты", "toDate(time)":"date"})
df_DAU_mes = df_DAU_mes.rename(columns={"uniqExact(user_id)":"DAU мессенджера", "toDate(time)":"date"})

df_DAU_feed['DAU мессенджера']=df_DAU_mes['DAU мессенджера']

#df_DAU_feed.plot(x='date', y=['DAU ленты','DAU мессенджера'])

sns.set(rc={'figure.figsize':(16,10)})                                                                                   
plt.tight_layout()
#ax=sns.lineplot(x=df_DAU_feed['date'], y=df_DAU_feed[['DAU ленты','DAU мессенджера']], label='DAU ленты') 
#df_DAU_feed.plot(x='date', y=['DAU ленты','DAU мессенджера'])
ax=sns.lineplot(data=df_DAU_feed)
ax.set(xlabel='time')
ax.set(ylabel='DAU ленты')
            
ax.set_title('DAU ленты')
ax.set(ylim=(0, None))
            
plot_object=io.BytesIO()
ax.figure.savefig(plot_object)
plot_object.seek(0)
plot_object.name="'DAU ленты'.png"
plt.close()
            
#plt.title("DAU ленты и мессенджера за 7 дней")
#plot_object=io.BytesIO()
#plt.savefig(plot_object)
#plot_object.name="DAU за 7 дней"
#plot_object.seek(0)
#plt.close()

bot.sendPhoto(chat_id=chat_id, photo=plot_object)


#График событий

view_q = "SELECT COUNT(user_id),toDate(time)  FROM {db}.feed_actions where toDate(time) BETWEEN today()-7 AND  today()-1 AND action='view' group by toDate(time)"
like_q = "SELECT COUNT(user_id),toDate(time)  FROM {db}.feed_actions where toDate(time) BETWEEN today()-7 AND  today()-1 AND action='like' group by toDate(time)"
mes_q = 'SELECT COUNT(user_id),toDate(time)  FROM {db}.message_actions where toDate(time) BETWEEN today()-7 AND  today()-1 group by toDate(time)'

df_mes_q = pandahouse.read_clickhouse(mes_q, connection=connection)
df_view = pandahouse.read_clickhouse(view_q, connection=connection)
df_likes = pandahouse.read_clickhouse(like_q, connection=connection)

df_view = df_view.rename(columns={"count(user_id)":"Просмотры", "toDate(time)":"date"})
df_likes = df_likes.rename(columns={"count(user_id)":"Лайки", "toDate(time)":"date"})
df_mes_q = df_mes_q.rename(columns={"count(user_id)":"Сообщения", "toDate(time)":"date"})

df_view['Лайки']=df_likes['Лайки']
df_view['Сообщения']=df_mes_q['Сообщения']

df_view.plot(x='date', y=['Просмотры','Лайки', 'Сообщения'])
plt.title("События за 7 дней")
plot_object=io.BytesIO()
plt.savefig(plot_object)
plot_object.name="События за 7 дней"
plot_object.seek(0)
plt.close()

bot.sendPhoto(chat_id=chat_id, photo=plot_object)

#График CR

columns = ['CR','date']
df_cr = pandas.DataFrame(columns=columns)

df_cr.CR=round(df_likes['Лайки']/df_view['Просмотры'],3)

df_cr.date=df_likes.date

df_cr.set_index('date')['CR'].plot()
plt.title("CR за 7 дней")
plot_object=io.BytesIO()
plt.savefig(plot_object)
plot_object.name="CR лайки к просмотрам за 7 дней"
plot_object.seek(0)
plt.close()

bot.sendPhoto(chat_id=chat_id, photo=plot_object)
