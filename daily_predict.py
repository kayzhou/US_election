# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    daily_predict.py                                   :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Kay Zhou <zhenkun91@outlook.com>           +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2019/05/08 18:48:50 by Kay Zhou          #+#    #+#              #
#    Updated: 2019/09/20 16:00:12 by Kay Zhou         ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

from SQLite_handler import *
from make_csv_for_web import *

# crontab -e
# 0 1 * * * cd /home/alex/kayzhou/Argentina_election; nohup /home/alex/anaconda3/bin/python daily_predict.py >> log.txt 2>&1 & 


def daily_election(today=-1):
    sess = get_session()
    if today == -1:
        end = pendulum.today(tz="UTC") # not include this date
        start = pendulum.yesterday(tz="UTC") # include this date
    else:
        end = pendulum.parse(today) # not include this date
        start = end.add(days=-1) # include this date

    print(f"{start} <= run < {end} ")

    # to Tweets
    # tweets_to_db_v2(sess, start, end, clear=True)
    ## tweets_to_db_v3(sess, start, end, clear=True)

    # save user snapshot
    save_today_user_snapshot(sess, start, prob=0.68)
    # save_today_user_snapshot(sess, start, prob=0.75)
    predict_cumulative(end, prob=0.68, clear=True)

    # stat
    db_to_users(sess, start, end)
    db_to_stat_predict(sess, start, end, clear=True)

    # bots
    db_to_users(sess, start, end, bots=True)
    db_to_stat_predict(sess, start, end, bots=True, clear=True)

    # predict -14 days ~ -1 day
    predict_day(sess, end, lag=14, clear=True)
    predict_percent(sess, end, clear=True)
    # predict3_day(sess, end, lag=7)
    predict_day(sess, end, lag=14, bots=True, clear=True)

    # make csv for web
    dt = "2019-06"
    make_main_plot_v3(last=None, now=dt)
    make_main_plot_Elypsis(last=dt, now=dt)
    make_stat_plot(now=dt)
    make_bot_stat_plot(now=dt)
    make_fitting_plot(now=dt)
    make_history_predict(now=dt)

    dt = "2019-05-14"
    make_main_plot_v3(last=dt, now=dt)
    make_main_plot_Elypsis(last=dt, now=dt)
    make_stat_plot(now=dt)
    make_bot_stat_plot(now=dt)
    make_fitting_plot(now=dt)
    make_history_predict(now=dt)
    
    # if end.day_of_week == 1:
    #     tweets_db_to_hashtags75_lastweek(sess, end=end)
    
    sess.close()


if __name__ == "__main__":
    # daily_election()    
    start = pendulum.datetime(2019, 10, 12, tz="UTC") # include this date
    end = pendulum.datetime(2019, 10, 27, tz="UTC") # not include this date
    print(f"{start} <= run < {end} ")
    for dt in pendulum.Period(start, end):
        daily_election(today=dt.format("YYYYMMDD"))
