# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    daily_predict.py                                   :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Kay Zhou <zhenkun91@outlook.com>           +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2019/05/08 18:48:50 by Kay Zhou          #+#    #+#              #
#    Updated: 2020/02/22 05:51:15 by Kay Zhou         ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

from make_csv_for_web import *
from prediction_from_db import *
from SQLite_handler import *

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

    # Tweets to Database
    demo_tweets_to_db(sess, start, end, clear=True)

    # Save user snapshot
    save_user_snapshot_perday(sess, start)
    sess.close()

    # Predict
    rst1 = calculate_window_share(start, end) # make *.csv
    rst2 = calculate_cumulative_share(start, end) # make *.csv

    demo_predict_to_db(end, rst1, rst2, clear=True)

    # Other stats
    # db_to_users(sess, start, end)
    # db_to_stat_predict(sess, start, end, clear=True)



if __name__ == "__main__":
    daily_election()    
