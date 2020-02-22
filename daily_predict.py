# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    daily_predict.py                                   :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Kay Zhou <zhenkun91@outlook.com>           +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2019/05/08 18:48:50 by Kay Zhou          #+#    #+#              #
#    Updated: 2020/02/22 08:34:14 by Kay Zhou         ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

from make_csv_for_web import *
from prediction_from_db import *
from SQLite_handler import *

# crontab -e
# 30 0 * * * cd /home/alex/kayzhou/US_election; nohup /home/alex/anaconda3/bin/python daily_predict.py >> log.txt 2>&1 & 


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
    demo_tweets_to_db(sess, start, end, clear=False)

    # Save user snapshot
    save_user_snapshot_perday(sess, start)
    sess.close()

    # Predict
    calculate_window_share(start, end) # make *.csv
    calculate_cumulative_share(start, end) # make *.csv

    demo_predict_to_db(end, clear=False)

    # Other stats
    # db_to_users(sess, start, end)
    # db_to_stat_predict(sess, start, end, clear=True)



if __name__ == "__main__":
    daily_election()    
