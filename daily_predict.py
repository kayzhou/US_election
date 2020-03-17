# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    daily_predict.py                                   :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Kay Zhou <zhenkun91@outlook.com>           +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2019/05/08 18:48:50 by Kay Zhou          #+#    #+#              #
#    Updated: 2020/03/17 21:14:54 by Kay Zhou         ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

from pathlib import Path

from make_csv_for_web import *
from prediction_from_db import *
from SQLite_handler import *

from analyze_user import write_users_today
from analyze_user_location import write_users_today_csv
from analyze_user_face import write_users_today_face_csv

# crontab -e
# 30 0 * * * cd /home/alex/kayzhou/US_election; nohup /home/alex/anaconda3/bin/python daily_predict.py >> log.txt 2>&1 & 

def remove_yesterday_temp_files(dt):
    dt_str = dt.to_date_string()
    Path("disk/users-profile/{dt_str}.lj").unlink()
    Path("disk/users-location/{dt_str}.csv").unlink()
    Path("disk/users-face/{dt_str}.lj").unlink()


def daily_election():
    sess = get_session()
    end = pendulum.today(tz="UTC") # not include this date
    start = pendulum.yesterday(tz="UTC") # include this date
    print(f"{start} <= run < {end} ")

    # Tweets to Database
    # demo_tweets_to_db(sess, start, end, clear=False)

    # Save user snapshot
    # save_user_snapshot_perday(sess, start)
    # sess.close()

    # New users with location
    write_users_today(end)
    write_users_today_csv(end)
    write_users_today_face_csv(end)
    remove_yesterday_temp_files(start)

    # Predict
    # calculate_window_share(start, end) # make *.csv
    # calculate_cumulative_share(start, end) # make *.csv

    # demo_predict_to_db(end, clear=False)

    # Other stats
    # db_to_users(sess, start, end)
    # db_to_stat_predict(sess, start, end, clear=True)



if __name__ == "__main__":
    daily_election()    
