# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    daily_predict.py                                   :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Zhenkun <zhenkun91@outlook.com>            +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2019/05/08 18:48:50 by Kay Zhou          #+#    #+#              #
#    Updated: 2020/06/05 23:10:29 by Zhenkun          ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

import pendulum
from pathlib import Path

from SQLite_handler import get_session, tweets_to_db
from analyze_user import write_users_today
from analyze_user_location import write_users_today_csv
from analyze_user_face import write_users_today_face_csv
from prediction_from_db import save_user_snapshot, calculate_cumulative_share
from make_csv_for_web import make_main_cumulative_plot


# crontab -e
# 30 0 * * * cd /home/alex/kayzhou/US_election; nohup /home/alex/anaconda3/bin/python daily_predict.py >> log.txt 2>&1 & 

def remove_yesterday_temp_files(dt):
    dt_str = dt.to_date_string()
    Path(f"disk/users-profile/{dt_str}.lj").unlink()
    Path(f"disk/users-location/{dt_str}.csv").unlink()
    Path(f"disk/users-face/{dt_str}.lj").unlink()
    Path(f"disk/users-face/{dt_str}.csv").unlink()


def daily_election(dt=None):
    sess = get_session()
    if dt:
        end = dt
    else:
        end = pendulum.today(tz="UTC")
    start = end.add(days=-1)
    print(f"{start} <= run < {end} ")

    # Tweets to Database
    tweets_to_db(sess, start, end, clear=False)

    # Save user snapshot
    save_user_snapshot(sess, start)
    sess.close()

    # New users with location
    write_users_today(end)
    write_users_today_csv(end)
    write_users_today_face_csv(end)
    remove_yesterday_temp_files(start)

    # Predict
    # calculate_window_share(start, end) # make *.csv
    calculate_cumulative_share(start, end, super_start_month="01", save_csv=False, save_users=True) # make *.csv

    make_main_cumulative_plot()


if __name__ == "__main__":
    daily_election()    
