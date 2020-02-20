# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    temp.py                                            :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Kay Zhou <kayzhou.mail@gmail.com>          +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2019/06/09 22:51:57 by Kay Zhou          #+#    #+#              #
#    Updated: 2019/08/27 16:34:20 by Kay Zhou         ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

from SQLite_handler import *
from make_csv_for_web import *

sess = get_session()
start = pendulum.datetime(2019, 10, 1, tz="UTC") # include this date
end = pendulum.datetime(2019, 10, 27, tz="UTC") # not include this date
print(f"{start} <= run < {end} ")

# for dt in pendulum.Period(start, end):
#     predict_culmulative_today(sess, dt, clear=True)
tweets_to_db_v2(sess, start, end, clear=True)
# tweets_to_db_v3(sess, start, end, clear=True)

# db_to_users(sess, start, end)
# db_to_users(sess, start, end, bots=True)
# db_to_stat_predict(sess, start, end, clear=True)
# db_to_stat_predict(sess, start, end, bots=True, clear=True)

# _period = pendulum.Period(start.add(days=1), end)
# for dt in _period:
    # print(dt)
    # predict_day(sess, dt, lag=14, clear=True)
    # predict_day(sess, dt, lag=14, bots=True, clear=True)
    # predict3_day(sess, dt, lag=14, clear=True)
    # predict_percent(sess, dt, clear=True)

    # predict_Ndays(sess, dt, win=3, clear=True)
    # predict_Ndays(sess, dt, win=14, clear=True)
    # predict_Ndays(sess, dt, win=30, clear=True)

# dt = "2019-05-14"
# make_main_plot_v2(last=dt, now=dt)
# make_main_plot_Elypsis(last=dt, now=dt)
# make_stat_plot(now=dt)
# make_bot_stat_plot(now=dt)

sess.close()