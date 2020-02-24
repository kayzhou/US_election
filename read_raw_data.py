# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    read_raw_data.py                                   :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Kay Zhou <zhenkun91@outlook.com>           +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2020/02/11 11:16:25 by Kay Zhou          #+#    #+#              #
#    Updated: 2020/02/24 09:58:17 by Kay Zhou         ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

demo_files = set([
    "Bernie Sanders",
    "SenSanders",
    "Joe Biden",
    "JoeBiden",
    "Mike Bloomberg",
    "MikeBloomberg",
    "Tulsi Gabbard",
    "TulsiGabbard",
    "Amy Klobuchar",
    "amyklobuchar",
    "Tom Steyer",
    "TomSteyer",
    "Elizabeth Warren",
    "ewarren",
    "Pete Buttigieg",
    "PeteButtigieg",
    "Mayor Pete",
    "Pete",
    "Buttigieg",
    # "Michael Bennet",
    # "SenatorBennet",
    # "Andrew Yang",
    # "AndrewYang",
    # "Deval Patrick",
    # "DevalPatrick",
    # "John Delaney",
    # "JohnDelaney",
])

from collections import Counter
from pathlib import Path

import pendulum
import ujson as json
from file_read_backwards import FileReadBackwards
from tqdm import tqdm


def read_tweets_json(start, end):

    months = set([
        "202002",
        "202001",
        # "201912",
        # "201911",
        # "201910",
        # "201909",
    ])

    set_tweets = set()
    file_names = sorted(Path("raw_data").rglob("*.txt"), reverse=True)

    for in_name in file_names:
        if in_name.stem.split("-")[-1] in demo_files and in_name.parts[1] in months:
            print(in_name)
            cnt = 0

            with FileReadBackwards(in_name) as f:
                while True:
                    line = f.readline()
                    if not line:
                        print(cnt, "end!")
                        print("-" * 50)
                        break

                    d = json.loads(line.strip())
                    tweet_id = d["id"]
                    if tweet_id in set_tweets:
                        continue
                    set_tweets.add(tweet_id)

                    dt = pendulum.from_format(
                        d["created_at"], 'ddd MMM DD HH:mm:ss ZZ YYYY')
                    if dt < start:
                        print("sum:", cnt, d["created_at"], "end!")
                        break
                    if dt >= end:
                        continue

                    if cnt % 50000 == 0:
                        print("New data ->", cnt)
                    cnt += 1
                    yield d, dt


def read_user_profile(start, end, set_users_before=None):

    months = set([
        "202002",
        # "202001",
        # "201912",
        # "201911",
        # "201910",
        # "201909",
    ])

    if set_users_before:
        set_users = set_users_before
    else:
        set_users = set()

    file_names = sorted(Path("raw_data").rglob("*.txt"), reverse=True)

    for in_name in file_names:
        if in_name.stem.split("-")[-1] in demo_files and in_name.parts[1] in months:
            print(in_name)
            cnt = 0

            with FileReadBackwards(in_name) as f:
                while True:
                    line = f.readline()
                    if not line:
                        print(cnt, "end!")
                        print("-" * 50)
                        break

                    d = json.loads(line.strip())
                    u = d["user"]
                    user_id = u["id"]
                    if "location" not in u:
                        continue
                    if user_id in set_users:
                        continue
                    set_users.add(user_id)

                    dt = pendulum.from_format(
                        d["created_at"], 'ddd MMM DD HH:mm:ss ZZ YYYY')
                    if dt < start:
                        print("sum:", cnt, d["created_at"], "end!")
                        break
                    if dt >= end:
                        continue

                    if cnt % 1000 == 0:
                        print("New user ->", cnt)
                    cnt += 1
                    yield u
    