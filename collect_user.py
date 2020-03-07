# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    collect_user.py                                    :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Kay Zhou <zhenkun91@outlook.com>           +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2019/06/07 20:29:42 by Kay Zhou          #+#    #+#              #
#    Updated: 2020/03/06 08:01:40 by Kay Zhou         ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

import datetime
import json
import os
import random
import sys
import time
import traceback
from collections import defaultdict

import pandas as pd
import pendulum
import tweepy
from dateutil.parser import parse
from file_read_backwards import FileReadBackwards
from tqdm import tqdm


APIS_INFO = [
    {
        "consumer_key": "BfdhlZ3VWzaJGt4XGf2g42APZ",
        "consumer_secret": "zF79bsglF5R5ASaxD81YXGE5ph8Eg82IezFOavgbnbdXS1kn2f",
        "access_token_key": "2987773038-BWLNt80ix80ktH8OaACaGreyhq8PxZJ490SGAo6",
        "access_token_secret": "OQLZtgFbIUdy1ByGvbiZI52LYZkKwtnty01MKIvKxc59o"
    },
    {
        "consumer_key": "itVIxsEC04CPTfGBEAhlHDygs",
        "consumer_secret": "1rg2Wcu72pGBhHiYqgyfHe93xnN5cu3nNuFgM2l0ZJpJeP1fgN",
        "access_token_key": "2987773038-iUj4EHA7PovkLsf5IJUhi2ymY5IbKzK9Ig406jr",
        "access_token_secret": "mlxUzO1SH1MKl5dEvEEoAaqJejTgP6UK7xZrOKf0eP3gz"
    },
    {
        "consumer_key": "DQIIAOqgVeDANkxhGga5V033N",
        "consumer_secret": "ZWWwx8ax2dFYMnK0DEQkudy8B6ehwAjaqlvk5txhE7WSqtrojo",
        "access_token_key": "2987773038-9Eqt59ZubaWu0pkyVZmmk3ULJZoKOFLf6pw6pf3",
        "access_token_secret": "KkNMhMAbPudOnfaM89xcpo4VItuHKk6HJkewdtjRTiJK5"
    },
    {
        "consumer_key": "nOhDGI80mUN0r73VI3DX3aE3j",
        "consumer_secret": "jIktD1KlkjcKsWqEVCAnAsTwbfjNyx3kp4F9saut4JattIUU6n",
        "access_token_key": "2987773038-iPeb90ocvGBHeIktBoVPDeEpdEWNJyE1jfZB3gm",
        "access_token_secret": "c0MxKwn5tgzvV7SVTgBD5oQSHPzeytKubJr0B74wYW05t"
    },
    {
        "consumer_key": "zHPPCqa5vTXfT7LvYpef5rkBN",
        "consumer_secret": "nEuwMvfINrnDQHYYGFzwzCogZ7zj9W4HuejQvJm49nl0gcTnji",
        "access_token_key": "2987773038-96T2NlnMcWH8PAeZyZTaNd9ijS5gcr6PBHVCZ7Q",
        "access_token_secret": "FjCJ2NonRgxOdQb1S6Tod3Mbd89RdPsUR9XCiGwXRccA3"
    },
    {
        "consumer_key": "pQzKeOFP4OVREbVc0HQsXAhw6",
        "consumer_secret": "vCxAxmQHaSuJ1c8GxZM6GYYMmyEE6ti7IXsAjyXD2FojJXJXpB",
        "access_token_key": "2987773038-ZnKnOH66lMjtAiaaj6KRsGNexG1iPs0PybQ2PcT",
        "access_token_secret": "cu5btDARImF0QQzz67p9sh4fW0Db78oTjU90miP03Un8p"
    },
    {
        "consumer_key": "qCt7oqrgcZyugxdHv04gy99pO",
        "consumer_secret": "KpXx5PnOMjLu3YhBrLiTf266Kkxy9ShLm8K5fL51eyrdEK5g7Y",
        "access_token_key": "2987773038-9NMNzaqXpJQVKnDSR70yoSXMlrz9SfP1pP5eYzq",
        "access_token_secret": "ACE2ybBrR4sKyfkm10I5nGmy66lGHU9yCFzog4e6A5XTK"
    },
    {
        "consumer_key": "zEksZkVoGgiAr6bdf5MhUEnnx",
        "consumer_secret": "ZZTdQPNjKl2pxXEYWiaTlQklklUnBevPb98xL5y4Y1GmOVt6hT",
        "access_token_key": "2987773038-mzdUTi4JBPTnCAghTp832FDP9C8E0lyfhQopnPm",
        "access_token_secret": "vu90sZxE7Cc2rrG8EqgblJUGj6sczDxvQh945BZV1SGtz"
    }
]


class Twitter_Apis(object):
    def __init__(self):
        self.Apis = []
        for _info in APIS_INFO:
            auth = tweepy.OAuthHandler(_info["consumer_key"], _info["consumer_secret"])
            auth.set_access_token(_info["access_token_key"], _info["access_token_secret"])
            api = tweepy.API(auth)
            self.Apis.append(api)
            
    def need_one(self):
        while True:
            for i in range(len(self.Apis)):
                yield self.Apis[i]


def WriteThem(rsts, opened_file):
    for r in rsts:
        opened_file.write(json.dumps(r._json) + "\n")
    
    
def GetThem(user_list, out_file=""):
    
    with open(f'disk/user_profile/{out_file}', 'w') as f:
        Apis = Twitter_Apis().need_one()

        for i in range(int(len(user_list) / 100)):
            try:
                print(i * 100)
                time.sleep(0.1)
                api = next(Apis)
                rsts = api.lookup_users(user_ids=user_list[i * 100: (i + 1) * 100], include_entities=False, tweet_mode="extended")
                WriteThem(rsts, f)
            except Exception as e:
                # print(type(e))
                print("Exceptions:", e)
        
        api = next(Apis)
        rsts = api.lookup_users(user_ids=user_list[i * 100:], include_entities=False, tweet_mode="extended")
        WriteThem(rsts, f)


def read_end_file(start, end):

    set_users = set()
    target_dir = ["201907"]
    cnt = 0

    for _dir in target_dir:
        for in_name in tqdm(os.listdir("data/" + _dir)):
            if in_name.endswith("PRO.txt") or in_name.endswith("Moreno.txt") \
                or in_name.endswith("Sola.txt") or in_name.endswith("PASO.txt"):
                continue
            
            print(in_name, "start ...")
            in_name = "data/" + _dir + "/" + in_name

            with FileReadBackwards(in_name) as f:
                while True:
                    line = f.readline()
                    if not line:
                        print("end of", in_name, cnt)
                        break

                    d = json.loads(line.strip())

                    dt = pendulum.from_format(
                        d["created_at"], 'ddd MMM DD HH:mm:ss ZZ YYYY')
                    if dt < start:
                        print("sum:", cnt, d["created_at"], "end!")
                        break
                    if dt >= end:
                        continue

                    if cnt % 10000 == 0:
                        print("New data:", cnt)
                    cnt += 1

                    if d["user"]["id"] in set_users:
                        continue
                    set_users.add(d["user"]["id"])

                    yield d["user"]


def read_all_files():
    set_users = set()
    target_dir = ["201902", "201903", "201904", "201905", "201906", "201907"]
    cnt = 0

    for _dir in target_dir:
        for in_name in tqdm(os.listdir("data/" + _dir)):
            if in_name.endswith("PRO.txt") or in_name.endswith("Moreno.txt") \
                or in_name.endswith("Sola.txt") or in_name.endswith("PASO.txt"):
                continue
            
            print(in_name, "start ...")
            in_name = "data/" + _dir + "/" + in_name

            for line in open(in_name):

                d = json.loads(line.strip())

                if d["user"]["id"] in set_users:
                    continue
                set_users.add(d["user"]["id"])

                yield d["user"]


def get_user_profile_from_raw_file(out_file="raw"):
    # file_name = f'data/user_profile/0731-week-{out_file}.txt'
    # f = open(file_name, 'w')
    # start = pendulum.datetime(2019, 7, 24, tz="UTC") # include this date
    # end = pendulum.datetime(2019, 7, 31, tz="UTC") # not include this date
    # for u in read_end_file(start, end):
    #     f.write(json.dumps(u, ensure_ascii=False) + "\n")

    file_name = 'data/user_profile/02-07-all.txt'
    with open(file_name, "w") as f:
        for u in read_all_files():
            f.write(json.dumps(u, ensure_ascii=False) + "\n")


def main():
    # get_user_profile_from_raw_file()
    # user_list = []
    # for line in open("../russian_trolls/data/influencers/C1-3.txt"):
    #     line = line.strip()
    #     if line.endswith("Name Not Found"):
    #         uid = line.split()[1]
    #         user_list.append(uid)

    # data = json.load(open("data/users/2019-07-26.txt"))
    # for k, v in data.items():
    #     # if (v["M"] > 30 and v["K"] < 10) or (v["K"] > 30 and v["M"] < 10):
    #     user_list.append(k)
    # GetThem(user_list)

    # have_them = set()
    # with open("data/users_0731.json", "w") as f:
    #     for line in open("data/user_profile/2019-07-25-M&K.txt"):
    #         d = json.loads(line.strip())

    #         if d["screen_name"] in user_list and d["id"] not in have_them:
    #             d["user_URL"] = "https://twitter.com/" + d["screen_name"]
    #             f.write(json.dumps(d, indent=2) + "\n\n")
    #             have_them.add(d["id"])
    
    users = pd.read_pickle("disk/data/df_users_remove_hillary_p=.7.pl").reset_index()
    users_list = users["uid"].to_list()
    GetThem(users_list, "2016election.json")


if __name__ == '__main__':
    main()
