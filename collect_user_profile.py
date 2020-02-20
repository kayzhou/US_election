# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    collect_user_profile.py                            :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Kay Zhou <zhenkun91@outlook.com>           +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2019/06/07 20:29:42 by Kay Zhou          #+#    #+#              #
#    Updated: 2019/09/22 15:11:58 by Kay Zhou         ###   ########.fr        #
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
from dateutil.parser import parse
from file_read_backwards import FileReadBackwards
from tqdm import tqdm

from api_pools import Twitter_Apis


def WriteThem(rsts, opened_file):
    for r in rsts:
        opened_file.write(json.dumps(r._json) + "\n")
    
    
def GetThem(user_list, out_file=""):
    f = open(f'disk/user_profile/{out_file}', 'w')
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
