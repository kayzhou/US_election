# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    collect_user.py                                    :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Zhenkun <zhenkun91@outlook.com>            +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2019/06/07 20:29:42 by Kay Zhou          #+#    #+#              #
#    Updated: 2020/06/05 22:58:34 by Zhenkun          ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

import datetime
import ujson as json
import os
import random
import sys
import time
import traceback
from collections import defaultdict,Counter

import pandas as pd
import pendulum
from dateutil.parser import parse
from tqdm import tqdm
import glob

from read_raw_data import  *
import tweepy
from file_read_backwards import FileReadBackwards

from analyze_user_face import analyze_face

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


def GetThem(user_list, out_name, face_analyze=False):
    Apis = Twitter_Apis().need_one()
    print("GetThem:", len(user_list))

    round_count = 100
    with open(out_name, "a") as out_file:
        users_to_image = []
        for i in range(int(len(user_list) / round_count)):
            print(f"----- {i * round_count} / {len(user_list)} -----")
            api = next(Apis)
            try:
                r = api.lookup_users(user_ids=user_list[i * round_count: (i + 1) * round_count], include_entities=False)
                time.sleep(0.5)
                r = [u._json for u in r]
                users_to_image.extend([{
                    "id": u["id"],
                    "location": u["location"],
                    "profile_image_url": u["profile_image_url"],
                    "screen_name": u["screen_name"]} for u in r]
                )
            except Exception as e:
                # print(type(e))
                print("Exceptions:", e)

            if (i * round_count) % 2000 == 0:
                if face_analyze:
                    analyze_face(users_to_image, out_file)
                else:
                    for _u in users_to_image:
                        out_file.write(json.dumps(_u) + "\n")
                users_to_image = []

        print("The last ...")
        api = next(Apis)
        try:
            r = api.lookup_users(user_ids=user_list[(i + 1) * round_count:], include_entities=False)
            r = [u._json for u in r]
            users_to_image.extend([{
                "id": u["id"],
                "location": u["location"],
                "profile_image_url": u["profile_image_url"],
                "screen_name": u["screen_name"]} for u in r]
            )
        except Exception as e:
            # print(type(e))
            print("Exceptions:", e)

        if face_analyze:
            analyze_face(users_to_image, out_file)
        else:
            for _u in users_to_image:
                out_file.write(json.dumps(_u) + "\n")


def get_user_list():
    # From 01 to 05
    have_face = set(str(json.loads(line.strip())["id"]) for line in open("disk/users-face/2020-04-30_old2.lj"))
    print("We have", len(have_face), "users.")
    user_list = [line.strip().split(",")[0] for line in open("disk/users-location/2020-04-30.csv")]
    user_list = [uid for uid in user_list if uid not in have_face]
    print("Need to run:", len(user_list))
    return user_list


def get_user_list_us2016():
    import numpy as np
    user_list = np.load("data/us2016_uid.npy").astype(int).tolist()
    print("us2016 all users:", len(user_list))
    set_users = set([int(json.loads(line.strip())["id"]) for line in open("data/us2016-users.lj")])
    user_list = [uid for uid in user_list if uid not in set_users]
    print("Need to run:", len(user_list))
    return user_list


def get_user_list_us2016_loc():
    user_list = list(
        set(
            [line.strip().split(",")[0] for line in open("data/us2016-users-location.csv")]
        )
    )
    print("Need to run:", len(user_list))
    return user_list


if __name__ == "__main__":
    def lst_2_mh():
        months = set([
            "202010",
            "202009",
        ])
        file_names = sorted(Path("raw_data").rglob("*.txt"), reverse=True)
        for in_name in file_names[44:]:
            print(in_name)
            if in_name.parts[1] in months:
                print(in_name)
                with FileReadBackwards(in_name) as f:
                    while True:
                        line = f.readline()
                        if not line:
                            print("end!")
                            print("-" * 50)
                            break
                        try:
                            d = json.loads(line.strip())
                        except Exception:
                            print('json.loads Error:')
                            continue
                        yield d
    file_names=sorted(glob.glob('raw_data/raw_data/*lj'),reverse=True)
    user_id=set()
    for i in open('raw_data/user_info/Users_info.lj'):
        user_id.add(json.loads(i)['id'])
    with open('raw_data/user_info/Users_info.lj', "a") as out_file:
        for d in lst_2_mh():
            _id=d['user']['id']
            if 'location' in d['user']:
                lc=d['user']["location"]
            else:
                lc='No_location'
                
            if "profile_image_url" in d['user']:
                img=d['user']["profile_image_url"]
            else:
                img='No_image'
            if _id not in user_id:
                user_id.add(_id)
                users_to_image={
                    "id": d['user']["id"],
                    "location": lc,
                    "profile_image_url": img,
                    "screen_name": d['user']["screen_name"]}
                out_file.write(json.dumps(users_to_image) + "\n")
           
        for _file in file_names:
            print(_file)
            for line in tqdm(open(_file)):
                d=json.loads(line.strip())
                _id=d['user']['id']
                if 'location' in d['user']:
                    lc=d['user']["location"]
                else:
                    lc='No_location'
                if "profile_image_url" in d['user']:
                    img=d['user']["profile_image_url"]
                else:
                    img='No_image'
                if _id not in user_id:
                    user_id.add(_id)
                    users_to_image={
                        "id": d['user']["id"],
                        "location": lc,
                        "profile_image_url": img,
                        "screen_name": d['user']["screen_name"]}
                    out_file.write(json.dumps(users_to_image) + "\n")
                
                
#user_list = get_user_list_us2016_loc()
#user_list_2 = []
#start = 0
#for u in user_list:
#    if u == 536107075:
#        start = 1
#    if start == 1:
#        user_list_2.append(u)
#
#    GetThem(user_list_2, out_name="data/us2016-location-face.lj", face_analyze=True)
# Since the program stops, I restart this again. Should union 2020-04-30.lj with 2020-04-30_old.lj
