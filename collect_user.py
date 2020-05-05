# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    collect_user.py                                    :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Kay Zhou <zhenkun91@outlook.com>           +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2019/06/07 20:29:42 by Kay Zhou          #+#    #+#              #
#    Updated: 2020/05/05 16:33:32 by Kay Zhou         ###   ########.fr        #
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
from tqdm import tqdm

import tweepy
from file_read_backwards import FileReadBackwards

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


def GetThem(user_list):
    # 每次获取100个
    Apis = Twitter_Apis().need_one()

    for i in range(int(len(user_list) / 100)):
        try:
            print(i * 100)
            time.sleep(0.1)
            api = next(Apis)
            r = api.lookup_users(user_ids=user_list[i * 100: (i + 1) * 100], include_entities=False, tweet_mode="extended")
            print(r)
        except Exception as e:
            # print(type(e))
            print("Exceptions:", e)
    
    time.sleep(0.1)
    api = next(Apis)
    r = api.lookup_users(user_ids=user_list[i * 100:], include_entities=False, tweet_mode="extended")
    print(r)


def get_user_list():

    have_face = set(str(json.loads(line.strip())["id"]) for line in open("disk/users-face/2020-04-24.lj"))
    user_list = [line.strip().split(",")[0] for line in open("disk/users-location/2020-04-30.csv")]
    user_list = [uid for uid in user_list if uid not in have_face]
    print("Run:", len(user_list))

    GetThem(user_list)
