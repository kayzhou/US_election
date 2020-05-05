# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    collect_user_profile.py                            :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Kay Zhou <zhenkun91@outlook.com>           +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2019/06/07 20:29:42 by Kay Zhou          #+#    #+#              #
#    Updated: 2020/05/05 16:19:23 by Kay Zhou         ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

import datetime
import json
import os
import time
import traceback
from collections import defaultdict

import pendulum
import twitter  # pip install python-twitter
from dateutil.parser import parse
from tqdm import tqdm

"""
>>> api.GetUserTimeline(user)
>>> api.GetUser(user_id=None, screen_name=None, include_entities=True, return_json=False)
>>> api.GetReplies()
>>> api.GetUserTimeline(user)
>>> api.GetHomeTimeline()
>>> api.GetStatus(status_id)
>>> api.GetStatuses(status_ids)
>>> api.DestroyStatus(status_id)
>>> api.GetFriends(user)
>>> api.GetFollowers()
>>> api.GetFeatured()
>>> api.GetDirectMessages()
>>> api.GetSentDirectMessages()
"""

my_apis = [
    twitter.Api(
        consumer_key="BfdhlZ3VWzaJGt4XGf2g42APZ",
        consumer_secret="zF79bsglF5R5ASaxD81YXGE5ph8Eg82IezFOavgbnbdXS1kn2f",
        access_token_key="2987773038-BWLNt80ix80ktH8OaACaGreyhq8PxZJ490SGAo6",
        access_token_secret="OQLZtgFbIUdy1ByGvbiZI52LYZkKwtnty01MKIvKxc59o",
        tweet_mode="extended",
    ),
    twitter.Api(
        consumer_key="itVIxsEC04CPTfGBEAhlHDygs",
        consumer_secret="1rg2Wcu72pGBhHiYqgyfHe93xnN5cu3nNuFgM2l0ZJpJeP1fgN",
        access_token_key="2987773038-iUj4EHA7PovkLsf5IJUhi2ymY5IbKzK9Ig406jr",
        access_token_secret="mlxUzO1SH1MKl5dEvEEoAaqJejTgP6UK7xZrOKf0eP3gz",
        tweet_mode="extended",
    ),
    twitter.Api(
        consumer_key="Gw32gwpksF7mAy4aSxYh1cL5C",
        consumer_secret= "S1JJbN2JyKQewv7SaZktvaeOMdcLfLy1TqDXD2YofOhKrotZIM",
        access_token_key="916015521920950274-83BPzCrqNTMA6NE0dkqk4nV1zLk2VEM",
        access_token_secret="qnXmNnSiPAEHqZNPxJf0RADf64JC3E1gOZz44SaVkNgQR",
        tweet_mode="extended",
    ),
    twitter.Api(
        consumer_key="DQIIAOqgVeDANkxhGga5V033N",
        consumer_secret="ZWWwx8ax2dFYMnK0DEQkudy8B6ehwAjaqlvk5txhE7WSqtrojo",
        access_token_key="2987773038-9Eqt59ZubaWu0pkyVZmmk3ULJZoKOFLf6pw6pf3",
        access_token_secret="KkNMhMAbPudOnfaM89xcpo4VItuHKk6HJkewdtjRTiJK5",
        tweet_mode="extended",
    ),
    twitter.Api(
        consumer_key="nOhDGI80mUN0r73VI3DX3aE3j",
        consumer_secret="jIktD1KlkjcKsWqEVCAnAsTwbfjNyx3kp4F9saut4JattIUU6n",
        access_token_key="2987773038-iPeb90ocvGBHeIktBoVPDeEpdEWNJyE1jfZB3gm",
        access_token_secret="c0MxKwn5tgzvV7SVTgBD5oQSHPzeytKubJr0B74wYW05t",
        tweet_mode="extended",
    ),
    twitter.Api(
        consumer_key="zHPPCqa5vTXfT7LvYpef5rkBN",
        consumer_secret="nEuwMvfINrnDQHYYGFzwzCogZ7zj9W4HuejQvJm49nl0gcTnji",
        access_token_key="2987773038-96T2NlnMcWH8PAeZyZTaNd9ijS5gcr6PBHVCZ7Q",
        access_token_secret="FjCJ2NonRgxOdQb1S6Tod3Mbd89RdPsUR9XCiGwXRccA3",
        tweet_mode="extended",
    ),
]

ITS_TURN = 0


def GetThem(uid_list):
    global ITS_TURN
    rst = []
    for uid in tqdm(uid_list):
        try:
            time.sleep(0.1)
            
            api = my_apis[ITS_TURN]
            ITS_TURN += 1
            if ITS_TURN == len(my_apis):
                ITS_TURN = 0  

            r = api.GetUser(user_id=uid, include_entities=False, return_json=True)
            print(r)
            rst.append(r)
            return r
        except Exception as e:
            print(uid, "Exceptions:", e)