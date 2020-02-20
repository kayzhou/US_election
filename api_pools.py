# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    api_pools.py                                       :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Kay Zhou <kayzhou.mail@gmail.com>          +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2019/09/12 16:26:19 by Kay Zhou          #+#    #+#              #
#    Updated: 2019/09/12 17:15:07 by Kay Zhou         ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

import tweepy
import json


class Twitter_Apis(object):
    def __init__(self):
        self.Apis = []
        apis_info = json.load(open("api_info.json"))
        for _info in apis_info:
            auth = tweepy.OAuthHandler(_info["consumer_key"], _info["consumer_secret"])
            auth.set_access_token(_info["access_token_key"], _info["access_token_secret"])
            api = tweepy.API(auth)
            self.Apis.append(api)
            
    def need_one(self):
        while True:
            for i in range(len(self.Apis)):
                yield self.Apis[i]