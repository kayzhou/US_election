# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    classifier.py                                      :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Kay Zhou <zhenkun91@outlook.com>           +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2020/02/22 12:48:20 by Kay Zhou          #+#    #+#              #
#    Updated: 2020/03/29 21:15:57 by Kay Zhou         ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

import collections
import unicodedata
from collections import Counter
from itertools import chain
from pathlib import Path
from random import sample
from string import punctuation

import joblib
import pendulum
import ujson as json
from nltk import BigramAssocMeasures, ngrams, precision, recall
from nltk.corpus import stopwords
from nltk.probability import ConditionalFreqDist, FreqDist
from sklearn.feature_extraction import DictVectorizer
from sklearn.feature_selection import SelectFromModel
from sklearn.model_selection import train_test_split

from my_weapon import *
# from myclf import *
from TwProcess import *

# def load_models(dt):
#     print("load models ", dt)
#     label2num = {
#         "BS": 0,
#         "JB": 1
#     }
#     hts, classified_hts = read_classified_hashtags(dt, label2num=label2num)
#     tokenizer = CustomTweetTokenizer(hashtags=hts)
#     v = joblib.load(f'data/{dt}/TfidfVectorizer.joblib')
#     clf = joblib.load(f'data/{dt}/LR.joblib')
#     return classified_hts, tokenizer, v, clf


def load_models_2party(dt):
    print("load models ", dt)
    # 一定要区分好
    label2num = {
        "DP": 0,
        "DT": 1
    }
    hts, classified_hts = read_classified_hashtags(dt, label2num=label2num)
    tokenizer = CustomTweetTokenizer(hashtags=hts)
    v = joblib.load(f'data/{dt}/TfidfVectorizer.joblib')
    clf = joblib.load(f'data/{dt}/LR.joblib')
    return classified_hts, tokenizer, v, clf


class Camp_Classifier(object):

    # know how many categories

    def __init__(self):
        "Init Classifer."

    # def load(self):
    #     self.classified_hts, self.token, self.v, self.clf = load_models("2020-03-25-2") 
        
    def load_2party(self):
        self.classified_hts, self.token, self.v, self.clf = load_models_2party("2020-03-25-2party")
        
    def predict(self, ds):

        def predict_from_hts(_hts):
            if _hts is None:
                return None
            # init
            label_num = None
            label_bingo_times = 0
            set_hts = set([ht["text"].lower() for ht in _hts])
            if not set_hts:
                return None
            for _label_num, _set_hts in self.classified_hts.items():
                for _ht in set_hts:
                    if _ht in _set_hts:
                        label_num = _label_num
                        label_bingo_times += 1
                        break

            if label_num and label_bingo_times == 1:
                # ht_rst = np.zeros(5) # alter
                ht_rst = np.zeros(len(self.classified_hts)) # alter
                ht_rst[label_num] = 1
                return ht_rst
            else:
                return None

        json_rst = {}
        ids = []
        X = []

        for d in ds:
            ht_rst = predict_from_hts(d["hashtags"])

            if ht_rst is not None:
                json_rst[d["id"]] = ht_rst
                continue

            text = d["text"].replace("\n", " ").replace("\t", " ")
            words = self.token.tokenize(text)
            
            ids.append(d["id"])
            X.append(" ".join(words))

        X = self.v.transform(X)
        y = self.clf.predict_proba(X)

        for _id, _y in zip(ids, y):
            json_rst[_id] = _y

        return json_rst
