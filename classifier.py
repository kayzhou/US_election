# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    classifier.py                                      :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Kay Zhou <zhenkun91@outlook.com>           +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2020/02/22 12:48:20 by Kay Zhou          #+#    #+#              #
#    Updated: 2020/03/07 03:50:23 by Kay Zhou         ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

import collections
import gc
import os
import re
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
from myclf import *
from SQLite_handler import *
from TwProcess import *


# def load_models(dt):
#     print("load models ", dt)
#     hts = [line.strip().split()[1] for line in open(f"data/{dt}/hashtags.txt")]
#     tokenizer = CustomTweetTokenizer(hashtags=hts)
#     v = joblib.load(f'data/{dt}/DictVectorizer.joblib')
#     clf = joblib.load(f'data/{dt}/LR.joblib')

#     return tokenizer, v, clf


def load_tfidf_models(dt):
    print("load models ", dt)
    hts, classified_hts = read_classified_hashtags(dt)
    tokenizer = CustomTweetTokenizer(hashtags=hts)
    v = joblib.load(f'data/{dt}/TfidfVectorizer.joblib')
    clf = joblib.load(f'data/{dt}/LR.joblib')

    return classified_hts, tokenizer, v, clf


class Camp_Classifier(object):

    # know how many categories

    def __init__(self):
        "Init Classifer."

    def load(self):
        # self.token5, self.v5, self.clf5 = load_models("2020-02-09")
        # self.token5, self.v5, self.clf5 = load_models("2020-02-22")
        self.classified_hts, self.token, self.v, self.clf = load_tfidf_models("2020-02-24-tfidf")
        
        
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
            for _ht in set_hts:
                for _label_num, _set_hts in self.classified_hts.items():
                    if _ht in _set_hts:
                        label_num = _label_num
                        label_bingo_times += 1
            # one tweet (in traindata) should have 0 or 1 class hashtag
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
