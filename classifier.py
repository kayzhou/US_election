# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    classifier.py                                      :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Kay Zhou <zhenkun91@outlook.com>           +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2020/02/22 12:48:20 by Kay Zhou          #+#    #+#              #
#    Updated: 2020/02/22 13:02:33 by Kay Zhou         ###   ########.fr        #
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
from imblearn.over_sampling import RandomOverSampler
from nltk import BigramAssocMeasures, ngrams, precision, recall
from nltk.corpus import stopwords
from nltk.probability import ConditionalFreqDist, FreqDist
from nltk.tokenize.casual import (EMOTICON_RE, HANG_RE, WORD_RE,
                                  TweetTokenizer, _replace_html_entities,
                                  reduce_lengthening, remove_handles)
from sklearn.feature_extraction import DictVectorizer
from sklearn.feature_selection import SelectFromModel
from sklearn.model_selection import train_test_split

from my_weapon import *
from myclf import *
from SQLite_handler import *


def load_models(dt):
    print("load models ", dt)
    hts = [line.strip().split()[1] for line in open(f"data/{dt}/hashtags.txt")]
    tokenizer = CustomTweetTokenizer(hashtags=hts)
    v = joblib.load(f'data/{dt}/DictVectorizer.joblib')
    clf = joblib.load(f'data/{dt}/LR.joblib')

    return tokenizer, v, clf

def load_tfidf_models(dt):
    print("load models ", dt)
    hts = [line.strip().split()[1] for line in open(f"data/{dt}/hashtags.txt")]
    tokenizer = CustomTweetTokenizer(hashtags=hts)
    v = joblib.load(f'data/{dt}/TfidfVectorizer.joblib')
    clf = joblib.load(f'data/{dt}/LR.joblib')

    return tokenizer, v, clf

#==============================================================================
# bag of words
#==============================================================================


def bag_of_words(words):
    return dict([(word, True) for word in words])


def bag_of_words_and_bigrams(words):
    bigrams = ngrams(words, 2)
    return bag_of_words(chain(words, bigrams))


def bag_of_bigrams_and_trigrams(words):
    bigrams = ngrams(words, 2)
    trigrams = ngrams(words, 3)
    return bag_of_words(chain(bigrams, trigrams))


def bag_of_words_not_in_set(words, badwords):
    return bag_of_words(set(words) - set(badwords))


def bag_of_words_in_set(words, goodwords):
    return bag_of_words(set(words) & set(goodwords))


def bag_of_non_stopwords(words, stopfile='spanish'):
    badwords = stopwords.words(stopfile)
    return bag_of_words_not_in_set(words, badwords)


#==============================================================================
# Custom Tokenizer for tweets
#==============================================================================


def normalize_mentions(text):
    """
    Replace Twitter username handles with '@USER'.
    """
    # ignores = set(["@CFKArgentina", "@mauriciomacri", "@alferdez", "@MiguelPichetto"])
    ignores = set(["@CFKArgentina", "@mauriciomacri"])
    
    def _replace(matched):
        if matched.group(0) in ignores:
            return "@USER"
        else:
            return matched.group(0)

    # pattern = re.compile(r"(^|(?<=[^\w.-]))@[A-Za-z_]+\w+")
    # return pattern.sub(_replace, text)

    pattern = re.compile(r"(^|(?<=[^\w.-]))@[A-Za-z_]+\w+")
    return pattern.sub('@USER', text)


def normalize_hashtags(text, ignores):
    """
    Replace Twitter username handles with '#HT'.
    """
    
    def _replace(matched):
        if matched.group(0).lower() in ignores:
            return "#HT"
        else:
            return matched.group(0)

    pattern = re.compile(r"\B#(\w*[A-Za-z_]+\w*)")
    return pattern.sub(_replace, text)


def normalize_urls(text):
    """
    Replace urls with 'URL'.
    """
    pattern = re.compile(
        r"""http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\), ]|(?:%[0-9a-fA-F][0-9a-fA-F]))+"""
    )
    
    # first shorten consecutive punctuation to 3
    # to avoid the pattern to hang in exponential loop in extreme cases.
    text = HANG_RE.sub(r'\1\1\1', text)

    return pattern.sub('URL', text)


def normalize_spanish(text):
    return unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode()


def _lowerize(word, hashtags_marked, keep_all_upper=False):
    if EMOTICON_RE.search(word):
        return word
    elif word == 'URL' or word == "@USER" or word == "#HT":
        return word
    elif word.isupper() and keep_all_upper:  # if all upper, keep it
        return word
    else:
        return word.lower()


class CustomTweetTokenizer(TweetTokenizer):
    """ Custom tweet tokenizer based on NLTK TweetTokenizer"""

    def __init__(self, hashtags, preserve_case=False, reduce_len=True, strip_handles=False,
                    normalize_usernames=False, normalize_urls=True, keep_allupper=True):

        TweetTokenizer.__init__(self, preserve_case=preserve_case, reduce_len=reduce_len,
                                strip_handles=strip_handles)

        self.hashtags_marked = set(
            ["#" + ht for ht in hashtags]
        )

        self.keep_allupper = keep_allupper
        self.normalize_urls = normalize_urls
        self.normalize_usernames = normalize_usernames

        
        # if normalize_usernames:
        #    self.strip_handles = False

        if self.preserve_case:
            self.keep_allupper = True


    def tokenize(self, text):
        """
        :param text: str
        :rtype: list(str)
        :return: a tokenized list of strings;

        Normalizes URLs, usernames and word lengthening depending of the
        attributes of the instance.

        """
        # Fix HTML character entities:
        text = _replace_html_entities(text)

        # Remove or replace username handles
        if self.strip_handles:
            text = remove_handles(text)
        elif self.normalize_usernames:
            text = normalize_mentions(text)

        # Normalize hashtags
        text = normalize_hashtags(text, self.hashtags_marked)
            
        if self.normalize_urls:
            # Shorten problematic sequences of characters
            text = normalize_urls(text)

        # for spanish
        text = normalize_spanish(text)

        # Normalize word lengthening
        if self.reduce_len:
            text = HANG_RE.sub(r'\1\1\1', text)
            text = reduce_lengthening(text)

        # Tokenize:
        safe_text = HANG_RE.sub(r'\1\1\1', text)
        words = WORD_RE.findall(safe_text)

        # Possibly alter the case, but avoid changing emoticons like :D into :d:
        # lower words but keep words that are all upper cases
        if not self.preserve_case:
            words = [_lowerize(w, self.keep_allupper) for w in words]

        # print(words)
        return words


# PB BS EW JB OT=others
def read_classified_hashtags(now):
    labels = "PB BS EW JB OT".split()
    classified_hts = {i: set() for i in range(5)}
    label2num = {labels[i]: i for i in range(5)}

    for line in open(f"data/{now}/hashtags.txt"):
        if not line.startswith("#"):
            w = line.strip().split(" ")
            if len(w) == 3:
                _label, _ht = w[0], w[1]
                classified_hts[label2num[_label]].add(_ht)
    return classified_hts


class Camp_Classifier(object):

    def __init__(self):
        "init Classifer!"

    def load5(self):
        self.classified_hts = read_classified_hashtags("2020-02-22")
        # self.token5, self.v5, self.clf5 = load_models("2020-02-09")
        self.token5, self.v5, self.clf5 = load_tfidf_models("2020-02-22-tfidf")
        # self.token5, self.v5, self.clf5 = load_models("2020-02-22")

    def predict(self, ds):

        def predict_from_hts(_hts):
            if _hts is None:
                return None

            # init
            label = None
            label_bingo_times = 0

            set_hts = set([ht["text"].lower() for ht in _hts])
            if not set_hts:
                return None

            for _ht in set_hts:
                for _label, _set_hts in self.classified_hts.items():
                    if _ht in _set_hts:
                        label = _label
                        label_bingo_times += 1
                        
            # one tweet (in traindata) should have 0 or 1 class hashtag
            if label and label_bingo_times == 1:
                ht_rst = np.zeros(5)
                ht_rst[label] = 1
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
            words = self.token5.tokenize(text)
            X.append(" ".join(words))
            # X.append(bag_of_words_and_bigrams(words))
            ids.append(d["id"])

        X = self.v5.transform(X)
        y = self.clf5.predict_proba(X)

        for _id, _y in zip(ids, y):
            json_rst[_id] = _y

        return json_rst
