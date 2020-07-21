# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    TwProcess.py                                       :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Zhenkun <zhenkun91@outlook.com>            +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2019/05/23 21:42:53 by Kay Zhou          #+#    #+#              #
#    Updated: 2020/07/17 07:43:59 by Zhenkun          ###   ########.fr        #
#                                                                              #
# **************************************************************************** #


import collections
import re
import unicodedata
from itertools import chain
from string import punctuation

import numpy as np
from nltk import BigramAssocMeasures, ngrams, precision, recall
from nltk.corpus import stopwords
from nltk.probability import ConditionalFreqDist, FreqDist
from nltk.tokenize.casual import (EMOTICON_RE, HANG_RE, WORD_RE,
                                  TweetTokenizer, _replace_html_entities,
                                  reduce_lengthening, remove_handles)


def read_classified_hashtags(now, label2num=None):
    print("func read_classified_hashtags ...")        
    print("label > num:", label2num)
    
    hts = []
    classified_hts = {i: set() for i in range(len(label2num))}

    for line in open(f"data/{now}/hashtags.txt"):
        if not line.startswith("#"):
            _ht, _label = line.strip().split()
            # print("hashtag:", _ht, "label:", _label)
            if _label in label2num:
                hts.append(_ht)
                classified_hts[label2num[_label]].add(_ht)
    # print(hts)
    # print(classified_hts)
    return hts, classified_hts

#==============================================================================
# bag of words
#==============================================================================


def bag_of_words(words):
    return dict([(word, True) for word in words])


def bag_of_words_and_bigrams(words):
    bigrams = ngrams(words, 2)
    return bag_of_words(chain(words, bigrams))


# def bag_of_words_not_in_set(words, badwords):
#     return bag_of_words(set(words) - set(badwords))


# def bag_of_words_in_set(words, goodwords):
#     return bag_of_words(set(words) & set(goodwords))


# def bag_of_non_stopwords(words, stopfile='english'):
#     badwords = stopwords.words(stopfile)
#     return bag_of_words_not_in_set(words, badwords)


#==============================================================================
# Custom Tokenizer for tweets
#==============================================================================

# def normalize_mentions(text):
#     """
#     Replace Twitter username handles with '@USER'.
#     """
#     # ignores = set(["@CFKArgentina", "@mauriciomacri", "@alferdez", "@MiguelPichetto"])
#     ignores = set(["@CFKArgentina", "@mauriciomacri"])
    
#     def _replace(matched):
#         if matched.group(0) in ignores:
#             return "@USER"
#         else:
#             return matched.group(0)

#     # pattern = re.compile(r"(^|(?<=[^\w.-]))@[A-Za-z_]+\w+")
#     # return pattern.sub(_replace, text)

#     pattern = re.compile(r"(^|(?<=[^\w.-]))@[A-Za-z_]+\w+")
#     return pattern.sub('@USER', text)


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
    # pattern = re.compile(
    #     r"""(?i)\b((?:[a-z][\w-]+:(?:/{1,3}|[a-z0-9%])|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:'".,<>?«»“”‘’]))""")
    pattern = re.compile(
        r"""http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\), ]|(?:%[0-9a-fA-F][0-9a-fA-F]))+"""
    )
    
    # first shorten consecutive punctuation to 3
    # to avoid the pattern to hang in exponential loop in extreme cases.
    text = HANG_RE.sub(r'\1\1\1', text)

    return pattern.sub('URL', text)


class CustomTweetTokenizer(TweetTokenizer):
    """ Custom tweet tokenizer based on NLTK TweetTokenizer"""

    def __init__(self, hashtags=None, preserve_case=False, reduce_len=True, strip_handles=False,
                 normalize_usernames=False, normalize_urls=True):

        TweetTokenizer.__init__(self, preserve_case=preserve_case, reduce_len=reduce_len,
                                strip_handles=strip_handles)

        if hashtags:
            self.hashtags_marked = set(
                ["#" + ht for ht in hashtags]
            )
        else:
            self.hashtags_marked = set()

        self.normalize_urls = normalize_urls
        self.normalize_usernames = normalize_usernames

        # if normalize_usernames:
        #    self.strip_handles = False

        if self.preserve_case:
            self.keep_allupper = True


    def _lowerize(self, word):
        if EMOTICON_RE.search(word):
            return word
        elif word == 'URL' or word == "@USER" or word == "#HT":
            return word
        elif word.isupper():
            return word
        else:
            return word.lower()
    
    
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
            
        # elif self.normalize_usernames:
        #     text = normalize_mentions(text)

        # Normalize hashtags, we can't use classified hashtags.
        text = normalize_hashtags(text, self.hashtags_marked)
        
        if self.normalize_urls:
            # Shorten problematic sequences of characters
            text = normalize_urls(text)

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
            words = [self._lowerize(w) for w in words]

        # print(words)
        return words
    
    
if __name__ == "__main__":
    t = CustomTweetTokenizer(hashtags=["big"])
    print(t.tokenize("#big !!!!!!!! You are the best YOUYOU"))