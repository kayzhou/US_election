# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    train.py                                           :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Kay Zhou <zhenkun91@outlook.com>           +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2019/07/06 14:11:24 by Kay Zhou          #+#    #+#              #
#    Updated: 2020/02/11 11:36:43 by Kay Zhou         ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

import gc
import os
import unicodedata
from collections import Counter
from itertools import chain
from pathlib import Path
from random import sample

import joblib
import pendulum
# from imblearn.over_sampling import RandomOverSampler
from nltk import ngrams
from sklearn.feature_extraction import DictVectorizer
from sklearn.feature_selection import SelectFromModel
from sklearn.model_selection import train_test_split

from my_weapon import *
from myclf import *
from SQLite_handler import *
from TwProcess import (CustomTweetTokenizer, bag_of_words,
                       bag_of_words_and_bigrams, get_hts, load_models)


class Classifer(object):
    def __init__(self, now):
        "init Classifer!"
        self.now = now

        # self.remove_hts = set([line.strip() for line in open("data/hashtags/removed_2019-09-05.txt")])
        self.classified_hts, self.hts = get_hts(f"data/{self.now}/hashtags.txt")
        # self.remove_usernames = set([line.strip() for line in open("data/remove_username.txt")])

    def load2(self, name):
        self.token2, self.v2, self.clf2 = load_models(name)

    def load3(self, name):
        self.token3, self.v3, self.clf3 = load_models(name)
        

    def save_tokens(self):
        """
        text > tokens
        """
        label2num = {
            "PB": 0,
            "BS": 1,
            "EW": 2,
            "JB": 3,
            # "OT": 4,
        }

        tokenizer = CustomTweetTokenizer(hashtags=self.hts)
        with open(f"data/{self.now}/tokens.txt", "w") as f:
            print("save tokens from:", f"data/{self.now}/train.txt")
            for line in tqdm(open(f"data/{self.now}/train.txt", encoding="utf8")):
                try:
                    camp, text = line.strip().split("\t")
                    if camp not in label2num:
                        continue
                    camp = label2num[camp]
                    words = tokenizer.tokenize(text)
                    f.write(str(camp) + "\t" + " ".join(words) + "\n")
                except ValueError as e:
                    print(e)


    def load_tokens(self):
        X = []
        y = []
        for line in tqdm(open(f"data/{self.now}/tokens.txt")):
            camp, line = line.strip().split("\t")
            words = line.split()
            # print(words)
            if len(words) > 0:
                X.append(bag_of_words_and_bigrams(words))
                y.append(int(camp))

        print("count of text in 5 camps:", Counter(y).most_common())
        return X, y


    def train(self):
        # read data
        X, y = self.load_tokens()

        print("Reading data finished! count:", len(y))
        # split train and test data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.1, random_state=23)

        print("Splitting data finished!")

        # build one hot embedding
        v = DictVectorizer(dtype=np.int8, sparse=True, sort=False)
        X_train = v.fit_transform(X_train)
        X_test = v.transform(X_test)

        joblib.dump(v, f'data/{self.now}/DictVectorizer.joblib')
        print("Building word embedding finished!")
        print(X_train[0].shape, X_train[1].shape)
        print(X_train.shape, X_test.shape)

        # ros = RandomUnderSampler(random_state=23)
        # X_train, y_train = ros.fit_resample(X_train, y_train)
        # print("After over sampling!")
        # print(X_train.shape, X_test.shape)

        # machine learning model
        list_classifiers = ['LR']
        # list_classifiers = ['SVC']
        # list_classifiers = ['GBDT']

        classifiers = {
            'NB': naive_bayes_classifier,
            'KNN': knn_classifier,
            'LR': logistic_regression_classifier,
            'RF': random_forest_classifier,
            'DT': decision_tree_classifier,
            'SVM': svm_classifier,
            'SVMCV': svm_cross_validation,
            'GBDT': gradient_boosting_classifier,
            'SVC': svm_linear_classifier,
        }

        for classifier in list_classifiers:
            print('******************* {} ********************'.format(classifier))
            if classifier == "LR":
                # clf = LogisticRegression(penalty='l2', multi_class="multinomial", solver="sag", max_iter=10e8)
                clf = LogisticRegression(penalty='l2', solver="sag", max_iter=10e8, multi_class="auto")
                clf.fit(X_train, y_train)
            elif classifier == "GBDT":
                clf = GradientBoostingClassifier(
                    learning_rate=0.1, max_depth=3)
                clf.fit(X_train, y_train)
                
            else:
                clf = classifiers[classifier](X_train, y_train)
            # print("fitting finished! Lets evaluate!")
            self.evaluate(clf, X_train, y_train, X_test, y_test)
            joblib.dump(clf, f'data/{self.now}/{classifier}.joblib')


    def evaluate(self, clf, X_train, y_train, X_test, y_test):
        # CV
        print('accuracy of CV=10:', cross_val_score(
            clf, X_train, y_train, cv=10).mean())

        y_pred = clf.predict(X_test)
        print(classification_report(y_test, y_pred))


    def history_predict(self, out_name):
        """
        0 for Cristina; 
        1 for Macri;
        """
        def _get_tweets():
            set_tweets = set()
            # target_dir = ["201902", "201903", "201904", "201905", "201906", "201907", "201908"]
            if self.now == "201904":
                target_dir = ["201902", "201903", "201904"]
            elif self.now == "201909":
                target_dir = ["201909", "201910"]
            else:
                target_dir = [self.now]
            
            from deal_with_Queries import File_Checker
            checker = File_Checker()
            
            for _dir in target_dir:
                for in_name in tqdm(os.listdir("disk/" + _dir)):
                    # ignore
                    if checker.ignore_it(in_name):
                        continue
                    in_name = "disk/" + _dir + "/" + in_name
                    for line in open(in_name):
                        d = json.loads(line.strip())
                        tweet_id = d["id"]

                        if tweet_id in set_tweets:
                            continue
                        set_tweets.add(tweet_id)
                        yield d
        
        def predict_from_hts(_hts):
            if _hts is None:
                return None

            _hts = list(set([normalize_lower(ht["text"]) for ht in _hts]))
            R_bingo = False
            K_bingo = False
            M_bingo = False
            # A_bingo = False

            for ht in _hts:
                if ht in self.remove_hts:
                    return -1
                if ht in self.K_ht:
                    K_bingo = True
                if ht in self.M_ht:
                    M_bingo = True
                # if ht in A_ht and not A_bingo:
                #     A_bingo = True
            
            if K_bingo and not M_bingo:
                return 0
            elif M_bingo and not K_bingo:
                return 1
            else:
                return None

        now = self.now
        tokenizer = CustomTweetTokenizer(hashtags=self.hts)
        v = joblib.load(f'disk/data/{now}/DictVectorizer.joblib')
        clf = joblib.load(f'disk/data/{now}/LR.joblib')

        pred_ids = []
        pred_users = []
        pred_dt = []

        X = []
        batch_size = 2000

        with open(f"disk/data/" + out_name, "w") as f:
            for d in tqdm(_get_tweets()):

                _sou = get_source_text(d["source"])
                if _sou is not None: # is from bot!
                    continue

                _id = d["id"]
                uid = d["user"]["id"]
                dt = pendulum.from_format(d["created_at"],
                    'ddd MMM DD HH:mm:ss ZZ YYYY').to_date_string()

                # hashtags
                ht_rst = predict_from_hts(d["hashtags"])
                if ht_rst is not None:
                    f.write(f"{_id},{uid},{dt},{ht_rst}\n")
                    continue

                text = d["text"]
                text = text.replace("\n", " ").replace("\t", " ")
                words = tokenizer.tokenize(text)

                # removing pop stars
                removed = False
                for w in words:
                    if w in self.remove_usernames:
                        removed = True
                        break
                if removed:
                    f.write(f"{_id},{uid},{dt},-1\n")
                    continue

                pred_ids.append(_id)
                pred_users.append(uid)
                pred_dt.append(dt)
                
                X.append(bag_of_words_and_bigrams(words))

                if len(X) >= batch_size:
                    # print(X)
                    X = v.transform(X)
                    y = clf.predict_proba(X)
                    for i in range(len(y)):
                        f.write(f"{pred_ids[i]},{pred_users[i]},{pred_dt[i]},{y[i][1]:.3}\n")
                    pred_ids = []
                    pred_users = []
                    pred_dt = []
                    X = []

            else:
                X = v.transform(X)
                y = clf.predict_proba(X)
                for i in range(len(y)):
                    f.write(f"{pred_ids[i]},{pred_users[i]},{pred_dt[i]},{y[i][1]:.3}\n")


    def history_predict_bots(self, out_name):
        """
        0 for Cristina; 
        1 for Macri;
        """
        def _get_tweets():
            set_tweets = set()
            target_dir = ["201902", "201903", "201904", "201905", "201906", "201907", "201908"]
            if self.now == "201904":
                target_dir = ["201902", "201903", "201904"]
            elif self.now == "201909":
                target_dir = ["201909", "201910"]
            else:
                target_dir = [self.now]
            
            from deal_with_Queries import File_Checker
            checker = File_Checker()
            
            for _dir in target_dir:
                for in_name in tqdm(os.listdir("disk/" + _dir)):
                    # ignore
                    if checker.ignore_it(in_name):
                        continue
                    in_name = "disk/" + _dir + "/" + in_name
                    for line in open(in_name):
                        d = json.loads(line.strip())
                        tweet_id = d["id"]

                        if tweet_id in set_tweets:
                            continue
                        set_tweets.add(tweet_id)
                        yield d

        with open(f"disk/data/" + out_name, "w") as f:
            for d in tqdm(_get_tweets()):
                _id = d["id"]
                uid = d["user"]["id"]
                # dt = pendulum.from_format(d["created_at"],
                #     'ddd MMM DD HH:mm:ss ZZ YYYY').to_date_string()

                _sou = get_source_text(d["source"])
                f.write(f"{_id},{uid},{_sou}\n")
    

    def history_predict_3(self):
        """
        0 for Cristina; 
        1 for Macri;
        2 for Massa;
        """

        def _get_tweets():
            tweets_id = set()
            target_dir = ["201904", "201905"]
            for _dir in target_dir:
                for in_name in tqdm(os.listdir("disk/" + _dir)):
                    if in_name.endswith("PRO.txt") or in_name.endswith("Moreno.txt") or in_name.endswith("Sola.txt"):
                        continue
                    in_name = "disk/" +_dir + "/" + in_name
                    # print(in_name)

                    for line in open(in_name):
                        d = json.loads(line.strip())
                        if d["id"] in tweets_id:
                            continue
                        tweets_id.add(d["id"])
                        yield d 
        
        K_ht, M_ht, A_ht, all_hts = get_hts("data/hashtags/2019-05-24.txt")
        
        def predict_from_hts(_hts):
            if _hts is None:
                return None
            _hts = list(set([normalize_lower(ht["text"]) for ht in _hts]))
            R_bingo = False
            K_bingo = False
            M_bingo = False
            A_bingo = False

            for ht in _hts:
                if ht in self.remove_hts:
                    return [-1, -1, -1]
                if ht in K_ht:
                    K_bingo = True
                if ht in M_ht:
                    M_bingo = True
                if ht in A_ht:
                    A_bingo = True
            
            if K_bingo and not M_bingo:
                return [1, 0, 0]
            elif M_bingo and not K_bingo:
                return [0, 1, 0]
            elif M_bingo and not K_bingo:
                return [0, 0, 1]
            else:
                return None

        tokenizer, v, clf = load_models("3-2019-05-27")
        pred_dt = []
        pred_users = []
        X = []
        batch_size = 2000

        with open("disk/data/201905-3-pred.txt", "w") as f:
            for d in tqdm(_get_tweets()):      
                uid = d["user"]["id"]
                dt = pendulum.from_format(d["created_at"], 'ddd MMM DD HH:mm:ss ZZ YYYY').format("YYYYMMDD")

                rst = predict_from_hts(d["hashtags"])
                if rst:
                    f.write(f"{uid},{dt},{rst}\n")
                    continue

                pred_users.append(uid)
                pred_dt.append(dt)

                text = d["text"].replace("\n", " ").replace("\t", " ")
                if len(text) == 140:
                    w = text.split()
                    if w[-2].endswith("…"):
                        text = " ".join(text[:-2])
                text = text[:140]

                words = bag_of_words_and_bigrams(tokenizer.tokenize(text))
                X.append(words)

                if len(X) >= batch_size:
                    # print(X)
                    X = v.transform(X)
                    y = clf.predict_proba(X)
                    for i in range(len(y)):
                        f.write(f"{pred_users[i]},{pred_dt[i]},{y[i]}\n")

                    pred_users = []
                    pred_dt = []
                    X = []

            else:
                X = v.transform(X)
                y = clf.predict_proba(X)
                for i in range(len(y)):
                    f.write(f"{pred_users[i]},{pred_dt[i]},{y[i]}\n")


    def predict_hashtags(self):
        """
        0 for Cristina; 
        1 for Macri;
        2 for Massa;
        """
        def get_hashtags_from_tweet(_hts):
            return list(set([normalize_lower(ht["text"]) for ht in _hts]))

        tokenizer = CustomTweetTokenizer()
        v = joblib.load('model/20190415-DictVectorizer.joblib')
        clf = joblib.load('model/20190415-LR.joblib')
        cnt_M_hashtags = Counter()
        cnt_K_hashtags = Counter()
        X = []
        batch_size = 1000

        tweets_id = set()
        # target_dir = ["201902"]
        target_dir = ["201902", "201903", "201904"]

        for _dir in target_dir:
            for in_name in tqdm(os.listdir("disk/" + _dir)):
                if in_name.endswith("PRO.txt") or in_name.endswith("Moreno.txt") or in_name.endswith("Sola.txt"):
                    continue

                in_name = "disk/" +_dir + "/" + in_name
                # print(in_name)

                for line in open(in_name):
                    d = json.loads(line.strip())
                    if d["hashtags"]:
                        if d["id"] in tweets_id:
                            continue
                        tweets_id.add(d["id"])

                        text = d["text"].replace("\n", " ").replace("\t", " ")

                        words = bag_of_words_and_bigrams(tokenizer.tokenize(text))
                        X.append(words)

                        if len(X) >= batch_size:
                            # print(X)
                            X = v.transform(X)
                            y = clf.predict_proba(X)
                            for i in range(len(y)):
                                if y[i][0] > 0.75:
                                    hts = get_hashtags_from_tweet(d["hashtags"])
                                    for ht in hts:
                                        cnt_M_hashtags[ht] += 1
                                if y[i][1] > 0.75:
                                    hts = get_hashtags_from_tweet(d["hashtags"])
                                    for ht in hts:
                                        cnt_K_hashtags[ht] += 1      

                            X = []
                            # break

        json.dump(cnt_M_hashtags.most_common(100), open("data/top-M-hashtags-20190415.json", "w"))
        json.dump(cnt_K_hashtags.most_common(100), open("data/top-K-hashtags-20190415.json", "w"))

        # get_source_text(d["source"])

    def predict_users(self):
        # list_classifiers = ['LR', 'SVC', 'NB', 'RF', 'DT']
        list_classifiers = ['LR']
        rst = {}
        for clas in list_classifiers:
            rst[clas] = {"K": 0, "M": 0, "U": 0}
        for line in open("disk/data/predict_results.json"):
            d = json.loads(line.strip())
            for clas in list_classifiers:
                if d[clas][0] > d[clas][1]:
                    rst[clas]["K"] += 1
                elif d[clas][0] < d[clas][1]:
                    rst[clas]["M"] += 1
                elif d[clas][0] == d[clas][1]:
                    rst[clas]["U"] += 1
        pd.DataFrame(rst).to_csv("rst.csv")


    def predict_dt_users(self, in_name, out_name, th1, th2):

        def support_Who(r, threshold):
            """
            classify tweets by probalibity
            """
            # print(r)
            if r < 0:
                return 3 # ignore
            elif r >= threshold:
                return 1
            elif r < (1 - threshold):
                return 0
            else:
                return 2

        def support_Who2(r, threshold=1):
            """
            classify users by number of tweets supporting candicates
            """
            if r[3] > 0:
                return "I"
            if r[0] > r[1] and r[0] >= threshold:
                return "K"
            elif r[0] < r[1] and r[1] >= threshold:
                return "M"
            elif r[0] == r[1] and r[0] > 0:
                return "U"
            else:
                return "I"

        # ts = 0.75
        rst = {} # dt - user
        for line in tqdm(open(in_name)):
            uid, dt, proM = line.strip().split(",")
            proM = float(proM)
            
            # next monday 
            dt = pendulum.parse(dt)
            # dt = dt.add(days=(8 - dt.day_of_week))
            dt = dt.format("YYYY-MM-DD")

            # if proM == -1:
            #     remove_dt_users[dt].add(uid)
            #     continue

            if dt not in rst:
                rst[dt] = {}
            if uid not in rst[dt]:
                rst[dt][uid] = [0, 0, 0, 0] # K, M, U, ignore
                
            rst[dt][uid][support_Who(proM, th1)] += 1

        # for ts2 in [1, 2, 3, 5]:
    #     ts2 = 1
        rst_3 = {} # moving average: dt - num of supporters
        for dt in rst:
            if dt not in rst_3:
                rst_3[dt] = {"K": 0, "M": 0, "U": 0, "I": 0}

            p_dt = pendulum.parse(dt)
            start = p_dt.add(days=-th2-1)
            end = p_dt.add(days=-1)
            _period = pendulum.Period(start, end)

            rst_2 = {} # user
            for _dt in _period:
                _dt = _dt.to_date_string()
                if _dt not in rst:
                    continue
                for uid in rst[_dt]:
                    if uid not in rst_2:
                        rst_2[uid] = [0, 0, 0, 0]

                    rst_2[uid][0] += rst[_dt][uid][0] 
                    rst_2[uid][1] += rst[_dt][uid][1] 
                    rst_2[uid][2] += rst[_dt][uid][2]
                    rst_2[uid][3] += rst[_dt][uid][3]

            for uid in rst_2:
                rst_3[dt][support_Who2(rst_2[uid])] += 1

        pd.DataFrame(rst_3).transpose().to_csv(out_name)
    

    def predict_cul_users(self, in_name, out_name, th1):
    
        def support_Who(r, threshold=0.5):
            """
            classify tweets by probalibity
            """
            # print(r)
            if r < 0:
                return 3 # ignore
            elif r >= threshold:
                return 1
            elif r < (1 - threshold):
                return 0
            else:
                return 2

        def support_Who2(r, threshold=1):
            """
            classify users by number of tweets supporting candicates
            """
            if r[3] > 0:
                return "I"
            if r[0] > r[1] and r[0] >= threshold:
                return "K"
            elif r[0] < r[1] and r[1] >= threshold:
                return "M"
            elif r[0] == r[1] and r[0] > 0:
                return "U"
            else:
                return "I"

        # ts = 0.75 
        rst = {} # dt - user
        for line in tqdm(open(in_name)):
            uid, dt, proM = line.strip().split(",")
            proM = float(proM)
            
            dt = pendulum.parse(dt)
            dt = dt.format("YYYY-MM-DD")

            if dt not in rst:
                rst[dt] = {}
            if uid not in rst[dt]:
                rst[dt][uid] = [0, 0, 0, 0] # K, M, U, ignore
                
            rst[dt][uid][support_Who(proM, th1)] += 1

        rst_3 = {} # moving average: dt - num of supporters
        for dt in rst:
            if dt not in rst_3:
                rst_3[dt] = {"K": 0, "M": 0, "U": 0, "I": 0}

            p_dt = pendulum.parse(dt)
            start = pendulum.parse("2019-03-01 00:00:00")
            end = p_dt.add(days=-1)
            _period = pendulum.Period(start, end)

            rst_2 = {} # user
            for _dt in _period:
                _dt = _dt.to_date_string()
                if _dt not in rst:
                    continue
                for uid in rst[_dt]:
                    if uid not in rst_2:
                        rst_2[uid] = [0, 0, 0, 0]

                    rst_2[uid][0] += rst[_dt][uid][0] 
                    rst_2[uid][1] += rst[_dt][uid][1] 
                    rst_2[uid][2] += rst[_dt][uid][2]
                    rst_2[uid][3] += rst[_dt][uid][3]

            for uid in rst_2:
                rst_3[dt][support_Who2(rst_2[uid])] += 1

        pd.DataFrame(rst_3).transpose().to_csv(out_name)

        
    def predict_detailed_users(self, in_name, out_name, th1, th2, _dt):

        def support_Who(r, threshold=0.5):
            """
            classify tweets by probalibity
            """
            # print(r)
            if r < 0:
                return "I" # ignore
            elif r >= threshold:
                return "M"
            elif r < (1 - threshold):
                return "K"
            else:
                return "U"

        check_that_day = pendulum.parse(_dt)

        win = th2

        rst = {} # dt - user
        for line in tqdm(open(in_name)):
            uid, dt, proM = line.strip().split(",")
            proM = float(proM)
            
            dt = pendulum.parse(dt)
            end = check_that_day 
            start = end.add(days=-win)

            if start <= dt < end:             
                if uid not in rst:
                    rst[uid] = {
                        "K": 0,
                        "M": 0,
                        "U": 0,
                        "I": 0,
                    }
                rst[uid][support_Who(proM, th1)] += 1

        json.dump(rst, open(out_name, "w"))


    def predict_dt_users_3(self):

        def support_Who(R, threshold=0.495):
            if R[2] > R[0] and R[2] > R[1]:
                return 2
            elif R[1] > R[0] and R[1] > threshold:
                return 1
            elif R[0] > R[1] and R[0] > threshold:
                return 0
            else:
                return 3

        def support_Who2(r, threshold=1):
            if r[2] > r[1] and r[2] > r[0]:
                return "A"
            elif r[0] > r[1] and r[0] > r[2]:
                return "K"
            elif r[1] > r[0] and r[1] > r[2]:
                return "M"
            elif r[0] == 0 and r[1] == 1 and r[2] == 1:
                return "Ud"
            else:
                return "U"


        import numpy as np
        remove_dt_users = {}

        ts = 0.75
        rst = {}
        for line in tqdm(open("disk/data/201905-3-pred.txt")):
            w = line.strip().split(",")
            # print(len(w), w)
            if len(w) == 3:
                uid, dt, proba = w
                P = [float(p) for p in proba[1:-1].split()]
                proK = P[0]
            if len(w) == 5:
                uid, dt, proK, proM, proA = w
                proK = int(proK[1:])
                proM = int(proM)
                proA = int(proA[:-1])
                P = [proK, proM, proA]

            # next monday 
            dt = pendulum.parse(dt)
            dt = dt.add(days=(8 - dt.day_of_week))
            dt = dt.format("YYYYMMDD")

            if dt not in remove_dt_users:
                remove_dt_users[dt] = set()
            if proK == -1:
                remove_dt_users[dt].add(uid)
            if uid in remove_dt_users[dt]:
                continue
            
            if dt not in rst:
                rst[dt] = {}
            if uid not in rst[dt]:
                rst[dt][uid] = [0, 0, 0, 0]
            rst[dt][uid][support_Who(P, threshold=ts)] += 1

        ts2 = 1
        rst_2 = {}
        for dt in rst:
            if dt not in rst_2:
                rst_2[dt] = {"K": 0, "M": 0, "A": 0, "Ud": 0, "U": 0}
            for uid in rst[dt]:
                if uid in remove_dt_users:
                    rst_2[dt]["U"] += 1 
                else:
                    rst_2[dt][support_Who2(rst[dt][uid], ts2)] += 1

        pd.DataFrame(rst_2).transpose().to_csv(f"data/support-{ts}-{ts2}-3.csv")


    def predict(self, ds):

        def predict_from_hts(_hts):
            if _hts is None:
                return None
            _hts = list(set([normalize_lower(ht["text"]) for ht in _hts]))
            R_bingo = False
            K_bingo = False
            M_bingo = False
            # A_bingo = False

            for ht in _hts:
                if ht in self.remove_hts:
                    return [-1, -1]
                if ht in self.K_ht:
                    K_bingo = True
                if ht in self.M_ht:
                    M_bingo = True
                # if ht in A_ht and not A_bingo:
                #     A_bingo = True
            
            if K_bingo and not M_bingo:
                return [1, 0]
            elif M_bingo and not K_bingo:
                return [0, 1]
            else:
                return None

        json_rst = {}
        ids = []
        X = []
        for d in ds:
            rst = predict_from_hts(d["hashtags"])
            if rst:
                json_rst[d["id"]] = rst
            else:
                text = d["text"]
                # if len(text) == 140:
                #     w = text.split()
                #     if w[-2].endswith("…"):
                #         text = " ".join(text[:-2])
                text = text.replace("\n", " ").replace("\t", " ")
                # text = text[:140]

                words = bag_of_words_and_bigrams(self.token2.tokenize(text))
                X.append(words)
                ids.append(d["id"])

        X = self.v2.transform(X)
        y = self.clf2.predict_proba(X)
        for _id, _y in zip(ids, y):
            json_rst[_id] = [round(_y[0], 3), round(_y[1], 3)]

        return json_rst


    def predict3(self, ds):

        def predict_from_hts(_hts):
            if _hts is None:
                return None
            _hts = list(set([normalize_lower(ht["text"]) for ht in _hts]))
            # R_bingo = False
            K_bingo = False
            M_bingo = False
            A_bingo = False

            for ht in _hts:
                if ht in self.remove_hts:
                    return 0
                if ht in self.K_ht:
                    K_bingo = True
                if ht in self.M_ht:
                    M_bingo = True
                if ht in self.A_ht:
                    A_bingo = True
            
            if A_bingo and not K_bingo and not M_bingo:
                return 1
            else:
                return None

        json_rst = {}
        ids = []
        X = []
        for d in ds:
            rst = predict_from_hts(d["hashtags"])
            if rst:
                json_rst[d["id"]] = rst
            else:
                text = d["text"]
                # if len(text) == 140:
                #     w = text.split()
                #     if w[-2].endswith("…"):
                #         text = " ".join(text[:-2])
                text = text.replace("\n", " ").replace("\t", " ")
                # text = text[:140]

                words = bag_of_words_and_bigrams(self.token3.tokenize(text))
                X.append(words)
                ids.append(d["id"])

        X = self.v3.transform(X)
        y = self.clf3.predict_proba(X)
        for _id, _y in zip(ids, y):
            if _y.argmax() == 2:
                # print(_y)
                json_rst[_id] = _y[2]
            else:
                json_rst[_id] = 0

        return json_rst

    def get_all_classified_tweets(self):
        K_id = set([int(line.strip()) for line in open("data/tweets-proM-0.25.txt")])
        M_id = set([int(line.strip()) for line in open("data/tweets-proM-0.75.txt")])
        all_id = K_id | M_id

        K_bingo_file = open("data/text-proM-0.25.txt", "w")
        M_bingo_file = open("data/text-proM-0.75.txt", "w")
        tweets_json = self.get_train_tweets(all_id)

        tokenizer = CustomTweetTokenizer(hashtags=[])

        for t in tweets_json:
            _id, text, dt = t
            words = tokenizer.tokenize(text)
            bingo = True
            for w in words:
                if w in self.remove_usernames or w in self.remove_hts:
                    bingo = False
                    break
            if _id in K_id:
                K_bingo_file.write(" ".join(words) + "\n")
            elif _id in M_id:
                M_bingo_file.write(" ".join(words) + "\n")

        K_bingo_file.close()
        M_bingo_file.close()


if __name__ == "__main__":
    dt = "2020-02-09-4C"
    Lebron = Classifer(now=dt)
    # After extract_train_data.py
    Lebron.save_tokens()
    Lebron.train()

