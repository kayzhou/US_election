# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    train.py                                           :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Kay Zhou <zhenkun91@outlook.com>           +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2019/07/06 14:11:24 by Kay Zhou          #+#    #+#              #
#    Updated: 2020/03/29 20:51:04 by Kay Zhou         ###   ########.fr        #
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
from imblearn.over_sampling import RandomOverSampler
from imblearn.under_sampling import RandomUnderSampler
from imblearn.over_sampling import SMOTE, ADASYN
from nltk import ngrams
from sklearn.feature_extraction import DictVectorizer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.feature_selection import SelectFromModel
from sklearn.model_selection import train_test_split

from my_weapon import *
from myclf import *
from SQLite_handler import *
from TwProcess import *


class Classifer(object):
    def __init__(self, train_dir):
        "init Classifer!"
        self.train_dir = train_dir

        # 2020-03-06
        # label2num = {
        #    "BS": 0,
        #    "JB": 1,
        #    "OT": 2,
        #}

        # 2020-03-25
        # self.label2num = {
        #     "BS": 0,
        #     "JB": 1,
        #     "DT": 2,
        # }

        # 一定要区分好，0和1分别代表了什么，该模型是判断是否支持trump
        self.label2num = {
            "JB": 0,
            "DT": 1,
        }
        self.hts, _ = read_classified_hashtags(self.train_dir, self.label2num)
        
    def save_tokens(self):
        """
        text > tokens
        """
        tokenizer = CustomTweetTokenizer(hashtags=self.hts)
        with open(f"data/{self.train_dir}/tokens.txt", "w") as f:
            print("save tokens from:", f"data/{self.train_dir}/train.txt")
            for line in tqdm(open(f"data/{self.train_dir}/train.txt", encoding="utf8")):
                try:
                    camp, text = line.strip().split("\t")
                    camp = self.label2num[camp]
                    words = tokenizer.tokenize(text)
                    f.write(str(camp) + "\t" + " ".join(words) + "\n")
                except ValueError as e:
                    print(e)
                

    def load_tokens(self):
        X = []; y = []
        for line in tqdm(open(f"data/{self.train_dir}/tokens.txt")):
            camp, line = line.strip().split("\t")
            # words = line.split()
            # print(words)
            # if len(words) > 0:
                # X.append(bag_of_words_and_bigrams(words))
            if line:
                X.append(line)
                y.append(int(camp))

        print("count of text in camps:", Counter(y).most_common())
        return X, y


    def train(self):
        # read data
        X, y = self.load_tokens()

        print("Reading data finished! count:", len(y))
        
        # split train and test data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.1, random_state=32)

        print("Splitting data finished!")

        # build one hot embedding
        # v = DictVectorizer(dtype=np.int8, sparse=True, sort=False)
        v = TfidfVectorizer(ngram_range=(1, 2), max_features=1000000)
        X_train = v.fit_transform(X_train)
        X_test = v.transform(X_test)

        # joblib.dump(v, f'data/{self.train_dir}/DictVectorizer.joblib')
        joblib.dump(v, f'data/{self.train_dir}/TfidfVectorizer.joblib')
        print("Building word embedding finished!")
        print(X_train[0].shape, X_train[1].shape)
        print(X_train.shape, X_test.shape)

        # X_train, y_train = SMOTE().fit_sample(X_train, y_train)
        # X_train, y_train = ADASYN().fit_sample(X_train, y_train)
        # X_train, y_train = RandomOverSampler(random_state=24).fit_sample(X_train, y_train)
        X_train, y_train = RandomUnderSampler(random_state=42).fit_sample(X_train, y_train)

        print("After sampling!")
        print(X_train.shape, X_test.shape)

        # machine learning model
        list_classifiers = ['LR']
        # list_classifiers = ['SVC']
        # list_classifiers = ['GBDT']

        classifiers = {
#             'NB': naive_bayes_classifier,
#             'KNN': knn_classifier,
            'LR': logistic_regression_classifier,
#             'RF': random_forest_classifier,
#             'DT': decision_tree_classifier,
#             'SVM': svm_classifier,
#             'SVMCV': svm_cross_validation,
#             'GBDT': gradient_boosting_classifier,
#             'SVC': svm_linear_classifier,
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
            joblib.dump(clf, f'data/{self.train_dir}/{classifier}.joblib')


    def evaluate(self, clf, X_train, y_train, X_test, y_test):
        # CV
        print('accuracy of CV=10:', cross_val_score(
            clf, X_train, y_train, cv=10).mean())

        y_pred = clf.predict(X_test)
        print(classification_report(y_test, y_pred))
    

if __name__ == "__main__":
    #dt = "2020-03-06-tfidf"
    #dt ="2020-03-08-tfidf_model3"
    #dt = "model 4_omg"

    _dir = "train-07"
    Lebron = Classifer(train_dir=_dir)
    # After extract_train_data.py
    Lebron.save_tokens()
    Lebron.train()
