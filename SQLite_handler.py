# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    SQLite_handler.py                                  :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Zhenkun <zhenkun91@outlook.com>            +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2019/06/07 20:40:05 by Kay Zhou          #+#    #+#              #
#    Updated: 2020/07/17 07:54:32 by Zhenkun          ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

import os
import unicodedata
from pprint import pprint

import joblib
import pendulum
from bs4 import BeautifulSoup
from file_read_backwards import FileReadBackwards
from sqlalchemy import (Column, DateTime, Float, Integer, String, Text, and_,
                        create_engine, desc, exists, or_, text)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound
from tqdm import tqdm

from my_weapon import *

Base = declarative_base()

official_twitter_clients = set([
    'Twitter for iPhone',
    'Twitter for Android',
    'Twitter Web Client',
    'Twitter Web App',
    'Twitter for iPad',
    'Mobile Web (M5)',
    'TweetDeck',
    'Mobile Web',
    'Mobile Web (M2)',
    'Twitter for Windows',
    'Twitter for Windows Phone',
    'Twitter for BlackBerry',
    'Twitter for Android Tablets',
    'Twitter for Mac',
    'Twitter for BlackBerry®',
    'Twitter Dashboard for iPhone',
    'Twitter for iPhone',
    'Twitter Ads',
    'Twitter for  Android',
    'Twitter for Apple Watch',
    'Twitter Business Experience',
    'Twitter for Google TV',
    'Chirp (Twitter Chrome extension)',
    'Twitter for Samsung Tablets',
    'Twitter for MediaTek Phones',
    'Google',
    'Facebook',
    'Twitter for Mac',
    'iOS',
    'Instagram',
    'Vine - Make a Scene',
    'Tumblr',
])

# class Demo_Tweet(Base):
#     __tablename__ = "demo_tweets"
#     tweet_id = Column(Integer, primary_key=True)
#     user_id = Column(Integer)
#     dt = Column(DateTime)
#     camp = Column(Integer)
#     max_proba = Column(Float)
#     probas = Column(String)
#     hashtags = Column(String)
#     source = Column(String)


#class Demo_Tweet(Base):
#    __tablename__ = "demo_tweets_v2"
#    tweet_id = Column(Integer, primary_key=True)
#    user_id = Column(Integer)
#    dt = Column(DateTime)
#    camp = Column(Integer)
#    max_proba = Column(Float)
#     probas = Column(String)
#     hashtags = Column(String)
#    source = Column(String)

## Matteo Changed it
class Demo_Tweet(Base):
    __tablename__ = "demo_tweets_v2"
    tweet_id = Column(Integer, primary_key=True)
    user_id = Column(Integer)
    dt = Column(DateTime)
    camp = Column(Integer)
    max_proba = Column(String)
#     probas = Column(String)
#     hashtags = Column(String)
    source = Column(String)


class Tweet(Base):
    __tablename__ = "tweets"
    tweet_id = Column(Integer, primary_key=True)
    user_id = Column(Integer)
    dt = Column(DateTime)
    camp = Column(Integer)
    max_proba = Column(String)
    source = Column(String)


# class Retweet(Base):
#     __tablename__ = "retweets"
#     tweet_id = Column(Integer, primary_key=True)
#     dt = Column(DateTime)
#     user_id = Column(Integer)
#     ori_tweet_id = Column(Integer)
#     ori_user_id = Column(Integer)


# class Source(Base):
#     __tablename__ = "sources"
#     tweet_id = Column(Integer, primary_key=True)
#     user_id = Column(Integer)
#     source = Column(String(50))


# class Term(Base):
#     __tablename__ = "terms"
#     name = Column(String(100), primary_key=True)
#     proK = Column(Integer)
#     proM = Column(Integer)
#     unclassified = Column(Integer)


# class Month_Term(Base):
#     __tablename__ = "month_terms"
#     name = Column(String(100), primary_key=True)
#     proK = Column(Integer)
#     proM = Column(Integer)
#     unclassified = Column(Integer)


# class New_clas(Base):
#     __tablename__ = "New_clas2_20190415"
#     tweet_id = Column(Integer, primary_key=True)
#     user_id = Column(Integer)
#     dt = Column(DateTime)
#     proK = Column(Float)
#     proM = Column(Float)
#     # proK3 = Column(Float)
#     # proM3 = Column(Float)
#     # proA3 = Column(Float)
#     # hashtags = Column(String)
#     # source = Column(String(50))


# class User(Base):
#     __tablename__ = "users"
#     user_id = Column(Integer, primary_key=True)
#     tweet_id = Column(Integer)
#     first_dt = Column(DateTime)
#     first_camp = Column(String)


# class User_location(Base):
#     __tablename__ = "users_location"
#     user_id = Column(Integer, primary_key=True)
#     location = Column(String)
#     parsed_location = Column(String)
#     country = Column(String)


# class User_Profile(Base):
#     __tablename__ = "user_profile"
#     user_id = Column(Integer, primary_key=True)
#     location = Column(String)
#     parsed_location = Column(String)
#     country = Column(String)
#     age = Column(Integer)
#     gender = Column(String)


# class Bot_User(Base):
#     __tablename__ = "bot_users"
#     # tweet_id = Column(Integer, primary_key=True)
#     user_id = Column(Integer, primary_key=True)
#     tweet_id = Column(Integer)
#     first_dt = Column(DateTime)
#     first_camp = Column(String)


# class Hashtag(Base):
#     __tablename__ = "hashtags"
#     hashtag = Column(Text, primary_key=True)
#     update_dt = Column(DateTime)
#     count = Column(Integer)
#     M_count = Column(Integer)
#     K_count = Column(Integer)


# class Camp_Hashtag(Base):
#     __tablename__ = "camp_hashtags"
#     hashtag = Column(Text, primary_key=True)
#     update_dt = Column(DateTime)
#     camp = Column(String(10))


# class Stat(Base):
#     __tablename__ = "stat"
#     dt = Column(DateTime, primary_key=True)
#     tweet_count = Column(Integer)
#     user_count = Column(Integer)
#     tweet_cum_count = Column(Integer)
#     cla_tweet_cum_count = Column(Integer)
#     user_cum_count = Column(Integer)
#     cla_user_cum_count = Column(Integer)
#     K_tweet_count = Column(Integer)
#     M_tweet_count = Column(Integer)
#     U_tweet_count = Column(Integer)
#     K_user_count = Column(Integer)
#     M_user_count = Column(Integer)
#     U_user_count = Column(Integer)
#     I_user_count = Column(Integer)


# class Bot_Stat(Base):
#     __tablename__ = "bot_stat"
#     dt = Column(DateTime, primary_key=True)
#     tweet_count = Column(Integer)
#     user_count = Column(Integer)
#     tweet_cum_count = Column(Integer)
#     cla_tweet_cum_count = Column(Integer)
#     user_cum_count = Column(Integer)
#     cla_user_cum_count = Column(Integer)
#     K_tweet_count = Column(Integer)
#     M_tweet_count = Column(Integer)
#     U_tweet_count = Column(Integer)
#     K_user_count = Column(Integer)
#     M_user_count = Column(Integer)
#     U_user_count = Column(Integer)
#     I_user_count = Column(Integer)


# class Daily_Predict(Base):
#     __tablename__ = "daily_predict"
#     dt = Column(DateTime, primary_key=True)
#     U_Cristina = Column(Integer)
#     U_Macri = Column(Integer)
#     U_unclassified = Column(Integer)
#     U_irrelevant = Column(Integer)

# Democrats
# class Cumulative_Predict_v1(Base):
#     __tablename__ = "cumualtive_predict_v1"
#     _id = Column(String, primary_key=True) # date_string + "-" + state
#     dt = Column(DateTime, primary_key=True)
#     state = Column(String) # USA
#     c0 = Column(Integer)
#     c1 = Column(Integer)
#     c2 = Column(Integer)
#     c3 = Column(Integer)


# Biden and Trump
class Cumulative_Predict_v2(Base):
    __tablename__ = "cumualtive_predict"
    _id = Column(String, primary_key=True) # date_string + "-" + state
    dt = Column(DateTime)
    state = Column(String) # state name or null (USA)
    Biden = Column(Integer)
    Trump = Column(Integer)
    Undec = Column(Integer)


# class Weekly_Predict_v1(Base):
#     __tablename__ = "weekly_predict_v1"
#     dt = Column(DateTime, primary_key=True)
#     c0 = Column(Integer)
#     c1 = Column(Integer)
#     c2 = Column(Integer)
#     c3 = Column(Integer)
#     c4 = Column(Integer)


# ------------- class definition ove -------------


def normalize_lower(text):
    # return unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode().lower())
    return text.lower()


def get_hashtags_from_tweet(_hashtags):
    if _hashtags:
        return ",".join(list(set([normalize_lower(tag["text"]) for tag in _hashtags])))
    else:
        return None


def get_source_text(_source):
    _sou = BeautifulSoup(_source, features="lxml").get_text()
    if _sou in official_twitter_clients:
        return None
    else:
        return _sou


def get_source_cnt(sess):
    from collections import Counter
    source_cnt = Counter()
    tweets = sess.query(Tweet.source).yield_per(1000)
    for t in tqdm(tweets):
        source_cnt[t[0]] += 1
    json.dump(source_cnt.most_common(), open(
        "data/source_cnt.json", "w"), indent=2)


def get_last_week():
    now = pendulum.now()
    dt = pendulum.DateTime(now.year, now.month, now.day)
    end = dt.add(days=-(dt.weekday-1))
    start = end.add(days=-7)
    # start <= dt < end
    return start, end


def get_all_tweets_id(sess):
    print("Get all tweets id ...")
    sess = get_session()
    tids_set = {t[0] for t in sess.query(Tweet.tweet_id).yield_per(5000)}
    print('have:', len(tids_set))
    sess.close()
    return tids_set


def count_file_hashtag(in_file):
    from collections import Counter
    hashtags = Counter()
    for line in tqdm(open(in_file)):
        d = json.loads(line.strip())
        if d["hashtags"]:
            hts = get_hashtags_from_tweet(d["hashtags"]).split(",")
            for ht in hts:
                hashtags[ht] += 1

    with open("data/count_hashtags.txt", "w") as f:
        for ht in hashtags.most_common():
            f.write(f"{ht[0]},{ht[1]}\n")


def count_paso_camp():
    from TwProcess import load_models, bag_of_words_and_bigrams
    import pandas as pd

    tokenizer, v2, clf2 = load_models()

    X = []
    rst = []
    for line in tqdm(open("disk/201905/201905-PASO.txt")):
        d = json.loads(line.strip())
        words = bag_of_words_and_bigrams(tokenizer.tokenize(d["text"]))
        X.append(words)
        if len(X) == 5000:
            y = clf2.predict_proba(v2.transform(X))
            for i in range(len(y)):
                proM = round(y[i][1], 4)
                rst.append(proM)

            X = []

    pd.Series(rst).to_csv("disk/data/PASO_proba.csv")


def tweets_to_retweets(sess, start, end, clear=False):
    """
    导入转发推特
    """
    if clear:
        print("deleting >=", start, "<", end)
        sess.query(Retweet).filter(
            Retweet.dt >= start, Retweet.dt < end).delete()
        sess.commit()

    tweets_data = []
    for d, dt in read_end_file_for_retweets(start, end):
        if "in_reply_to_status_id" in d:
            continue
        elif "quoted_status_id" in d:
            continue
        elif "retweeted_status" in d:
            tid = d["id"]
            uid = d["user"]["id"]
            o_tid = d["retweeted_status"]["id"]
            o_uid = d["retweeted_status"]["user"]["id"]

            if sess.query(exists().where(Tweet.tweet_id == tid)).scalar():
                tweets_data.append(
                    Retweet(
                        tweet_id=tid,
                        dt=dt,
                        user_id=uid,
                        ori_tweet_id=o_tid,
                        ori_user_id=o_uid
                    )
                )

        if len(tweets_data) == 5000:
            sess.add_all(tweets_data)
            sess.commit()
            tweets_data = []

    if tweets_data:
        sess.add_all(tweets_data)
        sess.commit()

# *** very important *** #
def demo_tweets_to_db(sess, start, end, clear=False):
    """
    import tweets to database with prediction
    """
    if clear:
        print("deleting >=", start, "<", end)
        sess.query(Demo_Tweet).filter(
            Demo_Tweet.dt >= start, Demo_Tweet.dt < end).delete()
        sess.commit()
    
    from classifier import Camp_Classifier
    Lebron = Camp_Classifier()
    Lebron.load()

    X = []
    tweets_data = []

    from read_raw_data import read_tweets_json

    for d, dt in read_tweets_json(start, end):
        tweet_id = d["id"]
        uid = d["user"]["id"]
        _sou = get_source_text(d["source"])
        # hts = get_hashtags_from_tweet(d["hashtags"])

        tweets_data.append(
            Demo_Tweet(tweet_id=tweet_id, user_id=uid,
                       dt=dt, source=_sou)
        )

        X.append(d)
        
        if len(tweets_data) == 2000:
            json_rst = Lebron.predict(X)
            for i in range(len(tweets_data)):
                rst = json_rst[tweets_data[i].tweet_id]
                # print(rst)
                # probas = " ".join([str(round(r, 3)) for r in rst])
                # tweets_data[i].probas = probas
                tweets_data[i].max_proba = round(rst.max(), 3)
                tweets_data[i].camp = int(rst.argmax())

            sess.add_all(tweets_data)
            sess.commit()
            X = []
            tweets_data = []

    if tweets_data:
        json_rst = Lebron.predict(X)
        for i in range(len(tweets_data)):
            rst = json_rst[tweets_data[i].tweet_id]
            # probas = " ".join([str(round(r, 3)) for r in rst])
            # tweets_data[i].probas = probas
            tweets_data[i].max_proba = round(rst.max(), 3)
            tweets_data[i].camp = int(rst.argmax())

        sess.add_all(tweets_data)
        sess.commit()


def tweets_to_db(sess, start, end, clear=False):
    """
    import tweets to database with prediction
    """
    if clear:
        print("deleting >=", start, "<", end)
        sess.query(Tweet).filter(Tweet.dt >= start, Tweet.dt < end).delete()
        sess.commit()
    
    from classifier import Camp_Classifier
    Lebron = Camp_Classifier()
    Lebron.load()

    X = []
    tweets_data = []

    from read_raw_data import read_historical_tweets as read_tweets

    for d, dt in read_tweets(start, end):
        # print(d)
        tweet_id = d["id"]
        uid = d["user"]["id"]
        if 'source' in d:
            _sou = get_source_text(d["source"])
        else:
            _sou = "No source"
        # hts = get_hashtags_from_tweet(d["hashtags"])

        tweets_data.append(
            Tweet(tweet_id=tweet_id,
                  user_id=uid,
                  dt=dt,
                  source=_sou)
        )
        X.append(d)
        
        if len(tweets_data) == 2000:
            json_rst = Lebron.predict(X)
            for i in range(len(tweets_data)):
                rst = json_rst[tweets_data[i].tweet_id]
                tweets_data[i].max_proba = round(rst.max(), 3)
                tweets_data[i].camp = int(rst.argmax()) # 0 for Biden, 1 for Trump

            sess.add_all(tweets_data)
            sess.commit()
            X = []
            tweets_data = []

    if tweets_data:
        json_rst = Lebron.predict(X)
        for i in range(len(tweets_data)):
            rst = json_rst[tweets_data[i].tweet_id]
            tweets_data[i].max_proba = round(rst.max(), 3)
            tweets_data[i].camp = int(rst.argmax()) # 0 for Biden, 1 for Trump

        sess.add_all(tweets_data)
        sess.commit()


def tweets_to_db_fast(sess):
    """
    import tweets to database with prediction
    """
    from classifier import Camp_Classifier
    Lebron = Camp_Classifier()
    Lebron.load()

    X = []
    tweets_data = []

    # from read_raw_data import read_historical_tweets as read_tweets
    from read_raw_data import read_raw_tweets_fromlj as read_tweets

    for d, dt in read_tweets():
        # print(d)
        tweet_id = d["id"]
        uid = d["user"]["id"]
        if 'source' in d:
            _sou = get_source_text(d["source"])
        else:
            _sou = "No source"
        # hts = get_hashtags_from_tweet(d["hashtags"])

        tweets_data.append(
            Tweet(tweet_id=tweet_id,
                  user_id=uid,
                  dt=dt,
                  source=_sou)
        )
        X.append(d)
        
        if len(tweets_data) == 5000:
            json_rst = Lebron.predict(X)
            for i in range(len(tweets_data)):
                rst = json_rst[tweets_data[i].tweet_id]
                tweets_data[i].max_proba = round(rst.max(), 3)
                tweets_data[i].camp = int(rst.argmax()) # 0 for Biden, 1 for Trump

            sess.add_all(tweets_data)
            sess.commit()
            X = []
            tweets_data = []

    if tweets_data:
        json_rst = Lebron.predict(X)
        for i in range(len(tweets_data)):
            rst = json_rst[tweets_data[i].tweet_id]
            tweets_data[i].max_proba = round(rst.max(), 3)
            tweets_data[i].camp = int(rst.argmax()) # 0 for Biden, 1 for Trump

        sess.add_all(tweets_data)
        sess.commit()


def demo_tweets_to_db_fast(sess, start, end, clear=False):
    """
    import tweets to database with prediction
    """
    if clear:
        print("deleting >=", start, "<", end)
        sess.query(Demo_Tweet).filter(
            Demo_Tweet.dt >= start, Demo_Tweet.dt < end).delete()
        sess.commit()
    
    from classifier import Camp_Classifier
    Lebron = Camp_Classifier()
    Lebron.load_2party()

    X = []
    tweets_data = []

    from read_raw_data import read_tweets_json_fast as read_tweets

    # for d, dt in read_tweets(start, end):
    for d, dt in read_tweets():
        tweet_id = d["id"]
        uid = d["user"]["id"]
        _sou = get_source_text(d["source"])
        # hts = get_hashtags_from_tweet(d["hashtags"])

        tweets_data.append(
            Demo_Tweet(tweet_id=tweet_id, user_id=uid,
                       dt=dt, source=_sou)
        )

        X.append(d)
        
        if len(tweets_data) == 2000:
            json_rst = Lebron.predict(X)
            for i in range(len(tweets_data)):
                rst = json_rst[tweets_data[i].tweet_id]
                # print(rst)
                # probas = " ".join([str(round(r, 3)) for r in rst])
                # tweets_data[i].probas = probas
                tweets_data[i].max_proba = str(np.round(rst, 3))#round(rst.max(), 3)
                tweets_data[i].camp = int(rst.argmax())

            sess.add_all(tweets_data)
            sess.commit()
            X = []
            tweets_data = []

    if tweets_data:
        json_rst = Lebron.predict(X)
        for i in range(len(tweets_data)):
            rst = json_rst[tweets_data[i].tweet_id]
            # probas = " ".join([str(round(r, 3)) for r in rst])
            # tweets_data[i].probas = probas
            tweets_data[i].max_proba =str(np.round(rst, 3))#round(rst.max(), 3)
            tweets_data[i].camp = int(rst.argmax())

        sess.add_all(tweets_data)
        sess.commit()


########################## 我是天才 ##########################
def db_to_users(sess, start, end, bots=False):
    # 只获取一次
    users = {}  # 还没插入，可以随时改
    exist_users = get_all_users(sess, bots=bots)
    tweets = get_tweets(sess, start, end, bots=bots)

    for t in tqdm(tweets):
        uid = t.user_id
        tid = t.tweet_id
        camp = None
        if t.proM >= 0.75:
            camp = "M"
        elif t.proM < 0.25:
            camp = "K"

        t_dt = t.dt
        if uid in exist_users:
            continue

        if uid not in users:
            users[uid] = [tid, t_dt, camp]
        elif tid < users[uid][0]:
            users[uid] = [tid, t_dt, camp]

    if bots:
        users = [Bot_User(user_id=uid, tweet_id=v[0],
                          first_dt=v[1], first_camp=v[2]) for uid, v in users.items()]
    else:
        users = [User(user_id=uid, tweet_id=v[0],
                      first_dt=v[1], first_camp=v[2]) for uid, v in users.items()]

    print(f"adding {len(users)} users ...")
    sess.add_all(users)
    sess.commit()


###################### hashtags ######################
def tweets_db_to_hashtags(sess, start, end):
    """
    One month
    """
    from collections import defaultdict
    _hashtags = defaultdict(int)
    _ht_M = defaultdict(int)
    _ht_K = defaultdict(int)

    tweets = sess.query(Tweet.hashtags).filter(
        Tweet.source.is_(None),
        Tweet.hashtags.isnot(None),
        Tweet.proM > 0.75,
        Tweet.dt >= start,
        Tweet.dt < end).yield_per(5000)

    for t in tqdm(tweets):
        hts = t[0].split(",")
        for ht in hts:
            _hashtags[ht] += 1
            _ht_M[ht] += 1

    tweets = sess.query(Tweet.hashtags).filter(
        Tweet.source.is_(None),
        Tweet.hashtags.isnot(None),
        Tweet.dt >= start,
        Tweet.proK > 0.75,
        Tweet.dt < end).yield_per(5000)

    for t in tqdm(tweets):
        hts = t[0].split(",")
        for ht in hts:
            _hashtags[ht] += 1
            _ht_K[ht] += 1

    end = pendulum.today()
    _hashtags = [Hashtag(hashtag=ht, update_dt=end, count=cnt, M_count=_ht_M[ht], K_count=_ht_K[ht])
                 for ht, cnt in _hashtags.items()]
    print(len(_hashtags))

    sess.query(Hashtag).delete()
    sess.commit()
    sess.add_all(_hashtags)
    sess.commit()


def get_top_hashtags(sess):
    from collections import Counter
    _hashtags = Counter()

    tweets = sess.query(Tweet.hashtags).filter(
        Tweet.source.is_(None),
        Tweet.hashtags.isnot(None),
        Tweet.dt >= "2019-04-10",
        Tweet.dt < "2019-05-24").yield_per(5000)

    for t in tqdm(tweets):
        hts = t[0].split(",")
        for ht in hts:
            _hashtags[ht] += 1

    print(_hashtags.most_common(200))


def tweets_db_to_hashtags75(sess, end):
    """
    all tweets
    """
    from collections import defaultdict
    _hashtags = defaultdict(int)

    tweets = sess.query(Tweet.hashtags).filter(
        Tweet.source.is_(None),
        Tweet.hashtags.isnot(None),
        Tweet.dt < end,
        or_(Tweet.proM > 0.75, Tweet.proK > 0.75)).yield_per(5000)

    for t in tqdm(tweets):
        hts = t[0].split(",")
        for ht in hts:
            _hashtags[ht] += 1

    _hashtags = [Hashtag75(hashtag=ht, update_dt=end, count=cnt)
                 for ht, cnt in _hashtags.items()]
    print(len(_hashtags))

    sess.query(Hashtag75).delete()
    sess.commit()

    sess.add_all(_hashtags)
    sess.commit()


def tweets_db_to_hashtags_KM(sess, end):
    from collections import defaultdict
    _hashtags = defaultdict(int)

    tweets = sess.query(Tweet.hashtags).filter(
        Tweet.source.is_(None),
        Tweet.hashtags.isnot(None),
        Tweet.dt < end, Tweet.proM >= 0.75).yield_per(5000)

    for t in tqdm(tweets):
        hts = t[0].split(",")
        for ht in hts:
            _hashtags[ht] += 1

    _hashtags = defaultdict(int)
    tweets = sess.query(Tweet.hashtags).filter(
        Tweet.source.is_(None),
        Tweet.hashtags.isnot(None),
        Tweet.dt < end, Tweet.proM >= 0.75).yield_per(5000)

    for t in tqdm(tweets):
        hts = t[0].split(",")
        for ht in hts:
            _hashtags[ht] += 1


def update_hashtags75(sess, end):
    from collections import defaultdict
    _hashtags = defaultdict(int)

    tweets = sess.query(Tweet.hashtags, Tweet.proM).filter(
        and_(Tweet.source.is_(None),
             and_(Tweet.hashtags.isnot(None),
                  and_(Tweet.dt < end)))).yield_per(5000)
    for t in tqdm(tweets):
        if t[1] > 0.75 or t[1] < 0.25:
            hts = t[0].split(",")
            for ht in hts:
                _hashtags[ht] += 1

    _hashtags = [Hashtag75(hashtag=ht, update_dt=end, count=cnt)
                 for ht, cnt in _hashtags.items()]
    print(len(_hashtags))

    sess.query(Hashtag75).delete()
    sess.commit()
    sess.add_all(_hashtags)
    sess.commit()


def count_of_hashtags(sess, start, end):
    from collections import defaultdict
    _hashtags = defaultdict(int)
    period = pendulum.period(start, end)

    for dt in period:
        tweets = get_tweets_day_with_hashtags(sess, dt)
        for t in tqdm(tweets):
            if t.proM > 0.75 or t.proM < 0.25:
                for ht in t.hashtags.split(","):
                    _hashtags[ht] += 1
    print(_hashtags)
    return dict(_hashtags)


def tweets_db_to_hashtags75_lastweek(sess, end):
    from collections import defaultdict
    _hashtags = defaultdict(int)
    start = end.add(days=-7)
    period = pendulum.period(start, end)

    for dt in period:
        tweets = get_tweets_day_with_hashtags(sess, dt)
        for t in tqdm(tweets):
            if t.proM > 0.75 or t.proK > 0.75:
                for ht in t.hashtags.split(","):
                    _hashtags[ht] += 1
    _hashtags = [Last_Week_Hashtag75(hashtag=ht, update_dt=end, count=cnt)
                 for ht, cnt in _hashtags.items()]
    print(len(_hashtags))

    sess.query(Last_Week_Hashtag75).delete()
    print("clear Last_Week_Hashtag75 ...")
    sess.commit()

    sess.add_all(_hashtags)
    sess.commit()


def get_hashtags75_v2(sess):
    from collections import Counter
    _hashtags = Counter()

    bingo_hashtags = [
        ('fernandezfernandez', 7535),
        ('ganafernandez', 2505),
        ('arrugocristina', 2200),
        ('ahora', 2033),
        ('cfk', 1896),
        ('lacornisa', 1674),
        ('elecciones2019', 1577),
        ('cristinasomostodos', 1546),
        ('unidad', 1522),
        ('buensabado', 1476),
        ('argentina', 1465),
        ('jujuy', 1295),
        ('salta', 1292),
        ('defensoresdelcambio', 1283),
        ('encuesta', 1225),
        ('cambiemos', 1208),
        ('lapampa', 1168),
        ('buendomingo', 1132),
        ('fernandezfernandez2019', 1022),
        ('haceminutos', 983),
        ('nsb', 981),
        ('lanochedeml', 958),
        ('26m', 913),
        ('politica', 877),
        ('albertofernandez', 877),
        ('porunaargentinamejor', 872),
        ('cristina', 871),
        ('aunestamosatiempo', 871),
        ('18may', 817),
        ('elecciones', 775),
        ('mauroenamerica', 668),
        ('venezuela', 665),
        ('convencionucrpba', 657),
        ('escontodos', 629),
        ('urgente', 578),
        ('macri', 530),
        ('4t', 508),
    ]

    set_hts = set([ht[0] for ht in bingo_hashtags])
    start = pendulum.datetime(2019, 5, 18)
    end = pendulum.datetime(2019, 5, 20)
    period = pendulum.period(start, end)
    M_SAT_MON = open("disk/data/M_SAT_MON.id", "w")
    K_SAT_MON = open("disk/data/K_SAT_MON.id", "w")

    for dt in period:
        print(dt)
        tweets = get_tweets_day_with_hashtags(sess, dt)
        for t in tqdm(tweets):
            _goal = False
            if t.proM > 0.75 or t.proK > 0.75:
                for ht in t.hashtags.split(","):
                    # _hashtags[ht] += 1
                    if ht in set_hts:
                        if t.proM > 0.75:
                            M_SAT_MON.write(str(t.tweet_id) + "\n")
                        if t.proM < 0.25:
                            K_SAT_MON.write(str(t.tweet_id) + "\n")
                        break


# ******************** Very important ******************** #
def db_to_stat_predict(sess, start, end, bots=False, clear=False):

    if clear:
        if bots:
            sess.query(Bot_Stat).filter(Bot_Stat.dt >=
                                        start, Bot_Stat.dt < end).delete()
        else:
            sess.query(Stat).filter(Stat.dt >= start, Stat.dt < end).delete()
        sess.commit()

    _dt = start
    while _dt < end:  # per day
        print(_dt)
        users_support = {}
        new_tweets_cnt = 0
        K_tweets, M_tweets, U_tweets = 0, 0, 0
        K_users, M_users, U_users, I_users = 0, 0, 0, 0

        if bots:
            tweets = get_bot_tweets_day(sess, _dt)
        else:
            tweets = get_tweets_day(sess, _dt)

        remove_uids = set()
        for t in tqdm(tweets):
            uid = t.user_id
            proM = t.proM
            if uid not in users_support:
                users_support[uid] = [0, 0, 0]  # K, M, unclassified
            new_tweets_cnt += 1

            if proM < 0:
                remove_uids.add(uid)
            elif proM >= 0.75:
                M_tweets += 1
                users_support[uid][1] += 1
            elif proM < 0.25:
                K_tweets += 1
                users_support[uid][0] += 1
            else:
                U_tweets += 1
                users_support[uid][2] += 1

        for u, _cla in users_support.items():
            if u in remove_uids or (_cla[0] == 0 and _cla[1] == 0):
                I_users += 1
            elif _cla[0] > _cla[1]:
                K_users += 1
            elif _cla[1] > _cla[0]:
                M_users += 1
            else:
                U_users += 1

        if bots:
            cum_t = sess.query(Tweet).filter(Tweet.dt < _dt.add(
                days=1), Tweet.source.isnot(None)).count()
            c_cum_t = sess.query(Tweet).filter(Tweet.dt < _dt.add(days=1), Tweet.source.isnot(None),
                                               or_(Tweet.proK > 0.75, Tweet.proM > 0.75)).count()

            cum_u = sess.query(Bot_User).filter(
                Bot_User.first_dt < _dt.add(days=1)).count()
            c_cum_u = sess.query(Bot_User).filter(Bot_User.first_dt < _dt.add(days=1),
                                                  Bot_User.first_camp.isnot(None)).count()
            sess.add(
                Bot_Stat(dt=_dt,
                         tweet_count=new_tweets_cnt, user_count=len(
                             users_support),
                         tweet_cum_count=cum_t, user_cum_count=cum_u,
                         cla_tweet_cum_count=c_cum_t, cla_user_cum_count=c_cum_u,
                         K_tweet_count=K_tweets, M_tweet_count=M_tweets, U_tweet_count=U_tweets,
                         K_user_count=K_users, M_user_count=M_users,
                         U_user_count=U_users, I_user_count=I_users,))
        else:
            cum_t = sess.query(Tweet).filter(Tweet.dt < _dt.add(
                days=1), Tweet.source.is_(None)).count()
            c_cum_t = sess.query(Tweet).filter(Tweet.dt < _dt.add(days=1), Tweet.source.is_(None),
                                               or_(Tweet.proK > 0.75, Tweet.proM > 0.75)).count()

            cum_u = sess.query(User).filter(
                User.first_dt < _dt.add(days=1)).count()
            c_cum_u = sess.query(User).filter(User.first_dt < _dt.add(days=1),
                                              User.first_camp.isnot(None)).count()

            # new_user_cnt = sess.query(User).filter(
            #     and_(User.first_dt >= _dt, User.first_dt < _dt.add(days=1))).count()

            _s = Stat(dt=_dt,
                      tweet_count=new_tweets_cnt, user_count=len(
                          users_support),

                      tweet_cum_count=cum_t, user_cum_count=cum_u,
                      cla_tweet_cum_count=c_cum_t, cla_user_cum_count=c_cum_u,

                      K_tweet_count=K_tweets, M_tweet_count=M_tweets, U_tweet_count=U_tweets,

                      K_user_count=K_users, M_user_count=M_users,
                      U_user_count=U_users, I_user_count=I_users,)

            pprint(_s.__dict__)
            sess.add(_s)

        sess.commit()
        _dt = _dt.add(days=1)


def add_camp_hashtags(clear=False):
    sess = get_session()

    if clear:
        sess.query(Camp_Hashtag).delete()
        sess.commit()

    for line in open("data/hashtags/2019-09-05.txt"):
        # print(line)
        w = line.strip().split()
        ht = normalize_lower(w[1])
        d = Camp_Hashtag(hashtag=ht, 
            update_dt=pendulum.datetime(2019, 6, 23), camp=w[0]
        )
        try:
            sess.add(d)
            sess.commit()
            print("add:", w)
        except Exception as e:
            print(e)

    sess.close()
    get_camp_hashtags()


def add_other_polls(clear=False):
    sess = get_session()

    if clear:
        sess.query(Other_Poll).delete()
        sess.commit()

    import pandas as pd

    data = pd.read_csv("data/wiki-data.csv")

    for i, row in data.iterrows():
        print(row.poll_name, row.poll_dt)
        sess.add(Other_Poll(
            dt=row.poll_dt,
            name=row.poll_name,
            K=row.K / 100,
            M=row.M / 100,
            U=1 - row.K / 100 - row.M / 100
        ))

    sess.commit()
    sess.close()


# run it each day
def predict_day(sess, dt, lag=14, bots=False, clear=False):
    """
    use tweets in the last 14 (lag) days to predict everyday
    dt is today,
    so, start is -14 day, end is -1 day.
    save -1 day in the db
    """

    if clear:
        if bots:
            sess.query(Bot_Weekly_Predict).filter(
                Bot_Weekly_Predict.dt == dt).delete()
            sess.commit()
        elif not bots:
            sess.query(Weekly_Predict).filter(Weekly_Predict.dt == dt).delete()
            sess.commit()

    start = dt.add(days=-lag)
    end = dt

    users = {}
    # print("predict daily!", start, "~", end)
    tweets = get_tweets(sess, start, end, bots=bots)
    # remove_uid = set()

    for t in tweets:
        uid = t.user_id
        # if uid in remove_uid:
        # continue
        if uid not in users:
            users[uid] = {
                "proM": 0,
                "proK": 0,
                "Unclassified": 0,
                "Junk": 0,
            }

        if t.proM < 0:
            # remove_uid.add(uid)
            # if uid in users:
                # users.pop(uid)
            users[uid]["Junk"] += 1
        elif t.proM >= 0.75:
            users[uid]["proM"] += 1
        elif t.proM < 0.25:
            users[uid]["proK"] += 1
        else:
            users[uid]["Unclassified"] += 1

    cnt = {
        "K": 0,
        "M": 0,
        "U": 0,
        # "irrelevant": len(remove_uid),
        "irrelevant": 0,
    }

    for u, v in users.items():
        if v["Junk"] > 0:
            continue
        if v["proM"] > v["proK"]:
            cnt["M"] += 1
        elif v["proM"] < v["proK"]:
            cnt["K"] += 1
        elif v["proM"] > 0 or v["proK"] > 0:
            cnt["U"] += 1
        else:
            cnt["irrelevant"] += 1

    print(dt, cnt)

    if not bots:
        sess.add(Weekly_Predict(dt=dt,
                                U_Cristina=cnt["K"],
                                U_Macri=cnt["M"],
                                U_unclassified=cnt["U"],
                                U_irrelevant=cnt["irrelevant"]))
    else:
        sess.add(Bot_Weekly_Predict(dt=dt,
                                    U_Cristina=cnt["K"],
                                    U_Macri=cnt["M"],
                                    U_unclassified=cnt["U"],
                                    U_irrelevant=cnt["irrelevant"]))

    sess.commit()


def demo_predict_to_db(dt, clear=False):

    sess = get_session()
    
    if clear:
        sess.query(Weekly_Predict_v1).filter(Weekly_Predict_v1.dt == dt).delete()
        sess.commit()
        sess.query(Cumulative_Predict_v1).filter(Cumulative_Predict_v1.dt == dt).delete()
        sess.commit()

    from prediction_from_db import get_share_from_csv
    rst = get_share_from_csv(f"disk/users-14days/{dt.to_date_string()}.csv")

    sess.add(Weekly_Predict_v1(
        dt=dt,
        state="USA",
        c0=rst[0],
        c1=rst[1],
        c2=rst[2],
        c3=rst[3],
        c4=rst[4],
    ))
    sess.commit()

    rst = get_share_from_csv(f"disk/users-culFrom01/{dt.to_date_string()}.csv")
    sess.add(Cumulative_Predict_v1(
        dt=dt,
        state="USA",
        c0=rst[0],
        c1=rst[1],
        c2=rst[2],
        c3=rst[3],
        c4=rst[4],
    ))
    sess.commit()
    
    sess.close()


def predict_cumulative_to_csv(start, end, in_dir="from_March_1", prob=0.68):
    """
    从历史开始每天累积
    """
    rsts = []
    for dt in pendulum.period(start, end):
        print("load ~", f"disk/cul_{in_dir}/{dt.to_date_string()}-{prob}.txt")
        cul_today = json.load(
            open(f"disk/cul_{in_dir}/{dt.to_date_string()}-{prob}.txt"))
        cnt = get_camp_count_from_users(cul_today)
        cnt["dt"] = dt.to_date_string()
        print(cnt)
        rsts.append(cnt)
    pd.DataFrame(rsts).set_index("dt").to_csv(f"data/cul_start_{in_dir}.csv")


def predict_culmulative_swing_loyal(start, end, prob=0.68):
    """
    从历史开始每天累积
    """
    rsts = []
    intention = {}

    _period = pendulum.Period(start, end)
    for dt in _period:
        print(dt)

        # save
        print(
            "load ~", f"disk/cul_from_March_1/{dt.to_date_string()}-{prob}.txt")
        cul_today = json.load(
            open(f"disk/cul_from_March_1/{dt.to_date_string()}-{prob}.txt"))

        # user-level
        for u, v in cul_today.items():
            if u not in intention:
                intention[u] = "new"
                if v["I"] > 0:
                    intention[u] = "JUNK"
                elif v["M"] > v["K"]:
                    intention[u] = "loyal MP"
                elif v["M"] < v["K"]:
                    intention[u] = "loyal FF"
                elif v["M"] == 0 and v["K"] == 0:
                    intention[u] = "loyal Others"
                else:
                    intention[u] = "swing Others"

            else:
                _int = intention[u]
                if v["I"] > 0:
                    intention[u] = "JUNK"

                if _int == "loyal MP":
                    if v["M"] > v["K"]:
                        continue
                    elif v["M"] == v["K"]:
                        intention[u] = "swing Others"
                    elif v["M"] < v["K"]:
                        intention[u] = "swing FF"

                elif _int == "loyal FF":
                    if v["K"] > v["M"]:
                        continue
                    elif v["M"] == v["K"]:
                        intention[u] = "swing Others"
                    elif v["K"] < v["M"]:
                        intention[u] = "swing MP"

                elif _int == "swing MP":
                    if v["M"] > 2 * v["K"]:
                        intention[u] = "loyal MP"
                    elif v["M"] > v["K"]:
                        continue
                    elif v["M"] == v["K"]:
                        intention[u] = "swing Others"
                    elif v["M"] < v["K"]:
                        intention[u] = "swing FF"

                elif _int == "swing FF":
                    if v["K"] > 2 * v["M"]:
                        intention[u] = "loyal FF"
                    elif v["K"] > v["M"]:
                        continue
                    elif v["M"] == v["K"]:
                        intention[u] = "swing Others"
                    elif v["K"] < v["M"]:
                        intention[u] = "swing MP"

                elif _int == "loyal Others":
                    if v["K"] == ["M"] and v["K"] > 0:
                        intention[u] = "swing Others"
                    elif v["K"] > v["M"]:
                        intention[u] = "loyal FF"
                    elif v["K"] < v["M"]:
                        intention[u] = "loyal MP"

                elif _int == "swing Others":
                    if v["K"] > v["M"]:
                        intention[u] = "swing FF"
                    elif v["K"] < v["M"]:
                        intention[u] = "swing MP"

        cnt = {
            "dt": dt.to_date_string(),
            "loyal FF": 0,
            "swing FF": 0,
            "swing Others": 0,
            "swing MP": 0,
            "loyal MP": 0,
            "loyal Others": 0,
            "JUNK": 0
        }

        for u, v in intention.items():
            cnt[v] += 1
        print(cnt)

        rsts.append(cnt)

    pd.DataFrame(rsts).set_index("dt").to_csv(
        f"data/swings_and_loyals-end-{dt.to_date_string()}.csv")


def predict_culmulative_swing_loyal_v2(start, end, prob=0.68):
    """
    从历史开始每天累积
    """
    rsts = []
    intention = {}

    _period = pendulum.Period(start, end)
    for dt in _period:
        print(dt)

        # save
        print(
            "load ~", f"disk/cul_from_March_1/{dt.to_date_string()}-{prob}.txt")
        cul_today = json.load(
            open(f"disk/cul_from_March_1/{dt.to_date_string()}-{prob}.txt"))

        # user-level
        for u, v in cul_today.items():
            if u not in intention:
                intention[u] = "new"
                if v["I"] > 0:
                    intention[u] = "JUNK"
                elif v["M"] > v["K"]:
                    intention[u] = "loyal MP"
                elif v["M"] < v["K"]:
                    intention[u] = "loyal FF"
                elif v["M"] == 0 and v["K"] == 0:
                    intention[u] = "loyal Others"
                else:
                    intention[u] = "swing Others"

            else:
                _int = intention[u]
                if v["I"] > 0:
                    intention[u] = "JUNK"

                if _int == "loyal MP":
                    if v["M"] > v["K"]:
                        continue
                    elif v["M"] == v["K"]:
                        intention[u] = "swing Others"
                    elif v["M"] < v["K"]:
                        intention[u] = "swing FF"

                elif _int == "loyal FF":
                    if v["K"] > v["M"]:
                        continue
                    elif v["M"] == v["K"]:
                        intention[u] = "swing Others"
                    elif v["K"] < v["M"]:
                        intention[u] = "swing MP"

                elif _int == "swing MP":
                    if v["M"] > 2 * v["K"]:
                        intention[u] = "loyal MP"
                    elif v["M"] > v["K"]:
                        continue
                    elif v["M"] == v["K"]:
                        intention[u] = "swing Others"
                    elif v["M"] < v["K"]:
                        intention[u] = "swing FF"

                elif _int == "swing FF":
                    if v["K"] > 2 * v["M"]:
                        intention[u] = "loyal FF"
                    elif v["K"] > v["M"]:
                        continue
                    elif v["M"] == v["K"]:
                        intention[u] = "swing Others"
                    elif v["K"] < v["M"]:
                        intention[u] = "swing MP"

                elif _int == "loyal Others":
                    if v["K"] == ["M"] and v["K"] > 0:
                        intention[u] = "swing Others"
                    elif v["K"] > v["M"]:
                        intention[u] = "loyal FF"
                    elif v["K"] < v["M"]:
                        intention[u] = "loyal MP"

                elif _int == "swing Others":
                    if v["K"] > v["M"]:
                        intention[u] = "swing FF"
                    elif v["K"] < v["M"]:
                        intention[u] = "swing MP"

        cnt = {
            "dt": dt.to_date_string(),
            "loyal FF": 0,
            "swing FF": 0,
            "swing Others": 0,
            "swing MP": 0,
            "loyal MP": 0,
            "loyal Others": 0,
            "JUNK": 0
        }

        for u, v in intention.items():
            cnt[v] += 1
        print(cnt)

        rsts.append(cnt)

    pd.DataFrame(rsts).set_index("dt").to_csv(
        f"data/swings_and_loyals-end-{dt.to_date_string()}.csv")


def predict_culmulative_user_class(start, end, prob=0.68):
    """
    从历史开始每天累积
    """
    _period = pendulum.Period(start, end)
    for dt in _period:
        print(dt)
        # save
        print(
            "load ~", f"disk/cul_from_March_1/{dt.to_date_string()}-{prob}.txt")
        cul_today = json.load(
            open(f"disk/cul_from_March_1/{dt.to_date_string()}-{prob}.txt"))

        # cnt = {
        #     "dt": dt.to_date_string(),
        #     "Fl": [],
        #     "Ml": [],
        #     "Fs": [],
        #     "Ms": [],
        #     "U": [],
        #     "I": [],
        # }
        # # user-level
        # for u, v in cul_today.items():
        #     if v["I"] > 0:
        #         continue
        #     if v["M"] > v["K"]:
        #         if v["M"] > (v["K"] * 2):
        #             cnt["Ml"].append(u)
        #         else:
        #             cnt["Ms"].append(u)
        #     elif v["M"] < v["K"]:
        #         if v["K"] > (v["M"] * 2):
        #             cnt["Fl"].append(u)
        #         else:
        #             cnt["Fs"].append(u)
        #     elif v["M"] > 0 or v["K"] > 0:
        #         cnt["U"].append(u)
        #     else:
        #         cnt["I"].append(u)

        # print("save ~", f"disk/user_class/{dt.to_date_string()}-{prob}.txt")
        # json.dump(cnt, open(f"disk/user_class/{dt.to_date_string()}-{prob}.txt", "w"))


def new_users_in_different_class(start, end, w=14, prob=0.68):
    rsts = []
    for dt in pendulum.Period(start, end):
        print(
            "load ~", f"disk/cul_from_March_1/{dt.to_date_string()}-{prob}.txt")
        previous = json.load(
            open(f"disk/cul_from_March_1/{dt.to_date_string()}-{prob}.txt"))
        previous_users = set(previous.keys())
        today_str = dt.add(days=w).to_date_string()
        if os.path.exists(f'disk/cul_from_March_1/{today_str}-{prob}.txt'):
            today = json.load(
                open(f'disk/cul_from_March_1/{today_str}-{prob}.txt'))
        else:
            break
        today_users = set(today.keys())

        new_users = today_users - previous_users
        cnt = {
            "dt": today_str,
            "New users (FF)": 0,
            "New users (MP)": 0,
            "New users (Others)": 0
        }
        for u in new_users:
            if today[u]["K"] > today[u]["M"]:
                cnt["New users (FF)"] += 1
            elif today[u]["K"] < today[u]["M"]:
                cnt["New users (MP)"] += 1
            else:
                cnt["New users (Others)"] += 1
        print(cnt)
        rsts.append(cnt)
    pd.DataFrame(rsts).set_index("dt").to_csv(
        f"data/new-users-end-{today_str}-{w}.csv")


def old_users_in_different_class(start, end, peri=1, w=14, prob=0.68, norm=False):
    import ujson as json

    rsts = []
    for dt in pendulum.Period(start, end):
        today_str = dt.add(days=peri * w).to_date_string()
        if not os.path.exists(f'disk/users-14days/{today_str}-{prob}.txt'):
            break

        live_K = []
        live_M = []
        live_U = []

        always_K_users = None
        always_M_users = None
        always_U_users = None

        for i in range(peri):
            print(f"period={peri}, i={i}")
            print(
                "load ~", f"disk/users-14days/{dt.add(days=i * w + w).to_date_string()}-{prob}.txt")
            prev = json.load(
                open(f"disk/users-14days/{dt.add(days=i * w + w).to_date_string()}-{prob}.txt"))
            K_users = set()
            M_users = set()
            U_users = set()
            for u, v in prev.items():
                if v["I"] > 0:
                    continue
                if v["K"] > v["M"]:
                    K_users.add(u)
                elif v["K"] < v["M"]:
                    M_users.add(u)
                else:
                    U_users.add(u)

            live_K.append(K_users)
            live_M.append(M_users)
            live_U.append(U_users)
            print(len(live_K[i]), len(live_M[i]), len(live_U[i]))

        for i in range(peri):
            Ku = live_K[i]
            # print("Users of K:", len(Ku))
            if always_K_users is None:
                always_K_users = Ku
            else:
                always_K_users = always_K_users & Ku
            # print("Union:", len(always_K_users))

        for i in range(peri):
            Mu = live_M[i]
            # print("Users of M:", len(Mu))
            if always_M_users is None:
                always_M_users = Mu
            else:
                always_M_users = always_M_users & Mu
            print("Union:", len(always_M_users))

        for i in range(peri):
            Uu = live_U[i]
            # print("Users of U:", len(Uu))
            if always_U_users is None:
                always_U_users = Uu
            else:
                always_U_users = always_U_users & Uu
            # print("Union:", len(always_U_users))

        cnt = {
            "dt": today_str,
            "users (FF)": len(always_K_users),
            "users (MP)": len(always_M_users),
            "users (Others)": len(always_U_users)
        }

        # if norm:
        #     today = json.load(open(f'disk/cul_from_March_1/{today_str}-{prob}.txt'))
        #     today_users_set = get_user_set(today)
        #     cnt["users (FF)"] /= len(today_users_set["K"])
        #     cnt["users (MP)"] /= len(today_users_set["M"])
        #     cnt["users (Other)"] /= len(today_users_set["U"])

        print(cnt)
        rsts.append(cnt)

    if norm:
        pd.DataFrame(rsts).set_index("dt").to_csv(
            f"data/permenant-users-end-{today_str}-{peri}-norm.csv")
    else:
        pd.DataFrame(rsts).set_index("dt").to_csv(
            f"data/permenant-users-end-{today_str}-{peri}.csv")


def predict_cumulative_file(start, end, out_dir="culFromSep"):
    """
    making users-culFromSep
    """
    # users = {}
    def union_users(u1, u2):
        u_temp = {}
        for uid, v in u1.items():
            u_temp[uid] = {}
            u_temp[uid]["M"] = v["M"]
            u_temp[uid]["K"] = v["K"]
            u_temp[uid]["U"] = v["U"]
            u_temp[uid]["I"] = v["I"]
        for uid, v in u2.items():
            if uid not in u_temp:
                u_temp[uid] = {
                    "M": 0,
                    "K": 0,
                    "U": 0,
                    "I": 0,
                }
            u_temp[uid]["M"] += v["M"]
            u_temp[uid]["K"] += v["K"]
            u_temp[uid]["U"] += v["U"]
            u_temp[uid]["I"] += v["I"]
        return u_temp

    for dt in pendulum.Period(start, end):
        # print(dt)
        if dt.to_date_string() <= "2019-03-02":
            continue
        # 在预测的时间序列上，永远是不包含end！也就是说3月2日的预测，实际用的是3月1日的数据
        yesterday_str = dt.add(days=-1).to_date_string()
        cul_yesterday = json.load(
            open(f"disk/cul_{out_dir}/{yesterday_str}-{prob}.txt"))
        users_today = json.load(open(f"disk/users/{yesterday_str}-{prob}.txt"))
        cul_today = union_users(users_today, cul_yesterday)
        # save
        print("save ~", f"disk/cul_{out_dir}/{dt.to_date_string()}-{prob}.txt")
        json.dump(cul_today, open(
            f"disk/cul_{out_dir}/{dt.to_date_string()}-{prob}.txt", "w"))


def predict_cumulative_file_ignore(start, end, prob=0.68):
    """
    从历史开始每天累积
    """
    # users = {}
    def union_users(u1, u2):
        u_temp = {}
        for uid, v in u1.items():
            u_temp[uid] = {}
            u_temp[uid]["M"] = v["M"]
            u_temp[uid]["K"] = v["K"]
            u_temp[uid]["U"] = v["U"]
            u_temp[uid]["I"] = v["I"]
        for uid, v in u2.items():
            if uid not in u_temp:
                u_temp[uid] = {
                    "M": 0,
                    "K": 0,
                    "U": 0,
                    "I": 0,
                }
            u_temp[uid]["M"] += v["M"]
            u_temp[uid]["K"] += v["K"]
            u_temp[uid]["U"] += v["U"]
            u_temp[uid]["I"] += v["I"]
        return u_temp

    for dt in pendulum.Period(start, end):
        print(dt)
        if dt.to_date_string() <= "2019-03-01":
            continue
        # 在预测的时间序列上，永远是不包含end！也就是说3月2日的预测，实际用的是3月1日的数据
        yesterday_str = dt.add(days=-1).to_date_string()
        cul_yesterday = json.load(
            open(f"disk/cul_from_March_1_ignore1/{yesterday_str}-{prob}.txt"))
        users_today = json.load(open(f"disk/users/{yesterday_str}-{prob}.txt"))
        cul_today = union_users(users_today, cul_yesterday)
        # save
        print(
            "save ~", f"disk/cul_from_March_1/{dt.to_date_string()}-{prob}.txt")
        json.dump(cul_today, open(
            f"disk/cul_from_March_1/{dt.to_date_string()}-{prob}.txt", "w"))

# ============================================================================

# def save_union_users():
#     """
#     新需求 for paper，看最后k条tweets, 2019-03-01 ~ 2019-10-10
#     """
#     all_users = {}
#     for dt in tqdm(pendulum.Period(pendulum.Date(2019, 3, 1), pendulum.Date(2019, 10, 10))):
#         print(dt)
#         users = json.load(open(f"disk/users/{dt.to_date_string()}-0.68.txt"))
#         for u, v in users.items():
#             v["dt"] = dt.to_date_string()
#             if u not in all_users:
#                 all_users[u] = [v]
#             else:
#                 all_users[u].append(v)

#     with open("disk/users-20190301-20191010.json", "w") as f:
#         for u, v in all_users.items():
#             r = {}
#             r["uid"] = u
#             bingo = True
#             for t in v:
#                 if t["I"] > 0:
#                     bingo = False
#             r["tweets"] = v
#             if bingo:
#                 f.write(json.dumps(r) + "\n")


def save_union_users_v2():
    """
    新需求，看最后k条tweets, 2019-03-01 ~ 2019-10-10
    """
    import random
    k = 20  # 50
    out_file = open("disk/users-20190301-20191010-opinion-20.json", "w")

    for line in tqdm(open("disk/users-20190301-20191010.json")):
        d = json.loads(line.strip())
        uid = d["uid"]
        tweets = d["tweets"]
        # print(len(tweets))
        user_rst = {"uid": uid, "opinion": []}
        # i = 0
        # t = tweets[i]
        user_cum = {"FF": 0, "MP": 0}
        last_k_tweets = []

        for t in tweets:
            today_tweets = []
            for i in range(t["K"]):
                today_tweets.append("K")
            for i in range(t["M"]):
                today_tweets.append("M")
            random.shuffle(today_tweets)
            last_k_tweets.extend(today_tweets)
            last_k_tweets = last_k_tweets[-k:]

            if t["K"] + t["M"] >= k:
                if t["K"] > t["M"]:
                    opinion_short = "FF"
                elif t["K"] < t["M"]:
                    opinion_short = "MP"
                else:
                    opinion_short = "Undecided"
            else:
                MP_count = 0
                FF_count = 0
                for camp in last_k_tweets:
                    if camp == "K":
                        FF_count += 1
                    elif camp == "M":
                        MP_count += 1
                if FF_count > MP_count:
                    opinion_short = "FF"
                elif FF_count < MP_count:
                    opinion_short = "MP"
                else:
                    opinion_short = "Undecided"

            # long
            user_cum["FF"] += t["K"]
            user_cum["MP"] += t["M"]

            if user_cum["FF"] > user_cum["MP"]:
                opinion_long = "loyal FF"
                if user_cum["MP"] == 0:
                    opinion_long = "Ultra loyal FF"
            elif user_cum["FF"] < user_cum["MP"]:
                opinion_long = "loyal MP"
                if user_cum["FF"] == 0:
                    opinion_long = "Ultra loyal MP"
            elif user_cum["FF"] == user_cum["MP"]:
                if user_cum["FF"] > 0:
                    opinion_long = "Undecided"
                else:
                    opinion_long = "Others"

            if opinion_long.startswith("Ultra") or opinion_long.startswith("Others"):
                opinion = opinion_long
            else:
                opinion = opinion_long + " - " + opinion_short

            # Opinion does not change
            if user_rst["opinion"]:
                if user_rst["opinion"][-1]["long - short"] != opinion:
                    user_rst["opinion"].append({
                        "dt": t["dt"],
                        "long - short": opinion
                    })
            else:
                user_rst["opinion"].append({
                    "dt": t["dt"],
                    "long - short": opinion
                })
        # print(user_rst)
        out_file.write(json.dumps(user_rst) + "\n")


def save_union_users_v3():
    """
    新需求，看最后k条tweets, 2019-03-01 ~ 2019-10-10
    """
    rsts = {}
    all_dates = [dt.to_date_string() for dt in pendulum.Period(
        pendulum.Date(2019, 3, 1), pendulum.Date(2019, 10, 10))]

    for dt in all_dates:
        rsts[dt] = {
            "Ultra loyal FF": 0,
            "Ultra loyal MP": 0,
            "loyal FF - FF": 0,
            "loyal FF - MP": 0,
            "loyal FF - Undecided": 0,
            "loyal MP - FF": 0,
            "loyal MP - MP": 0,
            "loyal MP - Undecided": 0,
            "Undecided - FF": 0,
            "Undecided - MP": 0,
            "Undecided - Undecided": 0,
            "Others": 0,
        }
    # print(rsts)

    for line in tqdm(open("disk/users-20190301-20191010-opinion-20.json")):
        d = json.loads(line.strip())
        # uid = d["uid"]
        opinion = d["opinion"]
        i = 0
        dt_op = opinion[0]["dt"]

        for dt in all_dates:
            if dt == dt_op:  # opinion may change
                op = opinion[i]["long - short"]
                i += 1
                if i <= len(opinion) - 1:
                    dt_op = opinion[i]["dt"]

            if i > 0:  # from the first opinion
                rsts[dt][op] += 1

    json.dump(rsts, open("disk/users-20190301-20191010-opinion-20-ts.json", "w"))


def save_today_user_snapshot_ignore(sess, now, prob, ignore_id):
    """
    保存每天用户的行为快照，在每天的数据下，应该是实际当天的日期。
    """
    tweets = get_tweets(sess, now, now.add(days=1))
    users = {}
    for t in tqdm(tweets):
        if t.tweet_id in ignore_id:
            continue
        uid = t.user_id
        if uid not in users:
            users[uid] = {
                "M": 0,
                "K": 0,
                "U": 0,
                "I": 0,
            }

        if t.proM < 0:
            users[uid]["I"] += 1
        elif t.proM >= prob:
            users[uid]["M"] += 1
        elif t.proM < 1 - prob:
            users[uid]["K"] += 1
        else:
            users[uid]["U"] += 1

    json.dump(users, open(
        f"disk/users-ignore1/{now.to_date_string()}-{prob}.txt", "w"))


# def save_user_snapshot(start, end, w=14, prob=0.68):

#     def union_users(u1, u2):
#         u_temp = {}
#         for uid, v in u1.items():
#             u_temp[uid] = {}
#             u_temp[uid]["M"] = v["M"]
#             u_temp[uid]["K"] = v["K"]
#             u_temp[uid]["U"] = v["U"]
#             u_temp[uid]["I"] = v["I"]
#         for uid, v in u2.items():
#             if uid not in u_temp:
#                 u_temp[uid] = {
#                     "M": 0,
#                     "K": 0,
#                     "U": 0,
#                     "I": 0,
#                 }
#             u_temp[uid]["M"] += v["M"]
#             u_temp[uid]["K"] += v["K"]
#             u_temp[uid]["U"] += v["U"]
#             u_temp[uid]["I"] += v["I"]
#         return u_temp

#     for dt in pendulum.Period(start, end):
#         users = {}
#         print(dt.to_date_string())
#         _end = dt.add(days=w-1)
#         for now in pendulum.Period(dt, _end):
#             _u = json.load(open(f"disk/users/{now.to_date_string()}-{prob}.txt"))
#             users = union_users(users, _u)

#         json.dump(users, open(
#             f"disk/users-{w}days/{_end.to_date_string()}-{prob}.txt", "w"))


def get_camp_count_from_users(_users):
    cnt = {
        "FF": 0,
        "MP": 0,
        "Others": 0,
    }

    for u, v in _users.items():
        if v["I"] > 0:
            continue
        if v["K"] > v["M"]:
            cnt["FF"] += 1
        elif v["M"] > v["K"]:
            cnt["MP"] += 1
        else:
            cnt["Others"] += 1
    return cnt


def predict_user_snapshot(win=7):
    """
    7天为时间窗口的用户快照
    """
    # keep_set = set([int(line.strip()) for line in open("data/0731-week-keep.txt")])
    # Cristina_set = set([int(line.strip()) for line in open("data/0731-week-Cristna.txt")])
    # elecciones_set = set([int(line.strip()) for line in open("data/0731-week-elecciones.txt")])

    # keep_set = set([int(line.strip()) for line in open("data/0731-week-terms1.txt")])
    # keep_set = set([int(line.strip()) for line in open("data/0731-week-terms2.txt")])
    sess = get_session()

    start = pendulum.datetime(2019, 3, 1, tz="UTC")
    end = pendulum.datetime(2019, 8, 21, tz="UTC")
    _period = pendulum.Period(start, end)

    for dt in _period:
        print(dt)
        users = {}
        tweets = get_tweets(sess, dt.add(days=-win), dt)

        for t in tqdm(tweets):
            uid = t.user_id
            if uid not in users:
                users[uid] = {
                    "M": 0,
                    "K": 0,
                    "U": 0,
                    "I": 0,
                }
            if t.proM < 0:
                users[uid]["I"] += 1
            elif t.proM >= 0.68:
                users[uid]["M"] += 1
            elif t.proM < 0.32:
                users[uid]["K"] += 1
            else:
                users[uid]["U"] += 1

        json.dump(users, open(
            f"disk/users/{dt.to_date_string()}-0.68.txt", "w"))

        # if win == 7:
        #     json.dump(users, open(f"disk/users/{dt.to_date_string()}.txt", "w"))
        # else:
        #     json.dump(users, open(f"disk/users-{win}days/{dt.to_date_string()}.txt", "w"))

        # cnt = {
        #     "K": 0,
        #     "M": 0,
        #     "U": 0,
        #     "I": 0,
        # }

        # for u, v in users.items():
        #     if v["M"] > v["K"]:
        #         cnt["M"] += 1
        #     elif v["M"] < v["K"]:
        #         cnt["K"] += 1
        #     elif v["M"] > 0 or v["K"] > 0:
        #         cnt["U"] += 1
        #     else:
        #         cnt["I"] += 1

        # print(dt, cnt)

    sess.close()


def predict_user_before_PASO(p):
    """
    PASO分析，设置不同的t_0
    """
    start = pendulum.datetime(2019, 3, 1, tz="UTC")
    end = pendulum.datetime(2019, 8, 10, tz="UTC")
    dt_users = {}
    # load user-data
    for dt in pendulum.Period(start, end):
        dt = dt.to_date_string()
        print("loading ...", dt)
        users = json.load(open(f"disk/users/{dt}-{p}.txt"))
        dt_users[dt] = users

    def union_users(u1, u2):
        u_temp = {}
        for uid, v in u1.items():
            u_temp[uid] = {}
            u_temp[uid]["M"] = v["M"]
            u_temp[uid]["K"] = v["K"]
            u_temp[uid]["U"] = v["U"]
            u_temp[uid]["I"] = v["I"]
        for uid, v in u2.items():
            if uid not in u_temp:
                u_temp[uid] = {
                    "M": 0,
                    "K": 0,
                    "U": 0,
                    "I": 0,
                }
            u_temp[uid]["M"] += v["M"]
            u_temp[uid]["K"] += v["K"]
            u_temp[uid]["U"] += v["U"]
            u_temp[uid]["I"] += v["I"]
        return u_temp

    start = pendulum.datetime(2019, 3, 1, tz="UTC")
    end = pendulum.datetime(2019, 8, 10, tz="UTC")
    now = end
    cul_users = {}
    while now >= start:
        print(now)
        now_str = now.to_date_string()
        cul_users = union_users(cul_users, dt_users[now_str])
        json.dump(cul_users, open(f"disk/PASO/{now_str}-{p}.txt", "w"))
        now = now.add(days=-1)  # 日期不断前推


def predict_percent(sess, dt, clear=False):
    """
    use tweets in the last 14 (lag) days to predict everyday
    dt is today,
    so, start is -14 day, end is -1 day.
    save -1 day in the db
    """

    if clear:
        sess.query(Percent).filter(Percent.dt == dt).delete()
        sess.commit()

    # 2019-06-27 updates
    # k = (0.67, 0.27, 0.525)
    k = (1, 0)
    r = sess.query(Weekly_Predict).filter(Weekly_Predict.dt == dt).one()

    M_pro = r.U_Macri / (r.U_Cristina + r.U_Macri)
    M_pro = M_pro * k[0] + k[1]
    K_pro = 1 - M_pro

    print(dt, K_pro, M_pro)
    sess.add(Percent(dt=dt, K=K_pro, M=M_pro))
    sess.commit()


def get_percent(sess, dt, clas=2):
    dt = pendulum.parse(dt)
    r = sess.query(Percent).filter(Percent.dt == dt).one()
    K_pro = r.K
    M_pro = r.M

    if clas == 2:
        # print(dt, r.K, r.M)
        return r.K, r.M

    elif clas == 3:
        r = sess.query(Weekly_Predict).filter(Weekly_Predict.dt == dt).one()
        U_pro = r.U_unclassified / \
            (r.U_Cristina + r.U_Macri + r.U_unclassified)
        left = 1 - U_pro
        K_pro3 = left * K_pro
        M_pro3 = left * M_pro
        # print(dt, K_pro3, M_pro3, U_pro)
        return K_pro3, M_pro3, U_pro
    

def get_camp_hashtags():
    # print("Loaded camp hashtags.")
    sess = get_session()
    hts = sess.query(Camp_Hashtag)
    hts = [(ht.hashtag, ht.camp) for ht in hts]
    print(f"Loaded {len(hts)} camp hashtags.")
    sess.close()
    return hts


def get_retweets(sess, start, end):
    """
    获取某段时间的全部retweets
    """
    print(f"Get retweets from {start} to {end}")
    tweets = sess.query(Retweet).filter(
        Retweet.dt >= start,
        Retweet.dt < end).yield_per(5000)
    return tweets


def get_ori_users(sess, start, end, uid):
    """
    获取原始user_id
    """
    # print(f"Get retweets from {start} to {end}")
    tweets = sess.query(Retweet.ori_user_id).filter(
        Retweet.user_id == uid,
        Retweet.dt >= start,
        Retweet.dt < end).distinct()
    return tweets


def get_ori_users_v2(start, end, uids):
    """
    获取原始user_id

    __tablename__ = "retweets"
    tweet_id = Column(Integer, primary_key=True)
    dt = Column(DateTime)
    user_id = Column(Integer)
    ori_tweet_id = Column(Integer)
    ori_user_id = Column(Integer)

    """
    sess = get_session()
    friends_of_users = defaultdict(set)
    for t in tqdm(get_retweets(sess, start, end)):
        if t.user_id in uids:
            friends_of_users[t.user_id].add(t.ori_user_id)
    sess.close()
    return friends_of_users


def get_retweets_graph():
    """
    导入转发推特
    """
    import networkx as nx
    sess = get_session()

    # ALL
    g = nx.DiGraph()
    for r, t in tqdm(sess.query(Retweet, Tweet).
                     filter(Retweet.tweet_id == Tweet.tweet_id).
                     filter(or_(Tweet.proM < 0.25, Tweet.proM > 0.75)).yield_per(5000)):

        g.add_edge(r.ori_user_id, r.user_id)

    out_name = "disk/data/network_ALL.gp"
    print("saving the graph ...", out_name)
    nx.write_gpickle(g, out_name)
    return 0

    # Cristina
    g = nx.DiGraph()
    for r, t in tqdm(sess.query(Retweet, Tweet).
                     filter(Retweet.tweet_id == Tweet.tweet_id).
                     filter(Tweet.proM < 0.25).yield_per(5000)):

        g.add_edge(r.ori_user_id, r.user_id)

    out_name = "disk/data/network_K.gp"
    print("saving the graph ...", out_name)
    nx.write_gpickle(g, out_name)

    # Macri
    g = nx.DiGraph()
    for r, t in tqdm(sess.query(Retweet, Tweet).
                     filter(Retweet.tweet_id == Tweet.tweet_id).
                     filter(Tweet.proM > 0.75).yield_per(5000)):

        g.add_edge(r.ori_user_id, r.user_id)

    out_name = "disk/data/network_M.gp"
    print("saving the graph ...", out_name)
    nx.write_gpickle(g, out_name)


def get_all_tweets_75():
    sess = get_session()
    tweets = sess.query(Tweet.tweet_id).filter(
        Tweet.source.is_(None), Tweet.proM >= 0.75).yield_per(5000)
    with open("data/tweets-proM-0.75.txt", "w") as f:
        for t in tqdm(tweets):
            f.write(str(t[0]) + "\n")
    sess.close()


def get_all_tweets_25():
    sess = get_session()
    tweets = sess.query(Tweet.tweet_id).filter(
        Tweet.source.is_(None), Tweet.proM <= 0.25).yield_per(5000)
    with open("data/tweets-proM-0.25.txt", "w") as f:
        for t in tqdm(tweets):
            f.write(str(t[0]) + "\n")
    sess.close()


def get_all_tweets_with_hashtags(sess):
    tweets = sess.query(Tweet.tweet_id, Tweet.hashtags, Tweet.dt).filter(
        Tweet.hashtags.isnot(None)).yield_per(5000)
    return tweets


def get_tweets_with_hashtags(sess, start, end):
    tweets = sess.query(Tweet.tweet_id, Tweet.hashtags).filter(
        Tweet.hashtags.isnot(None),
        Tweet.source.is_(None),
        Tweet.dt >= start,
        Tweet.dt < end).yield_per(5000)
    return tweets


def get_tweets(sess, start, end):
    tweets = sess.query(Tweet).filter(
        Tweet.source.is_(None),
        Tweet.dt >= start,
        Tweet.dt < end).yield_per(5000)
    return tweets


def get_demo_tweets(sess, start, end):
    tweets = sess.query(Demo_Tweet).filter(
        Demo_Tweet.source.is_(None),
        Demo_Tweet.dt >= start,
        Demo_Tweet.dt < end).yield_per(5000)
    return tweets
    

def get_tweets_day(sess, dt):
    """
    获取某天的全部tweets
    """
    print(f"Get tweets from in {dt}")
    tweets = sess.query(Tweet).filter(
        Tweet.source.is_(None),
        Tweet.dt >= dt,
        Tweet.dt < dt.add(days=1)).yield_per(5000)
    return tweets


def get_bot_tweets_day(sess, dt):
    """
    获取某天的全部tweets
    """
    print(f"Get bots tweets from in {dt}")
    tweets = sess.query(Tweet).filter(
        Tweet.source.isnot(None),
        Tweet.dt >= dt,
        Tweet.dt < dt.add(days=1)).yield_per(5000)
    return tweets


def get_all_users(sess, bots=False):
    """
    获取某天的全部tweets
    """
    print(f"Get all users from DB")
    if not bots:
        users = sess.query(User).all()
    else:
        users = sess.query(Bot_User).all()

    users = {u.user_id: u.first_camp for u in users}
    return users


def clients_stat():
    """
    统计clients
    """
    sess = get_session()
    tweets = sess.query(Source.source).yield_per(5000)

    from collections import Counter
    cnt = Counter()
    for t in tweets:
        cnt[t[0]] += 1

    json.dump(cnt.most_common(), open(
        "data/client_stat_2019-05-07.json", "w"), indent=2)
    sess.close()


def get_tweets_day_with_hashtags(sess, dt):
    """
    获取某天的全部tweets
    """
    tweets = sess.query(Tweet).filter(
        Tweet.source.is_(None),
        Tweet.hashtags.isnot(None),
        Tweet.dt >= dt,
        Tweet.dt < dt.add(days=1)).yield_per(5000)
    return tweets


def get_term_stat():
    sess = get_session()
    new_data = []
    for t in sess.query(Term).order_by(desc(Term.proK)):
        new_data.append([
            t.name,
            t.proK,
            t.proM,
            t.unclassified,
        ])
    sess.close()
    return new_data


def cumulative_prediction_results_to_db(rsts): # rewrite
    sess = get_session()
    for r in rsts:
        if sess.query(exists().where(Cumulative_Predict_v2._id == r["_id"])).scalar(): # 若存在，则删除
            sess.query(Cumulative_Predict_v2).filter(Cumulative_Predict_v2._id == r["_id"]).delete()
        d = Cumulative_Predict_v2(
            _id=r["_id"],
            dt=pendulum.parse(r["dt"]),
            state=r["state"],
            Biden=r["Biden"],
            Trump=r["Trump"],
            Undec=r["Undec"],
        )
        sess.add(d)
        sess.commit()
    sess.close()


def get_db_prediction_results(state="all"):
    sess = get_session()
    if state == "all":
        rsts = sess.query(Cumulative_Predict_v2).all()
    else:
        rsts = sess.query(Cumulative_Predict_v2).filter(Cumulative_Predict_v2.state==state).all()
    sess.close()
    return rsts


def get_session():
    engine = create_engine("sqlite:////home/alex/kayzhou/US_election/data/election-trump-biden.db")
    # engine = create_engine("sqlite:////home/alex/kayzhou/US_election/data/election-trump-biden-July.db")
    DBSession = sessionmaker(bind=engine)
    session = DBSession()
    return session


def get_session_2():
    engine = create_engine("sqlite:////home/alex/kayzhou/US_election/data/election-trump-biden.db")
    # engine = create_engine("sqlite:////home/alex/kayzhou/US_election/data/election-trump-biden-July.db")
    DBSession = sessionmaker(bind=engine)
    session = DBSession()
    return session

    
def init_db():
    engine = create_engine("sqlite://///media/zhenkun/election-from-Jan-to-June.db")
    Base.metadata.create_all(engine)


def init_db_2():
    engine = create_engine("sqlite:////media/zhenkun/election-from-Jan-to-June.db")
    Base.metadata.create_all(engine)


if __name__ == "__main__":
    init_db_2()
    sess = get_session_2()
    tweets_to_db_fast(sess)