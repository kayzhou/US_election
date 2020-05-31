# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    US2016.py                                          :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Zhenkun <zhenkun91@outlook.com>            +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2019/09/12 10:34:13 by Kay Zhou          #+#    #+#              #
#    Updated: 2020/05/31 23:46:53 by Zhenkun          ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

from my_weapon import *
from TwProcess import CustomTweetTokenizer

# df_proba1 = pd.read_pickle("/media/alex/data/election_data/data/complete_trump_vs_hillary/df_proba_corrected_official_client_june1_sep1_signi_final_2.pickle")
# df_proba2 = pd.read_pickle("/media/alex/data/election_data/data/complete_trump_vs_hillary_sep-nov/df_proba_corrected_official_client_sep1_nov9_signi_final.pickle")
# df_proba = pd.concat([df_proba1, df_proba2])
# df_proba = df_proba.set_index("tweet_id")

# df_proba = pd.read_pickle("disk/data/df_proba_corrected_official_client_june1_nov9_signi_final.pickle")
# df_proba.to_dict()
# print(len(df_proba))

import sqlite3
# from tqdm import tqdm_notebook as tqdm

# deal with Hillary
DB1_NAME = "/media/alex/data/election_data/data/complete_trump_vs_hillary_db.sqlite"
DB2_NAME = "/media/alex/data/election_data/data/complete_trump_vs_hillary_sep-nov_db.sqlite"

# file_hillary_id = open("disk/data/hillary_ids_ignore.txt", "w")

# conn = sqlite3.connect(DB1_NAME)
# c = conn.cursor()
# # c.execute("SELECT count(*) FROM tweet_to_retweeted_uid LIMIT 1;")
# c.execute("SELECT tweet_id, text FROM tweet;")
# for t in tqdm(c.fetchall()):
#     text = t[1].lower()
#     if "trump" in text or "clinton" in text:
#         continue
#     elif "hillary" in text:
#         file_hillary_id.write(str(t[0]) + "\n")
# conn.close()

# conn = sqlite3.connect(DB2_NAME)
# c = conn.cursor()
# # c.execute("SELECT count(*) FROM tweet_to_retweeted_uid LIMIT 1;")
# c.execute("SELECT tweet_id, text FROM tweet;")
# for t in tqdm(c.fetchall()):
#     text = t[1].lower()
#     if "trump" in text or "clinton" in text:
#         continue
#     elif "hillary" in text:
#         file_hillary_id.write(str(t[0]) + "\n")
# conn.close()

# file_hillary_id = open("disk/data/only_trump_ids.txt", "w")

def only_query():
    file_trump = open("disk/data/only_trump_tweets.txt", "w")

    conn = sqlite3.connect(DB1_NAME)
    c = conn.cursor()
    # c.execute("SELECT count(*) FROM tweet_to_retweeted_uid LIMIT 1;")
    c.execute("SELECT tweet_id, text FROM tweet;")
    for t in tqdm(c.fetchall()):
        text = t[1].lower()
        if "clinton" in text or "hillary" in text:
            continue
        elif "trump" in text:
        #     file_hillary_id.write(str(t[0]) + "\n")
            file_trump.write(text + "\n")
    conn.close()

    conn = sqlite3.connect(DB2_NAME)
    c = conn.cursor()
    # c.execute("SELECT count(*) FROM tweet_to_retweeted_uid LIMIT 1;")
    c.execute("SELECT tweet_id, text FROM tweet;")
    for t in tqdm(c.fetchall()):
        text = t[1].lower()
        if "clinton" in text or "hillary" in text:
            continue
        elif "trump" in text:
            # file_hillary_id.write(str(t[0]) + "\n")
            file_trump.write(text + "\n")
    conn.close()


def processing_tweets():
    in_file = open("disk/data/only_trump_words.txt", "w")
    tokenizer = CustomTweetTokenizer()
    for line in tqdm(open("disk/data/only_trump_tweets.txt")):
        words = tokenizer.tokenize(line)
        in_file.write(" ".join(words) + "\n")
    in_file.close()


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

tuple_official_twitter_clients = (
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
)


def get_trump_client():
    # tweet_ids_only_trump = {int(line.strip()) for line in open("disk/data/only_trump_ids.txt")}
    out_file = open("disk/data/tweetid_clientid.txt", "w")

    conn = sqlite3.connect(DB1_NAME)
    c = conn.cursor()
    # c.execute("SELECT count(*) FROM tweet_to_retweeted_uid LIMIT 1;")
    c.execute("SELECT tweet.tweet_id, source_content FROM tweet, source_content WHERE tweet.source_content_id=source_content.id;")
    for t in tqdm(c.fetchall()):
        if t[1] in official_twitter_clients:
            out_file.write(f"{t[0]}\t1\n")
        else:
            out_file.write(f"{t[0]}\t0\n")
    conn.close()

    conn = sqlite3.connect(DB2_NAME)
    c = conn.cursor()
    c.execute("SELECT tweet.tweet_id, source_content FROM tweet, source_content WHERE tweet.source_content_id=source_content.id;")
    for t in tqdm(c.fetchall()):
        if t[1] in official_twitter_clients:
            out_file.write(f"{t[0]}\t1\n")
        else:
            out_file.write(f"{t[0]}\t0\n")
    conn.close()
    

def get_tweets_proba():
    with open("disk/data/tweetid_userid_pro_hillary.csv", "w") as f:
        conn = sqlite3.connect(DB1_NAME)
        c = conn.cursor()
        # c.execute("SELECT count(*) FROM tweet_to_retweeted_uid LIMIT 1;")
        sql = f'SELECT tweet.user_id, tweet.datetime_EST, class_proba.p_pro_hillary_anti_trump FROM class_proba, tweet, source_content WHERE tweet.source_content_id=source_content.id and tweet.tweet_id=class_proba.tweet_id and source_content.source_content in {tuple_official_twitter_clients}'
        print("SQL excute:", sql)
        c.execute(sql)
        for t in c.fetchall():
            f.write(f'{t[0]},{t[1]},{t[2]:.3f}\n')
        conn.close()

        conn = sqlite3.connect(DB2_NAME)
        c = conn.cursor()
        print("SQL excute:", sql)
        c.execute(sql)
        for t in c.fetchall():
            f.write(f'{t[0]},{t[1]},{t[2]:.3f}\n')
        conn.close()


def get_tweets_proba():
    ### move from Argentina_election
    with open("disk/data/tweetid_userid_pro_hillary.csv", "w") as f:
        conn = sqlite3.connect(DB1_NAME)
        c = conn.cursor()
        # c.execute("SELECT count(*) FROM tweet_to_retweeted_uid LIMIT 1;")
        sql = f'SELECT tweet.user_id, tweet.datetime_EST, class_proba.p_pro_hillary_anti_trump FROM class_proba, tweet, source_content WHERE tweet.source_content_id=source_content.id and tweet.tweet_id=class_proba.tweet_id and source_content.source_content in {tuple_official_twitter_clients}'
        print("SQL excute:", sql)
        c.execute(sql)
        for t in c.fetchall():
            f.write(f'{t[0]},{t[1]},{t[2]:.3f}\n')
        conn.close()

        conn = sqlite3.connect(DB2_NAME)
        c = conn.cursor()
        print("SQL excute:", sql)
        c.execute(sql)
        for t in c.fetchall():
            f.write(f'{t[0]},{t[1]},{t[2]:.3f}\n')
        conn.close()


def get_location_users():
    with open("disk/us2016_users_location.csv", "w") as f:
        f.write("uid,location\n")
        conn = sqlite3.connect(DB1_NAME)
        c = conn.cursor()
        # c.execute("SELECT count(*) FROM tweet_to_retweeted_uid LIMIT 1;")
        sql = 'SELECT user_id, first_location FROM users;'
        print("SQL excute:", sql)
        c.execute(sql)
        for t in c.fetchall():
            f.write(f'{t[0]},"{t[1]}"\n')
        conn.close()

        conn = sqlite3.connect(DB2_NAME)
        c = conn.cursor()
        print("SQL excute:", sql)
        c.execute(sql)
        for t in c.fetchall():
            f.write(f'{t[0]},"{t[1]}"\n')
        conn.close()


if __name__ == '__main__':
    # processing_tweets()
    # get_tweets_proba()
    get_location_users()