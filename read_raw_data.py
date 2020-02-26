# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    read_raw_data.py                                   :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Kay Zhou <zhenkun91@outlook.com>           +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2020/02/11 11:16:25 by Kay Zhou          #+#    #+#              #
#    Updated: 2020/02/24 18:43:38 by Kay Zhou         ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

demo_files = set([
    "Bernie Sanders",
    "SenSanders",
    "Joe Biden",
    "JoeBiden",
    "Mike Bloomberg",
    "MikeBloomberg",
    "Tulsi Gabbard",
    "TulsiGabbard",
    "Amy Klobuchar",
    "amyklobuchar",
    "Tom Steyer",
    "TomSteyer",
    "Elizabeth Warren",
    "ewarren",
    "Pete Buttigieg",
    "PeteButtigieg",
    "Mayor Pete",
    "Pete",
    "Buttigieg",
    # "Michael Bennet",
    # "SenatorBennet",
    # "Andrew Yang",
    # "AndrewYang",
    # "Deval Patrick",
    # "DevalPatrick",
    # "John Delaney",
    # "JohnDelaney",
])

from collections import Counter
from pathlib import Path

import pendulum
import ujson as json
from file_read_backwards import FileReadBackwards
from tqdm import tqdm


def read_tweets_json(start, end):

    months = set([
        "202002",
        # "202001",
        # "201912",
        # "201911",
        # "201910",
        # "201909",
    ])

    set_tweets = set()
    file_names = sorted(Path("raw_data").rglob("*.txt"), reverse=True)

    for in_name in file_names:
        if in_name.stem.split("-")[-1] in demo_files and in_name.parts[1] in months:
            print(in_name)
            cnt = 0

            with FileReadBackwards(in_name) as f:
                while True:
                    line = f.readline()
                    if not line:
                        print(cnt, "end!")
                        print("-" * 50)
                        break

                    d = json.loads(line.strip())
                    tweet_id = d["id"]
                    if tweet_id in set_tweets:
                        continue
                    set_tweets.add(tweet_id)

                    dt = pendulum.from_format(
                        d["created_at"], 'ddd MMM DD HH:mm:ss ZZ YYYY')
                    if dt < start:
                        print("sum:", cnt, d["created_at"], "end!")
                        break
                    if dt >= end:
                        continue

                    if cnt % 50000 == 0:
                        print("New data ->", cnt)
                    cnt += 1
                    yield d, dt


def read_user_profile(start, end, set_users_before=None):

    months = set([
        "202002",
        # "202001",
        # "201912",
        # "201911",
        # "201910",
        # "201909",
    ])

    if set_users_before:
        set_users = set_users_before
    else:
        set_users = set()

    file_names = sorted(Path("raw_data").rglob("*.txt"), reverse=True)

    for in_name in file_names:
        if in_name.stem.split("-")[-1] in demo_files and in_name.parts[1] in months:
            print(in_name)
            cnt = 0

            with FileReadBackwards(in_name) as f:
                while True:
                    line = f.readline()
                    if not line:
                        print(cnt, "end!")
                        print("-" * 50)
                        break

                    d = json.loads(line.strip())
                    u = d["user"]
                    user_id = u["id"]
                    if "location" not in u:
                        continue
                    if user_id in set_users:
                        continue
                    set_users.add(user_id)

                    dt = pendulum.from_format(
                        d["created_at"], 'ddd MMM DD HH:mm:ss ZZ YYYY')
                    if dt < start:
                        print("sum:", cnt, d["created_at"], "end!")
                        break
                    if dt >= end:
                        continue

                    if cnt % 1000 == 0:
                        print("New user ->", cnt)
                    cnt += 1
                    yield u


# {"created_at": "Mon Oct 07 14:04:18 +0000 2019", "hashtags": [], "id": 1181208634031841281, "id_str": "1181208634031841281", "lang": "pt", "retweet_count": 5191, "retweeted_status": {"created_at": "Tue Oct 01 22:23:20 +0000 2019", "favorite_count": 36024, "full_text": "A Câmara aprovou a MP885 que agiliza a venda de bens confiscados do tráfico de drogas e autoriza a utilização pelo Estado do dinheiro decorrente da venda.Minhas congratulações e agradecimentos aos deputados,em especial ao Rel Dep Capitão Wagner e ao PR Dep @RodrigoMaia .", "hashtags": [], "id": 1179159890163650562, "id_str": "1179159890163650562", "lang": "pt", "retweet_count": 5191, "source": "<a href=\"https://mobile.twitter.com\" rel=\"nofollow\">Twitter Web App</a>", "urls": [], "user": {"created_at": "Tue Apr 02 15:04:28 +0000 2019", "default_profile": true, "description": "Ministro da Justiça e Segurança Pública", "favourites_count": 5, "followers_count": 1521172, "friends_count": 19, "id": 1113094855281008641, "id_str": "1113094855281008641", "listed_count": 1161, "location": "Brasília, Brazil", "name": "Sergio Moro", "profile_background_color": "F5F8FA", "profile_banner_url": "https://pbs.twimg.com/profile_banners/1113094855281008641/1554225040", "profile_image_url": "http://pbs.twimg.com/profile_images/1113126472225619968/HOtiwqV9_normal.png", "profile_image_url_https": "https://pbs.twimg.com/profile_images/1113126472225619968/HOtiwqV9_normal.png", "profile_link_color": "1DA1F2", "profile_sidebar_border_color": "C0DEED", "profile_sidebar_fill_color": "DDEEF6", "profile_text_color": "333333", "profile_use_background_image": true, "screen_name": "SF_Moro", "statuses_count": 523, "url": "https://t.co/xIFEp6gr0A", "verified": true}, "user_mentions": [{"id": 35260027, "id_str": "35260027", "name": "Rodrigo Maia", "screen_name": "RodrigoMaia"}]}, "source": "<a href=\"https://mobile.twitter.com\" rel=\"nofollow\">Twitter Web App</a>", "urls": [], "user": {"created_at": "Mon Oct 18 14:19:17 +0000 2010", "description": "A esquerda abaixo de Tudo. Deus Acima de Todos.", "favourites_count": 6221, "followers_count": 57, "friends_count": 99, "id": 204351979, "id_str": "204351979", "location": "Rio Grande do Sul", "name": "charles master", "profile_background_color": "B2DFDA", "profile_background_image_url": "http://abs.twimg.com/images/themes/theme13/bg.gif", "profile_background_image_url_https": "https://abs.twimg.com/images/themes/theme13/bg.gif", "profile_banner_url": "https://pbs.twimg.com/profile_banners/204351979/1564116291", "profile_image_url": "http://pbs.twimg.com/profile_images/1154611820902211591/ft7xbmQM_normal.jpg", "profile_image_url_https": "https://pbs.twimg.com/profile_images/1154611820902211591/ft7xbmQM_normal.jpg", "profile_link_color": "93A644", "profile_sidebar_border_color": "EEEEEE", "profile_sidebar_fill_color": "FFFFFF", "profile_text_color": "333333", "profile_use_background_image": true, "screen_name": "charlescdr1", "statuses_count": 5482}, "user_mentions": [{"id": 1113094855281008641, "id_str": "1113094855281008641", "name": "Sergio Moro", "screen_name": "SF_Moro"}], "keyword": "capitao_wagner", "text": "RT @SF_Moro: A Câmara aprovou a MP885 que agiliza a venda de bens confiscados do tráfico de drogas e autoriza a utilização pelo Estado do d…", "stamp": 1570457058}


def write_fast_raw_data(start, end):
    months = set([
        "202002",
        "202001",
    ])

    out_file = open(f"data/fast_raw_tweets/{start.to_date_string()}-{end.to_date_string()}.lj", "w")
    set_tweets = set()
    file_names = sorted(Path("raw_data").rglob("*.txt"), reverse=True)

    for in_name in file_names:
        if in_name.stem.split("-")[-1] in demo_files and in_name.parts[1] in months:
            print(in_name)
            cnt = 0
            with FileReadBackwards(in_name) as f:
                while True:
                    line = f.readline()
                    if not line:
                        print(cnt, "end!")
                        print("-" * 50)
                        break

                    d = json.loads(line.strip())

                    tweet_id = d["id"]
                    if tweet_id in set_tweets:
                        continue
                    set_tweets.add(tweet_id)

                    dt = pendulum.from_format(
                        d["created_at"], 'ddd MMM DD HH:mm:ss ZZ YYYY')
                    if dt < start:
                        print("sum:", cnt, d["created_at"], "end!")
                        break
                    if dt >= end:
                        continue

                    if cnt % 50000 == 0:
                        print("New data ->", cnt)
                    cnt += 1

                    if "retweeted_status" in d and d["text"].startswith("RT @"):
                        d = {
                            "created_at": d["created_at"],
                            "hashtags": d["retweeted_status"]["hashtags"],
                            "id": d["id"],
                            "user": {
                                "id": d["user"]["id"],
                                "screen_name": d["user"]["screen_name"],
                            },
                            "source": d["source"],
                            "text": d["retweeted_status"]["full_text"]
                        }

                    else:
                        d = {
                            "created_at": d["created_at"],
                            "hashtags": d["hashtags"],
                            "id": d["id"],
                            "user": {
                                "id": d["user"]["id"],
                                "screen_name": d["user"]["screen_name"],
                            },
                            "source": d["source"],
                            "text": d["text"]
                        }

                    out_file.write(json.dumps(d, ensure_ascii=False) + "\n")


def read_tweets_json_fast():

    in_names = [
        "2020-01-01-2020-02-24.lj"
    ]

    for in_name in in_names:
        for line in tqdm(open("data/fast_raw_tweets/" + in_name)):
            d = json.loads(line.strip())
            dt = pendulum.from_format(
                d["created_at"], 'ddd MMM DD HH:mm:ss ZZ YYYY')
            yield d, dt



if __name__ == '__main__':
    start = pendulum.datetime(2020, 1, 1, tz="UTC")
    end = pendulum.datetime(2020, 2, 24, tz="UTC")
    write_fast_raw_data(start, end)
    
