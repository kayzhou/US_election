# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    extract_train_data.py                              :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Zhenkun <zhenkun91@outlook.com>            +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2020/01/21 09:47:55 by Kay Zhou          #+#    #+#              #
#    Updated: 2020/09/24 14:45:21 by Zhenkun          ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

from my_weapon import *
from pathlib import Path
from collections import Counter
from tqdm import tqdm

train_dir = "data/train-08/"

# demo_files = set([
#     "Bernie Sanders",
#     "SenSanders",
#     "Joe Biden",
#     "JoeBiden",
#     # "Mike Bloomberg",
#     # "MikeBloomberg",
#     # "Tulsi Gabbard",
#     # "TulsiGabbard",
#     # "Elizabeth Warren",
#     # "ewarren",
#     # "Amy Klobuchar",
#     # "amyklobuchar",  
#     # "Pete Buttigieg",
#     # "PeteButtigieg",
#     # "John Delaney",
#     # "JohnDelaney",
#     # "Tulsi Gabbard",
#     # "TulsiGabbard",
#     # "Amy Klobuchar",
#     # "amyklobuchar",
#     # "Deval Patrick",
#     # "DevalPatrick",
#     # "Bernie Sanders",
#     # "SenSanders",
#     # "Tom Steyer",
#     # "TomSteyer",
#     # "Elizabeth Warren",
#     # "ewarren",
#     # "Andrew Yang",
#     # "AndrewYang",
#     "Donald Trump",
#     "realDonaldTrump",
# ])

election_files = set([
    "Biden"
    "Trump",
    "Joe Biden",
    "JoeBiden",
    "Donald Trump",
    "realDonaldTrump"
    "Trump OR Biden",
    "biden OR joebiden",
    "trump OR donaldtrump OR realdonaldtrump",
])

months = set([
    "202001",
    "202002",
    "202003",
    "202004",
    "202005",
    "202006",
    "202007",
])


# PB BS EW JB OT=others
def read_classified_hashtags():

    classified_hts = {
        "JB": set(),
        "DT": set()
    }
    category_hts = {
        "JB": set(),
        "DT": set()
    }

    for line in open(train_dir + "hashtags.txt"):     # 2020-03-06
        if not line.startswith("#"):
            w = line.strip().split()
            print(w)
            _ht, label, category = w[0], w[1], w[2]
            if label == "UNK":
                continue
            # print(_ht, label)
            if label in classified_hts:
                classified_hts[label].add(_ht)
                category_hts[category].add(_ht)
                
    print(classified_hts)
    return classified_hts, category_hts


def ext_1():
    """
    只考虑第一列，支持或反对
    """
    classified_hts, category_hts = read_classified_hashtags()
    with open(train_dir + "train.txt", "w") as f:
        months = ["202001", "202002", "202003", "202004", "202005", "202006"]
        for month in months:
            print(month)
            for line in tqdm(open(f"raw_data/raw_data/{month}.lj")):       
                label_bingo_times = 0
                label = None
                try:
                    data = json.loads(line)
                except Exception:
                    print("json.loads ERROR:", line)
                    
                # ignoring retweets
                if 'retweeted_status' in data and data["text"].startswith("RT @"): 
                    continue
                set_hts = set([ht["text"].lower() for ht in data["hashtags"]])
                # 如果没有hashtags
                if not set_hts:
                    continue
                
                for _label, _set_hts in classified_hts.items():
                    for _ht in set_hts:
                        if _ht in _set_hts:
                            label = _label
                            label_bingo_times += 1
                            break
                            
                # one tweet (in traindata) should have 0 or 1 class hashtag
                if label and label_bingo_times == 1:
                    text = data["text"].replace("\n", " ").replace("\t", " ")
                    f.write(label + "\t" + text + "\n")


def ext_2():
    """
    考虑支持和类别
    根据category完全分成两个分类器～ 
    """
    classified_hts, category_hts = read_classified_hashtags()
    JB_hts = classified_hts["JB"]
    DT_hts = classified_hts["DT"]
    JB_hts_c = category_hts["JB"]
    DT_hts_c = category_hts["DT"]

    print(len(JB_hts), len(JB_hts_c), len(DT_hts), len(DT_hts_c))

    f_trump = open(train_dir + "train-T.txt", "w")
    f_biden = open(train_dir + "train-B.txt", "w")

    months = ["202001", "202002", "202003", "202004", "202005", "202006"]

    for month in months:
        print(month)
        for line in tqdm(open(f"raw_data/raw_data/{month}.lj")):       
            try:
                data = json.loads(line)
            except Exception:
                print("json.loads ERROR:", line)
                
            # ignoring retweets
            if 'retweeted_status' in data and data["text"].startswith("RT @"): 
                continue
            set_hts = set([ht["text"].lower() for ht in data["hashtags"]])
            
            # 如果没有hashtags
            if not set_hts:
                continue
            
            label_bingo_times = 0
            label = None
            for _label, _set_hts in classified_hts.items():
                for _ht in set_hts:
                    if _ht in _set_hts and _ht in DT_hts_c:
                        label = _label
                        label_bingo_times += 1
                        break   
            # one tweet (in traindata) should have 0 or 1 class hashtag
            if label and label_bingo_times == 1:
                text = data["text"].replace("\n", " ").replace("\t", " ")
                f_trump.write(label + "\t" + text + "\n")

            label_bingo_times = 0
            label = None
            for _label, _set_hts in classified_hts.items():
                for _ht in set_hts:
                    if _ht in _set_hts and _ht in JB_hts_c:
                        label = _label
                        label_bingo_times += 1
                        break   
            # one tweet (in traindata) should have 0 or 1 class hashtag
            if label and label_bingo_times == 1:
                text = data["text"].replace("\n", " ").replace("\t", " ")
                f_biden.write(label + "\t" + text + "\n")


if __name__ == "__main__":
    # ext_1()
    ext_2()