# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    extract_train_data.py                              :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Zhenkun <zhenkun91@outlook.com>            +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2020/01/21 09:47:55 by Kay Zhou          #+#    #+#              #
#    Updated: 2020/08/12 19:15:07 by Zhenkun          ###   ########.fr        #
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
    # "202007",
])


# PB BS EW JB OT=others
def read_classified_hashtags():
    # labels = "PB BS EW JB OT".split()
    # classified_hts = {
    #     "PB": set(),
    #     "BS": set(),
    #     "EW": set(),
    #     "JB": set(),
    #     "OT": set(),
    # }

    # 2020-01-21
    # classified_hts = {
    #     "PB": set(),
    #     "BS": set(),
    #     "EW": set(),
    #     "JB": set(),
    #     "OT": set(),
    #     "MB": set()
    # }

    # 2020-03-06
    #classified_hts = {
    #    "BS": set(),
    #    "JB": set(),
    #    "OT": set(),
    #}
    
    # 2020-03-25
    # classified_hts = {
    #     "BS": set(),
    #     "JB": set(),
    # }

    # classified_hts = {
    #     "DT": set(),
    #     "BS": set(),
    #     "JB": set(),
    #     "OT": set(),
    # }

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
            _ht, label, category = w[0], w[1], w[2]
            if label == "UNK":
                continue
            # print(_ht, label)
            if label in classified_hts:
                classified_hts[label].add(_ht)
                category_hts[category].add(_ht)
                
    print(classified_hts)
    return classified_hts, category_hts

# with open(train_dir + "train.txt", "w") as f: # 2020-03-06
#     for dt_dir in Path("raw_data").iterdir():
#         set_id = set() # remove dups
#         for in_name in dt_dir.iterdir():
#             if in_name.stem.split("-")[-1] in election_files and in_name.parts[1] in months:
#                 print(in_name)
#                 for line in tqdm(open(in_name)):
#                     label_bingo_times = 0
#                     label = None
#                     data = json.loads(line.strip())
                    
#                     # ignoring retweets
#                     if 'retweeted_status' in data and data["text"].startswith("RT @"): 
#                         continue
#                     set_hts = set([ht["text"].lower() for ht in data["hashtags"]])
#                     if not set_hts:
#                         continue
                        
#                     if data["id"] in set_id:
#                         continue
#                     set_id.add(data["id"])
                    
#                     for _label, _set_hts in classified_hts.items():
#                         for _ht in set_hts:
#                             if _ht in _set_hts:
#                                 label = _label
#                                 label_bingo_times += 1
#                                 break
                                
#                     # one tweet (in traindata) should have 0 or 1 class hashtag
#                     if label and label_bingo_times == 1:
#                         text = data["text"].replace("\n", " ").replace("\t", " ")
#                         f.write(label + "\t" + text + "\n")


if __name__ == "__main__":
    classified_hts, category_hts = read_classified_hashtags()
    JB_hts = classified_hts["JB"]
    DT_hts = classified_hts["DT"]
    JB_hts_c = category_hts["JB"]
    DT_hts_c = category_hts["DT"]
    print(len(JB_hts), len(JB_hts_c), len(DT_hts), len(DT_hts_c))

    with open(train_dir + "train.txt", "w") as f: # 2020-03-06
        # for dt_dir in Path("raw_data").iterdir():
        set_id = set()  # remove dups
        #     for in_name in dt_dir.iterdir():
        #         if in_name.stem.split("-")[-1] in election_files and in_name.parts[1] in months:
        #             print(in_name)

        months = ["202001", "202002", "202003", "202004", "202005", "202006"]
        for month in months:
            print(month)
            for line in tqdm(open(f"/media/alex/data/US2020_raw/{month}.lj")):
                try:
                    data = json.loads(line.strip())
                except Exception:
                    print('json.loads Error:', line)
                    continue

                label_bingo_times = 0
                label = None
                
                # ignoring retweets
                if 'retweeted_status' in data and data["text"].startswith("RT @"): 
                    continue
                set_hts = set([ht["text"].lower() for ht in data["hashtags"]])
                if not set_hts:
                    continue
                    
                if data["id"] in set_id:
                    continue
                set_id.add(data["id"])

                for _ht in set_hts:
                    if _ht in JB_hts:
                        label_bingo_times += 1
                        label = "JB"
                        break
                for _ht in set_hts:
                    if _ht in DT_hts:
                        label_bingo_times += 1
                        label = "DT"
                        break

                if not (label and label_bingo_times == 1):
                    continue

                # 判断类别
                cate = "UNK"
                cate_bingo_times = 0
                for _ht in set_hts:
                    if _ht in JB_hts_c:
                        cate_bingo_times += 1
                        cate = "JB"
                        break
                for _ht in set_hts:
                    if _ht in DT_hts_c:
                        cate_bingo_times += 1
                        cate = "DT"
                        break
                
                if cate_bingo_times == 2:
                    cate = "JB+DT"
                    
                # one tweet (in traindata) should have 0 or 1 class hashtag
                if label and label_bingo_times == 1:
                    text = data["text"].replace("\n", " ").replace("\t", " ")
                    f.write(label + "\t" + cate + "\t" + text + "\n")
