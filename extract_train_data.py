# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    extract_train_data.py                              :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Kay Zhou <zhenkun91@outlook.com>           +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2020/01/21 09:47:55 by Kay Zhou          #+#    #+#              #
#    Updated: 2020/03/27 15:42:55 by Kay Zhou         ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

from my_weapon import *
from pathlib import Path
from collections import Counter
from tqdm import tqdm

train_dir = "data/2020-03-25-3/"
##Matteo changed  this 3/11
demo_files = set([
    "Bernie Sanders",
    "SenSanders",
    "Joe Biden",
    "JoeBiden",
    # "Mike Bloomberg",
    # "MikeBloomberg",
    # "Tulsi Gabbard",
    # "TulsiGabbard",
    # "Elizabeth Warren",
    # "ewarren",
    # "Amy Klobuchar",
    # "amyklobuchar",  
    # "Pete Buttigieg",
    # "PeteButtigieg",
    # "John Delaney",
    # "JohnDelaney",
    # "Tulsi Gabbard",
    # "TulsiGabbard",
    # "Amy Klobuchar",
    # "amyklobuchar",
    # "Deval Patrick",
    # "DevalPatrick",
    # "Bernie Sanders",
    # "SenSanders",
    # "Tom Steyer",
    # "TomSteyer",
    # "Elizabeth Warren",
    # "ewarren",
    # "Andrew Yang",
    # "AndrewYang",
    "Donald Trump",
    "realDonaldTrump",
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

    classified_hts = {
        "DT": set(),
        "BS": set(),
        "JB": set(),
        "OT": set(),
    }

    for line in open(train_dir + "hashtags.txt"):     # 2020-03-06
        if not line.startswith("#"):
            w = line.strip().split()
            _ht, label = w[0], w[1]

            if label == "UNK":
                continue
            elif label not in ["DT", "BS", "JB"]:
                label = "OT"
            
            print(_ht, label)

            if label in classified_hts:
                classified_hts[label].add(_ht)
                
    print(classified_hts)
    return classified_hts

classified_hts = read_classified_hashtags()


with open(train_dir + "train.txt", "w") as f: # 2020-03-06
    for dt_dir in Path("raw_data").iterdir():
        set_id = set()
        for in_name in dt_dir.iterdir():
            if in_name.stem.split("-")[-1] not in demo_files:
                continue

            print(in_name)
            for line in tqdm(open(in_name)):
                label_bingo_times = 0
                label = None
                data = json.loads(line.strip())
                _id = data["id"]
                
                # ignoring retweets
                if 'retweeted_status' in data and data["text"].startswith("RT @"): 
                    continue
                    
                if _id in set_id:
                    continue
                set_id.add(_id)

                set_hts = set([ht["text"].lower() for ht in data["hashtags"]])
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
