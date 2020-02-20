# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    extract_train_data.py                              :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Kay Zhou <zhenkun91@outlook.com>           +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2020/01/21 09:47:55 by Kay Zhou          #+#    #+#              #
#    Updated: 2020/02/09 04:15:11 by Kay Zhou         ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

from my_weapon import *
from pathlib import Path
from collections import Counter
from tqdm import tqdm

demo_files = set([
    "Michael Bennet",
    "SenatorBennet",
    "Joe Biden",
    "JoeBiden",
    "Mike Bloomberg",
    "MikeBloomberg",
    "Pete Buttigieg",
    "PeteButtigieg",
    "John Delaney",
    "JohnDelaney",
    "Tulsi Gabbard",
    "TulsiGabbard",
    "Amy Klobuchar",
    "amyklobuchar",
    "Deval Patrick",
    "DevalPatrick",
    "Bernie Sanders",
    "SenSanders",
    "Tom Steyer",
    "TomSteyer",
    "Elizabeth Warren",
    "ewarren",
    "Andrew Yang",
    "AndrewYang",
])

# PB BS EW JB OT=others
def read_classified_hashtags():
    # labels = "PB BS EW JB OT".split()
    classified_hts = {
        "PB": set(),
        "BS": set(),
        "EW": set(),
        "JB": set(),
        "OT": set(),
    }
    for line in open("data/hashtags-20200201_classified_hernan_Feb6.txt"):
        if not line.startswith("#"):
            w = line.strip().split(" ")
            label, _ht = w[0], w[1]
            if len(w) == 3:
                classified_hts[label].add(_ht)
    return classified_hts

classified_hts = read_classified_hashtags()

with open("data/traindata-20200209.txt", "w") as f:
    for in_name in Path("raw_data").rglob("*.txt"):
        if in_name.stem.split("-")[-1] in demo_files:
            print(in_name)
            for line in tqdm(open(in_name)):
                label_bingo_times = 0
                label = None
                data = json.loads(line.strip())
                
                # ignoring retweets
                if 'retweeted_status' in data and data["text"].startswith("RT @"): 
                    continue
                    
                for _ht in data["hashtags"]:
                    _ht = _ht["text"].lower()
                    for _label, set_hts in classified_hts.items():
                        if _ht in set_hts:
                            label = _label
                            label_bingo_times += 1
                            
                # one tweet (in traindata) should have 0 or 1 class hashtag
                if label and label_bingo_times == 1:
                    text = data["text"].replace("\n", " ").replace("\t", " ")
                    f.write(label + "\t" + text + "\n")


            
                
