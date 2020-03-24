# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    analyze_hashtag.py                                 :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Kay Zhou <zhenkun91@outlook.com>           +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2020/01/21 09:47:55 by Kay Zhou          #+#    #+#              #
#    Updated: 2020/03/24 17:17:03 by Kay Zhou         ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

from my_weapon import *
from pathlib import Path
from collections import Counter
from tqdm import tqdm


demo_files = set([
    "Joe Biden",
    "JoeBiden",
    "Bernie Sanders",
    "SenSanders",
    # "Michael Bennet",
    # "SenatorBennet",
    # "Mike Bloomberg",
    # "MikeBloomberg",
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
    # "Tom Steyer",
    # "TomSteyer",
    # "Elizabeth Warren",
    # "ewarren",
    # "Andrew Yang",
    # "AndrewYang",
])

trump_files = [
    "Donald Trump",
    "realDonaldTrump",
    "Joe Biden",
    "JoeBiden",
    "Bernie Sanders",
    "SenSanders",
]
 
months = set([
    "202003",
    "202002",
    "202001",
    "201912",
    "201911",
    "201910",
    "201909",
])


def write_top_hashtags(in_files, out_name):
    all_hts = Counter()
    file_names = sorted(Path("raw_data").rglob("*.txt"), reverse=True)

    for in_name in file_names:
        if in_name.stem.split("-")[-1] in in_files and in_name.parts[1] in months:
            print(in_name)
            for line in tqdm(open(in_name)):
                hts = json.loads(line)["hashtags"]
                for ht in hts:
                    all_hts[ht["text"].lower()] += 1


    with open(f"data/{out_name}", "w") as f:
        for ht, cnt in all_hts.most_common(1000):
            print(ht, cnt, file=f)


def write_cooccurrence_hashtags(in_files, out_name):
    all_hts = Counter()
    file_names = sorted(Path("raw_data").rglob("*.txt"), reverse=True)

    with open(f"data/{out_name}", "w") as f:
        for in_name in file_names:
            if in_name.stem.split("-")[-1] in in_files and in_name.parts[1] in months:
                print(in_name)
                for line in tqdm(open(in_name)):
                    hts = json.loads(line)["hashtags"]
                    f.write(" ".join([ht["text"].lower() for ht in hts]))
                    

def get_hts(in_name):
    hts = {}
    for line in open(in_name):
        if not line.startswith("#"):
            w = line.strip().split()
            if len(w) == 3:
                hts[w[1]] = w[0]
    print(hts)
    return hts


def label_based_on_before(in_name, out_name):
    hts = get_hts("data/hashtags-20200201_classified_hernan_Feb6.txt")
    with open(out_name, "w") as f:
        for line in open(in_name):
            w = line.strip().split()
            if w[0] in hts:
                f.write(f"{hts[w[0]]} {w[0]} {w[1]}\n")
            else:
                f.write(f"{w[0]} {w[1]}\n")
            

if __name__ == "__main__":
    # write_top_hashtags(demo_files, "hashtags-democrats-20200305.txt")
    # write_top_hashtags(trump_files, "hashtags-trump-20200318.txt")
    # label_based_on_before("data/hashtags-democrats-20200121.txt", "data/hashtags-democrats-20200121-v2.txt")
    # label_based_on_before("data/hashtags-trump-20200121.txt", "data/hashtags-trump-20200121-v2.txt")

    write_cooccurrence_hashtags(trump_files, "hashtags-co-trump-20200324.txt")