# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    analyze_hashtag.py                                 :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Zhenkun <zhenkun91@outlook.com>            +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2020/01/21 09:47:55 by Kay Zhou          #+#    #+#              #
#    Updated: 2020/10/19 09:45:35 by Zhenkun          ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

from my_weapon import *
from pathlib import Path
from collections import Counter
from tqdm import tqdm


def repetition_verify():
    """
    确定每月没有重复数据
    """
    months = ["202008", "202007", "202006", "202005", "202004", "202003", "202002", "202001"]
    for month in months:
        set_tweetid = set()
        print(month)
        in_name = f"D:\\US2020\\{month}.lj"
        for line in tqdm(open(in_name, encoding="utf8")):
            try:
                d = json.loads(line.strip())
            except Exception as e:
                print('json.loads() Error:', e)
                print('line ->', line)
                continue
            if d['id'] in set_tweetid:
                print(d['id'])
                continue
            set_tweetid.add(d['id'])
         

def read_raw_tweets_fromlj(_month="all"):
    """直接读取raw_data/month.lj文件
    Yields:
        [type]: [description]
    """
    if _month == "all":
        months = ["202008", "202007", "202006", "202005",
                  "202004", "202003", "202002", "202001"]
        for month in months:
            print(month)
            in_name = f"D:\\US2020\\{month}.lj"
            for line in open(in_name, encoding="utf8"):
                try:
                    d = json.loads(line.strip())
                except Exception as e:
                    print('json.loads() Error:', e)
                    print('line ->', line)
                    continue
                yield d

    else:
        print(_month)
        in_name = f"D:\\US2020\\{_month}.lj"
        for line in tqdm(open(in_name, encoding="utf8")):
            try:
                d = json.loads(line.strip())
            except Exception as e:
                print('json.loads Error:', e)
                print('line ->', line)
                continue
            yield d


def write_top_hashtags(out_name):
    """
    预计通过每月的hashtags作为入口进行分析
    """
    file_names = sorted(Path("D:\\US2020").rglob("*.lj"))

    all_hts = Counter()
    for in_name in file_names:
        print(in_name)
        month_hts = Counter()
        for line in tqdm(open(in_name, encoding="utf8")):
            hts = json.loads(line)["hashtags"]
            for ht in hts:
                all_hts[ht["text"].lower()] += 1
                month_hts[ht["text"].lower()] += 1
        with open(out_name + "_" + in_name.stem, "w", encoding="utf8") as f:
            for ht, cnt in month_hts.most_common(10000):
                print(ht, cnt, file=f)

    with open(out_name, "w", encoding="utf8") as f:
        for ht, cnt in all_hts.most_common(10000):
            print(ht, cnt, file=f)



def write_top_trump_biden_hashtags(out_name):
    all_hts = Counter()
    file_names = sorted(Path("raw_data").rglob("*.txt"), reverse=True)

    for in_name in file_names:
        name = in_name.stem.split("-")[-1].lower()
        if "trump" in name or "biden" in name:
            if in_name.parts[1] in months:
                print(in_name)
                for line in tqdm(open(in_name)):
                    try:
                        hts = json.loads(line)["hashtags"]
                    except:
                        pass
                    for ht in hts:
                        all_hts[ht["text"].lower()] += 1

    with open(out_name, "w") as f:
        for ht, cnt in all_hts.most_common(1000):
            f.write(f"{ht},{cnt}\n")


def write_cooccurrence_hashtags():
    # with open(out_name, "w") as f:
    #     for tweet in read_raw_tweets_fromlj(_month="all"):
    #         hts = tweet["hashtags"]
    #         if hts and len(hts) >= 1:
    #             f.write(" ".join([ht["text"].lower() for ht in hts]) + "\n")
    months = ["202010", "202009", "202008", "202007", "202006",
              "202005", "202004", "202003", "202002", "202001"]
    for month in months:
        with open(f"data/hashtags-coocurrence-{month}.txt", "w", encoding="utf8") as f:
            for tweet in read_raw_tweets_fromlj(_month=month):
                hts = tweet["hashtags"]
                if hts and len(hts) >= 1:
                    f.write(" ".join([ht["text"].lower() for ht in hts]) + "\n")
    
        

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
    # repetition_verify() # 已经确认没有重复
    # write_top_hashtags("data/hashtags-US2020-from-Jan-to-Oct.txt")
    write_cooccurrence_hashtags()