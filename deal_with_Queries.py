# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    deal_with_Queries.py                               :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Kay Zhou <zhenkun91@outlook.com>           +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2019/08/30 17:11:45 by Kay Zhou          #+#    #+#              #
#    Updated: 2019/09/18 21:16:05 by Kay Zhou         ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

from my_weapon import *
import unicodedata
import os
import orjson as json


def normalize_lower(text):
    return unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode().lower()


def load_queries():
    queries_keep = [line.strip().lower() for line in open("data/query_keep.txt")]
    queries_ignore = [line.strip().lower() for line in open("data/query_ignore.txt")]
    return queries_keep, queries_ignore


def deal_the_OR_trouble():
    queries_keep, queries_ignore = load_queries()
    queries_keep, queries_ignore = set(queries_keep), set(queries_ignore)
    
    target_dir = ["201902", "201903", "201904", "201905", "201906", "201907"]
    
    keep_ids = set()
    ignore_ids = set()
    dontKnow_ids = set()
        
    for _dir in target_dir:    
        for in_name in tqdm(os.listdir("disk/" + _dir)):
            words = in_name[7: -4]
            words = [w.strip().lower() for w in words.split("OR")]
            maybe_ignore = False
            keep_this_file = False
            
            #该文件若包括ignore的关键词则考虑分析
            #否则完全不分析ignore的文件，或是完全分析在keep的文件
            if len(words) == 1:
                if words[0] in queries_keep:
                    keep_this_file = True
                    
                if keep_this_file:
                    for line in tqdm(open("disk/" + _dir + "/" + in_name)):
                        keep_ids.add(json.loads(line.strip())["id"])
                    
            else:
                _keep_words = []
                _igno_words = []
                maybe_ignore = False
                keep_this_file = True
                for w in words:
                    if w in queries_ignore:
                        maybe_ignore = True
                        keep_this_file = False
                        _igno_words.append(w)
                    if w in queries_keep:
                        _keep_words.append(w)
                        
                if keep_this_file:
                    for line in tqdm(open("disk/" + _dir + "/" + in_name)):
                        keep_ids.add(json.loads(line.strip())["id"])
                        
                elif maybe_ignore:
                    for line in tqdm(open("disk/" + _dir + "/" + in_name)):
                        d = json.loads(line.strip())
                        context = d["text"]
                        context += d["user"]["screen_name"]
                        if "name" in d["user"]:
                            context += d["user"]["name"]
                        if "user_mentions" in d:
                            for men in d["user_mentions"]:
                                context += men["screen_name"]
                        context = normalize_lower(context)
                        
                        igno_bingo = False
                        keep_bingo = False

                        for w in _keep_words:
                            if w in context:
                                keep_bingo = True
                                break
                        
                        if not keep_bingo:
                            for w in _igno_words:
                                if w in context:
                                    igno_bingo = True
                                
                        if keep_bingo:
                            keep_ids.add(d["id"])
                        elif igno_bingo:
                            ignore_ids.add(d["id"])
                        else:
                            dontKnow_ids.add(d["id"])
                            
        real_ignore_ids1 = ignore_ids - keep_ids
        real_ignore_ids2 = (ignore_ids | dontKnow_ids) - keep_ids
        
        with open(f"disk/data/02-07-ignore-1.txt", "w") as f:
            for _id in real_ignore_ids1:
                f.write(str(_id) + "\n")
        with open(f"disk/data/02-07-ignore-2.txt", "w") as f:
            for _id in real_ignore_ids2:
                f.write(str(_id) + "\n")


def split_obscure_files():
    """
    Cristina OR Kirchner OR Macri OR elecciones OR CFK OR CFKArgentina.txt
    mauriciomacri OR PASO OR macrismo OR kirchnerismo OR peronismo.txt
    """
    queries_keep, queries_ignore = load_queries()
    queries_keep, queries_ignore = set(queries_keep), set(queries_ignore)
    
    target_dir = ["201902", "201903", "201904", "201905", "201906", "201907"]
        
    for _dir in target_dir:    
        for in_name in tqdm(os.listdir("disk/" + _dir)):
            words = in_name[7: -4]
            words = [w.strip() for w in words.split("OR")]
            maybe_ignore = False

            if len(words) > 1:
                print(words)
                _keep_words = []
                _igno_words = []
                maybe_ignore1 = False
                maybe_ignore2 = False
                for w in words:
                    w_low = w.lower()
                    if w_low in queries_ignore:
                        maybe_ignore1 = True
                    if w_low in queries_keep:
                        maybe_ignore2 = True
                
                bingo_tweet_id = {}
                if maybe_ignore1 & maybe_ignore2:
                    print("disk/" + _dir + "/" + in_name)
                    
                    for line in tqdm(open("disk/" + _dir + "/" + in_name)):
                        d = json.loads(line.strip())
                        context = d["text"]
                        context += d["user"]["screen_name"]
                        if "name" in d["user"]:
                            context += d["user"]["name"]
                        if "user_mentions" in d:
                            for men in d["user_mentions"]:
                                context += men["screen_name"]
                        context = normalize_lower(context)

                        for w in words:
                            w_low = w.lower()
                            if w_low in context:
                                if w not in bingo_tweet_id:
                                    bingo_tweet_id[w] = [d["id_str"]]
                                else:
                                    bingo_tweet_id[w].append(d["id_str"])
                                
                    for w, tids in bingo_tweet_id.items():
                        with open(f"disk/temp/{_dir}-{w}.txt", "a") as f:
                            for _id in tids:
                                f.write(_id + "\n")
                                

def split_obscure_files_v2():
    """
    Cristina OR Kirchner OR Macri OR elecciones OR CFK OR CFKArgentina.txt
    mauriciomacri OR PASO OR macrismo OR kirchnerismo OR peronismo.txt
    """
    queries_keep, queries_ignore = load_queries()
    queries_keep, queries_ignore = set(queries_keep), set(queries_ignore)
    
    # target_dir = ["201902", "201903", "201904", "201905", "201906", "201907"]
    target_dir = ["201904", "201905", "201906", "201907"]
        
    for _dir in target_dir:    
        for in_name in os.listdir("disk/" + _dir):
            words = in_name[7: -4]
            words = [w.strip() for w in words.split("OR")]
            maybe_ignore = False

            if len(words) > 1:
                print(words)
                _keep_words = []
                _igno_words = []
                maybe_ignore1 = False
                maybe_ignore2 = False
                for w in words:
                    w_low = w.lower()
                    if w_low in queries_ignore:
                        maybe_ignore1 = True
                    if w_low in queries_keep:
                        maybe_ignore2 = True
                
                bingo_tweet_id = {}
                if maybe_ignore1 & maybe_ignore2:
                    print("disk/" + _dir + "/" + in_name)
                    for w in words:
                        with open(f"disk/temp/{_dir}-{w}-v2.txt", "w") as f:
                            tids = {line.strip() for line in open(f"disk/temp/{_dir}-{w}.txt")}
                            for line in tqdm(open("disk/" + _dir + "/" + in_name)):
                                d = json.loads(line.strip())
                                if d["id_str"] in tids:
                                    f.write(line)
 

class File_Checker(object):
    def __init__(self, *args):
        super(File_Checker, self).__init__(*args)
        self.queries_ignore = {line.strip().lower() for line in open("data/query_ignore.txt")}
        # print(self.queries_ignore)
        
    def ignore_it(self, in_name):
        
        if in_name.count("-") == 2:
            words = [w.strip().lower() for w in in_name.split("-")[1].split("OR")]
        else:
            words = [w.strip().lower() for w in in_name.split("-")[1][:-4].split("OR")]
            
        for w in words:
            if w in self.queries_ignore:
                # print("Ignored:", in_name)
                return True
        return False
    
                      
if __name__ == '__main__':
    # deal_the_OR_trouble()
    # split_obscure_files()
    # split_obscure_files_v2()
    
    target_dir = ["201902", "201903", "201904", "201905", "201906", "201907", "201908", "201909", "201910"]
    
    from deal_with_Queries import File_Checker
    checker = File_Checker()
    
    for _dir in target_dir:
        set_tweets = set()
        print(_dir)
        out_file = open(f"new_disk/raw_tweets/{_dir}.lj", "w")
        for in_name in os.listdir("disk/" + _dir):
            # ignore
            if checker.ignore_it(in_name):
                print("~Ignore:", in_name)
                continue

            in_name = "disk/" + _dir + "/" + in_name
            print(in_name)
            for line in open(in_name):
                d = json.loads(line.strip())
                tweet_id = d["id"]
                if tweet_id not in set_tweets:
                    set_tweets.add(tweet_id)
                    out_file.write(line)
                    
        
