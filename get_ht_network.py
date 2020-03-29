# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    get_ht_network.py                                  :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Kay Zhou <zhenkun91@outlook.com>           +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2019/05/29 14:33:53 by Kay Zhou          #+#    #+#              #
#    Updated: 2020/03/29 18:18:37 by Kay Zhou         ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

from SQLite_handler import get_all_tweets_with_hashtags, get_session
from collections import Counter
import os
from pathlib import Path
import ujson as json
import networkx as nx
from tqdm import tqdm
import pandas as pd


def get_hts():
    focus_ht = set()
    for line in open("/twitter-analysis/model-2020-03-25/hashtags.txt"):
        w = line.strip().split()
        ht, label = w[0], w[1]
        if label != "UNK":
            focus_ht.add(ht)
    print(len(focus_ht))
    return focus_ht


focus_ht = get_hts()
Ntweets = 0
only_focus_count = Counter()
hts_count = Counter()

for line in tqdm(open("data/hashtags-co-trump-20200324.txt")):
    _hts = [_ht for _ht in line.strip().split() if _ht in focus_ht]
    if len(_hts) > 0:
        Ntweets += 1
        for i in range(len(_hts)):
            hts_count[_hts[i]] += 1
            for j in range(i+1, len(_hts)):
                n1, n2 = _hts[i], _hts[j]
                if n1 > n2:
                    n1, n2 = n2, n1
                only_focus_count[(n1, n2)] += 1

G = nx.Graph()

for e in only_focus_count:
    w = only_focus_count[e]
    G.add_edge(*e, weight=w)

for n in G.nodes():
    G.nodes[n]["num"] = hts_count[n]
G.graph["Ntweets"] = Ntweets

print(G.number_of_nodes(), G.number_of_edges())
nx.write_gpickle(G, "data/hts_20200329.gpickle")
