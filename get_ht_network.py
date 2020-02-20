# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    get_ht_network.py                                  :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Kay Zhou <kayzhou.mail@gmail.com>          +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2019/05/29 14:33:53 by Kay Zhou          #+#    #+#              #
#    Updated: 2019/08/08 11:29:33 by Kay Zhou         ###   ########.fr        #
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
    K_ht = set()
    M_ht = set()
    A_ht = set()
    focus_ht = set()

    for line in open("data/hashtags/2019-08-08.txt"):
        w = line.strip().split()
        ht = w[1]
        focus_ht.add(ht)
        if w[0] == "K":
            K_ht.add(ht)
        elif w[0] == "M":
            M_ht.add(ht)
        elif w[0] == "L":
            A_ht.add(ht)
    focus_ht = K_ht | M_ht | A_ht
    print(len(K_ht), len(M_ht), len(A_ht), len(focus_ht))
    return K_ht, M_ht, A_ht, focus_ht


K_ht, M_ht, A_ht, focus_ht = get_hts()
Ntweets = 0
only_focus_count = Counter()
hts_count = Counter()

sess = get_session()
tweets = get_all_tweets_with_hashtags(sess)
for t in tqdm(tweets):
    _hts = [_ht for _ht in t[1].split(",") if _ht in focus_ht]
    if len(_hts) > 0:
        Ntweets += 1
        for i in range(len(_hts)):
            hts_count[_hts[i]] += 1
            for j in range(i+1, len(_hts)):
                n1, n2 = _hts[i], _hts[j]
                if n1 > n2:
                    n1, n2 = n2, n1
                only_focus_count[(n1, n2)] += 1

sess.close()
G = nx.Graph()

for e in only_focus_count:
    w = only_focus_count[e]
    G.add_edge(*e, weight=w)

for n in G.node():
    G.node[n]["num"] = hts_count[n]
G.graph["Ntweets"] = Ntweets

print(G.number_of_nodes(), G.number_of_edges())
nx.write_gpickle(G, "data/hts_20190808.gpickle")
