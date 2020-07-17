# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    get_ht_network.py                                  :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Kay Zhou <zhenkun91@outlook.com>           +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2019/05/29 14:33:53 by Kay Zhou          #+#    #+#              #
#    Updated: 2020/03/30 10:59:47 by Kay Zhou         ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

import os
from collections import Counter
from pathlib import Path

import networkx as nx
import pandas as pd
import ujson as json
from tqdm import tqdm

import run_stat_sign_cooc


def get_hts():
    focus_ht = {}
    for line in open("data/train-07/hashtags.txt"):
        w = line.strip().split()
        ht, label = w[0], w[1]
        if label in ["JB", "DT"]:
        # if label in ["JB", "DT", "DP", "UNK"]:
            focus_ht[ht] = label
    print(len(focus_ht))
    return focus_ht


if __name__ == "__main__":
    focus_ht = get_hts()
    Ntweets = 0
    only_focus_count = Counter()
    hts_count = Counter()

    for line in tqdm(open("data/hashtags-co-20200301-20200625.txt", encoding="utf8")):
        _hts = list(set([_ht for _ht in line.strip().split() if _ht in focus_ht]))
        if len(_hts) > 1:
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
        # if focus_ht[n] not in ["DP", "DT", "JB", "UNK"]:
        #     focus_ht[n] = "DP"
        G.nodes[n]["camp"] = focus_ht[n]
    G.graph["Ntweets"] = Ntweets

    print(G.number_of_nodes(), G.number_of_edges())
    nx.write_gpickle(G, "data/hts_202003-202007.gpickle")

    largest_components = max(nx.connected_components(G), key=len)
    G = G.subgraph(largest_components)
    print(G.number_of_nodes(), G.number_of_edges())

    G = run_stat_sign_cooc.add_prop_to_edges(G)
    G = run_stat_sign_cooc.remove_edges_by_prop(G)
    nx.write_gml(G, "data/hts_202003-202007.sig.gml")
