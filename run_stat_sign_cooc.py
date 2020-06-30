# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    run_stat_sign_cooc.py                              :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Kay Zhou <zhenkun91@outlook.com>           +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2019/06/07 20:30:31 by Kay Zhou          #+#    #+#              #
#    Updated: 2020/03/30 10:57:53 by Kay Zhou         ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

import os
import sys
from functools import partial

import networkx as nx
import numpy as np
# from joblib import Parallel, delayed
from tqdm import tqdm


def first_seq(N, n1, j):
    return 1 - n1 / (N-j)


def second_seq(N, n1, n2, k, j):
    return (n1-j) * (n2-j) / ((N-n2+k-j) * (k-j))


def first_product_array(N, n1, n2, k):
    # prod = np.ones(n2-k, dtype=np.float128)
    # for j in range(0, n2-k-1+1):
    #     prod[j] = first_seq(N,n1,j)
    # return prod
    # return np.array([first_seq(N, n1, j) for j in range(0, n2-k-1+1)], dtype=np.float128)
    return np.array([first_seq(N, n1, j) for j in range(0, n2-k-1+1)], dtype=np.float64)


def second_product_array(N, n1, n2, k):
    # prod = np.ones(k, dtype=np.float128)
    # for j in range(0, k-1+1):
    #     prod[j] = second_seq(N,n1,n2,k,j)
    # return prod
    # return np.array([second_seq(N, n1, n2, k, j) for j in range(0, k-1+1)], dtype=np.float128)
    return np.array([second_seq(N, n1, n2, k, j) for j in range(0, k-1+1)], dtype=np.float64)


def p_val_np(N, n1, n2, r):
    N, n1, n2, r = int(N), int(n1), int(n2), int(r)
    print(N, n1, n2, r)
    assert N >= n1 and n1 >= n2 and n2 >= r

    _sum = 0
    for k in range(r, n2 + 1):
        _sum += first_product_array(N, n1, n2, k).prod() * \
               second_product_array(N, n1, n2, k).prod()
    print("p-value:", _sum)
    # print(N, n1, n2, r)
    # TODO
    return _sum


def add_p_val_to_edges(G):

    N = G.graph["Ntweets"]
    # edge significance
    for e in tqdm(G.edges(data=True)):
        # print(str(i) + ' over ' + str(G.number_of_edges()))
        r = e[2]["weight"]

        # num of occurence of v1 and v2
        n2, n1 = sorted((G.nodes[e[0]]["num"], G.nodes[e[1]]["num"]))

        if r < 100:
            p_v = -1
        else:
            p_v = p_val_np(N, n1, n2, r)
            # print(p_v)

        # print(p_v)
        if p_v >= 0.01:
            p_v = -1
        if p_v > 0:
            p_v = - np.log10(p_v)
        print(p_v)
        G[e[0]][e[1]]["sign"] = p_v


def add_prop_to_edges(G):

    N = G.graph["Ntweets"]
    # edge significance
    for e in tqdm(G.edges(data=True)):
        # print(str(i) + ' over ' + str(G.number_of_edges()))
        r = e[2]["weight"]

        # num of occurence of v1 and v2
        n2, n1 = sorted((G.nodes[e[0]]["num"], G.nodes[e[1]]["num"]))
        prop = (r * r) / (n2 * n1) 

        G[e[0]][e[1]]["prop"] = prop
    return G


def remove_edges_by_prop(G):
    prop_list = [e[2]["prop"] for e in G.edges(data=True)]
    thre = sorted(prop_list)[int(len(prop_list) * 0.9)]
    print("threshold:", thre)

    G2 = nx.Graph(G)
    # edge significance
    for e in tqdm(G.edges(data=True)):
        if e[2]["prop"] < thre:
            G2.remove_edge(*e[:2])
    return G2


# def compute_significance(e):
#     r = e[2]["weight"]
#     p0 = 1e-4
#     N = G.graph["Ntweets"]
#     # num of occurence of v1 and v2
#     n2, n1 = sorted((G.nodes[e[0]]["num"], G.nodes[e[1]]["num"]))
#     print(N, n1, n2, r)
#     p_v = p_val_np(N, n1, n2, r)
#     if p_v == 0:
#         G[e[0]][e[1]]["sign"] = -1
#     else:
#         G[e[0]][e[1]]["sign"] = np.log10(p0 / p_v)


# def fast_add_p_val_to_edges(G, ncpu=6):
#     N = G.graph["Ntweets"]
#     Parallel(n_jobs=ncpu, verbose=1,
#              batch_size=1)(delayed(compute_significance)(e) for e in G.edges(data=True))


if __name__ == "__main__":
    # G = nx.read_gpickle("data/hts_20190611.gpickle")
    G = nx.read_gpickle("data/hts_20200301-20200625.gpickle")
    print(G.number_of_nodes(), G.number_of_edges())
    # G = nx.complete_graph(20)
    # for e in G.edges(data=True):
    #     e[2]["weight"] = 2

    largest_components = max(nx.connected_components(G), key=len)
    G = G.subgraph(largest_components)
    print(G.number_of_nodes(), G.number_of_edges())
    # G = cal_G(G)

    G = add_prop_to_edges(G)
    # add_p_val_to_edges(G)

    # should_be_removed = []
    # for e in G.edges(data=True):
    #     if e[2]["sign"] < 0:
    #         should_be_removed.append((e[0], e[1]))

    # G = nx.Graph(G)
    # for e in should_be_removed:
    #     G.remove_edge(e[0], e[1])

    # largest_components = max(nx.connected_components(G), key=len)
    # G = G.subgraph(largest_components)

    # nx.write_gpickle(G, "data/hts_20200301-20200625.sig.gpickle")
    nx.write_gml(G, "data/hts_20200301-20200625.sig.gml")
