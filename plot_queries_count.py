#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Feb 11 16:47:03 2020

@author: Carles
"""

import pandas as pd
from pandas import read_csv
import numpy as np
import matplotlib.pyplot as plt


fig = plt.figure(figsize=(15.0, 8.3)) # in inches!

df = read_csv("data/queries_count.csv")
xs = np.array(df[df.columns[0]])
ys = np.array(df[df.columns[1::1]])

for i in range(1,df.shape[1]):
    plt.plot(xs, ys[:,i-1], label=df.columns[i]) 

plt.title('Tweets count per candidate')
plt.xlabel('Days')
plt.ylabel('Number of tweets')
plt.xticks(rotation=70)
plt.legend(loc='best', title='Series')
plt.savefig("data/counting_plot.png", bbox_inches='tight')