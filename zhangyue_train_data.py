# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    zhangyue_train_data.py                             :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Zhenkun <zhenkun91@outlook.com>            +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2022/04/05 09:48:21 by Zhenkun           #+#    #+#              #
#    Updated: 2022/04/05 11:45:32 by Zhenkun          ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

names = [line.strip() for line in open("data/names.txt")]
# with open("data/zhangyue_推文标注_bingo.txt", "w") as f:
#     for line in open("data/zhangyue_推文标注.txt"):
#         words = line.strip().split("\t")
#         init_label = words[0]
#         new_label_1 = words[-2]
#         new_label_2 = words[-1]
#         if init_label == new_label_1:
#             # print(new_label_1)
#             f.write(new_label_1 + "\t" + words[1] + "\n")
    
# from collections import Counter
# import random

# DT_text = []
# JB_text = []

# cnt = Counter()

# for line in open("data/zhangyue_推文标注_bingo.txt"):
#     label, text = line.strip().split("\t")
#     if label == "DT":
#         DT_text.append(text)
#     elif label == "JB":
#         JB_text.append(text)
        
# with open("data/DT_random_4800.txt", "w") as f:   
#     texts = random.sample(DT_text, 48 * 100)
#     for t in texts:
#         f.write(t + "\n")

# with open("data/JB_random_4800.txt", "w") as f:   
#     texts = random.sample(JB_text, 48 * 100)
#     for t in texts:
#         f.write(t + "\n")
    
import pandas as pd
from torch import le

DT_texts = [line.strip() for line in open("data/DT_random_4800.txt")]
JB_texts = [line.strip() for line in open("data/JB_random_4800.txt")]

for i, n in zip(range(48), names):
    labels = []
    texts = []
    for j in range(200):
        if j < 50:
            labels.append("DT")
            texts.append(DT_texts[i * 50 + j])
        else:
            labels.append("JB")
            texts.append(JB_texts[i * 50 + j - 100])
    
    print(len(labels))
    print(len(texts))
    
    pd.DataFrame(
        {
            "pre label": labels,
            "new label": [None] * 200,
            "text": texts
        }
    ).to_excel(f"data/选举观点标注数据集/{i+1}-{n}.xlsx")
    
    