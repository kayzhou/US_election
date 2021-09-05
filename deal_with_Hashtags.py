# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    deal_with_Hashtags.py                              :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Kay Zhou <kayzhou.mail@gmail.com>          +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2019/09/05 14:12:04 by Kay Zhou          #+#    #+#              #
#    Updated: 2019/09/05 14:29:18 by Kay Zhou         ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

# web端label后的数据添加到新的文件中
import json


def get_hashtags(in_name):
    rst = {}
    for line in open("data/hashtags/" + in_name):
        camp, ht = line.strip().split()
        rst[ht] = camp
    return rst


def add_hashtags(in_name, new_name, out_name):
    rst_hts = get_hashtags(in_name)
    print("Before added", len(rst_hts))
    new_hts = json.load(open("web/data/submit/" + new_name))
    for new_ht in new_hts:
        ht, camp = new_ht
        if camp == "R":
            print(ht)
        else:
            rst_hts[ht] = camp
    
    print("After added", len(rst_hts))
    
    with open("data/hashtags/" + out_name, "w") as f:
        for ht, camp in rst_hts.items():
            f.write(f"{camp} {ht}\n")
            

if __name__ == '__main__':
    add_hashtags("2019-08-09.txt", "2019-08-24 14:03:49.json", "2019-09-05.txt")
    add_hashtags("2019-09-05.txt", "2019-08-27 15:41:57.json", "2019-09-05.txt")
    
            