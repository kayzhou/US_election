# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    make_csv_for_web.py                                :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Kay Zhou <kayzhou.mail@gmail.com>          +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2019/05/10 14:43:26 by Kay Zhou          #+#    #+#              #
#    Updated: 2019/09/05 10:30:03 by Kay Zhou         ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

from my_weapon import *
from SQLite_handler import *
import math


def make_main_cumulative_plot():
    rsts = get_db_prediction_results(state="USA")
    print(rsts)
    data = [
        {
            "dt": r.dt,
            "Joe Biden": r["Biden"] / (r["Biden"] + r["Trump"]),
            "Donald Trump": r["Trump"] / (r["Biden"] + r["Trump"])
        } for r in rsts
    ]
    data = pd.DataFrame(data).set_index("dt")
    data = data.round(3)
    data = data.sort_index()
    data = data.dropna(how='all')
    print("save:", f"web/data/p1.csv")
    data.to_csv(f"web/data/p1.csv")
    

def make_main_plot_v2(last, now=None):
    last_data = pd.read_csv(f"web/data/{last}/p1-h.csv").set_index("dt")
    # print(last_data)
    _d = last_data[["Cristina (AI)", "Macri (AI)"]]
    last_dt = _d.dropna().tail(1).index[0]
    # print(last_dt, last_data)

    all_data = []
    # wiki_data = pd.read_csv("data/wiki-updated-0503.csv").set_index("dt")
    # wiki_data = wiki_data[wiki_data.index > last_dt]
    # wiki_data["Cristina (Aggregate Polls)"] = wiki_data["K"] / (wiki_data["K"] + wiki_data["M"])
    # wiki_data["Macri (Aggregate Polls)"] = wiki_data["M"] / (wiki_data["K"] + wiki_data["M"])
    # wiki_data = wiki_data.round(3)

    # day_wiki_data = wiki_data.reset_index().pivot_table(columns=["dt"]).transpose()
    # day_wiki_data[day_wiki_data.index > last_data]
    # print(day_wiki_data)

    # all_data.append(day_wiki_data)
    
    to_show_cols = ["Cristina (AI)", "Macri (AI)",
                    "Cristina (Aggregate Polls)", "Macri (Aggregate Polls)"]
    to_show_cols2 = ["Cristina (AI)", "Macri (AI)", "Cristina (Elypsis)", "Macri (Elypsis)"]
    to_show_cols1 = list(last_data.columns)
    to_show_cols1.remove("Cristina (Aggregate Polls)")
    to_show_cols1.remove("Macri (Aggregate Polls)")
    
    # for _name in trusted_name:
    #     _d = wiki_data[wiki_data.name == _name].copy()
    #     _d.loc[:, f"Cristina ({_name})"] = _d["Cristina (Aggregate Polls)"]
    #     _d.loc[:, "Cristina (Aggregate Polls)"] = None
    #     _d.loc[:, f"Macri ({_name})"] = _d["Macri (Aggregate Polls)"]
    #     _d.loc[:, "Macri (Aggregate Polls)"] = None
    #     if len(_d) > 0:
    #         all_data.append(_d)
    #         to_show_cols.append(f"Cristina ({_name})")
    #         to_show_cols.append(f"Macri ({_name})")
    #         to_show_cols1.append(f"Cristina ({_name})")
    #         to_show_cols1.append(f"Macri ({_name})")

    pred_rst = []
    sess = get_session()
    for r in sess.query(Percent):
        _dt = r.dt.strftime("%Y-%m-%d")
        pred_rst.append({
            "dt": _dt,
            "Cristina (AI)": r.K,
            "Macri (AI)": r.M,
        })

    pred = pd.DataFrame(pred_rst)
    all_data.append(pred)
    data = pd.concat(all_data, sort=True)
    data = data.set_index("dt")
    # print(data)
    
    if now is None:
        now = pendulum.now().to_date_string()
    
    print("last dt:", last_dt)
    data = data[data.index > last_dt]
    data = last_data.append(data, sort=False)
    # data = data[to_show_cols]
    data = data.sort_index()
    data = data.round(3)
    # print(data)

    data = data.dropna(how='all')
    print("save:", f"web/data/{now}/p1-h.csv")
    data.to_csv(f"web/data/{now}/p1-h.csv")

    data = data[data.index >= "2019-03-01"]
    print("save:", f"web/data/{now}/p1.csv")
    data.to_csv(f"web/data/{now}/p1.csv")

    d2 = data[to_show_cols2]
    print("save:", f"web/data/{now}/p1-2.csv")
    d2.to_csv(f"web/data/{now}/p1-2.csv")

    print(to_show_cols1)
    d1 = data[to_show_cols1]
    print("save:", f"web/data/{now}/p1-3.csv")
    d1.to_csv(f"web/data/{now}/p1-3.csv")


    support_data = []
    for r in sess.query(Weekly_Predict):
        dt = r.dt.strftime("%Y-%m-%d")
        if dt[-5:] in set(["05-01", "05-02", "05-03", "05-04", "05-05", "05-06", "05-07",
            "05-08", "05-09", "05-10", "05-11", "05-12", "05-13", "05-14", "05-15", "05-16",
            "05-18", "05-19", "05-20", "05-21", "05-22", "05-23", "05-24"]):
            continue

        if dt < "2019-05-25":
            support_data.append({
                "dt": dt,
                "Cristina (AI)": int(r.U_Cristina / 3),
                "Macri (AI)": int(r.U_Macri / 3),
            })
        else:
            support_data.append({
                "dt": dt,
                "Cristina (AI)": r.U_Cristina,
                "Macri (AI)": r.U_Macri,
            })

    support_data = pd.DataFrame(support_data).set_index("dt")
    support_data = support_data[support_data.index >= "2019-03-01"]
    support_data = support_data.sort_index()
    
    # print("save:", f"web/data/{now}/p2.csv")
    # support_data.to_csv(f"web/data/{now}/p2.csv")


def make_main_plot_v3(last=None, now=None):

    to_show_cols = ["Fernandez (AI)", "Macri (AI)", "Fernandez (Aggregate Polls)", "Macri (Aggregate Polls)", "Fernandez (Elypsis)", "Macri (Elypsis)",
                     "Fernandez (Isonomia)", "Macri (Isonomia)", "Fernandez (Imagen y Gestión Política)", "Macri (Imagen y Gestión Política)",
                     "Fernandez (M&F)", "Macri (M&F)", "Fernandez (Opinaia)", "Macri (Opinaia)", "Fernandez (Giacobbe)", "Macri (Giacobbe)"]
    to_show_cols2 = ["Fernandez (AI)", "Macri (AI)", "Fernandez (Elypsis)", "Macri (Elypsis)"]

    to_show_cols1 = ["Fernandez (AI)", "Macri (AI)", "Fernandez (Aggregate Polls)", "Macri (Aggregate Polls)", "Fernandez (Elypsis)", "Macri (Elypsis)",
                     "Fernandez (Isonomia)", "Macri (Isonomia)", "Fernandez (Imagen y Gestión Política)", "Macri (Imagen y Gestión Política)",
                     "Fernandez (M&F)", "Macri (M&F)", "Fernandez (Opinaia)", "Macri (Opinaia)", "Fernandez (Giacobbe)", "Macri (Giacobbe)"]
    to_show_cols1.remove("Fernandez (Aggregate Polls)")
    to_show_cols1.remove("Macri (Aggregate Polls)")

    care = ["Elypsis", "Isonomia", "Imagen y Gestión Política", "M&F", "Opinaia", "Giacobbe"]
    
    all_data = []
    sess = get_session()
    poll_rst = []
    from collections import defaultdict
    aggregate_polls = defaultdict(list)
    for r in sess.query(Other_Poll):
        if r.name in care:
            poll_rst.append({
                "dt": r.dt,
                f"Fernandez ({r.name})": r.K / (r.K + r.M),
                f"Macri ({r.name})": r.M / (r.K + r.M),
            })
        if r.name != "Elypsis":
            aggregate_polls[r.dt].append({
                "dt": r.dt,
                "Fernandez (Aggregate Polls)": r.K / (r.K + r.M),
                "Macri (Aggregate Polls)": r.M / (r.K + r.M),
            })

    for k, v in aggregate_polls.items():
        if len(v) == 1:
            poll_rst.append(v[0])
        else:
            _K = 0
            _M = 0
            for _v in v:
                _K += _v["Fernandez (Aggregate Polls)"]
                _M += _v["Macri (Aggregate Polls)"]
            _K /= len(v)
            _M /= len(v)

            poll_rst.append({
                "dt": k,
                "Fernandez (Aggregate Polls)": _K,
                "Macri (Aggregate Polls)": _M,
            })

    all_data.append(pd.DataFrame(poll_rst))
    
    pred_rst = []
    for r in sess.query(Percent):
        _dt = r.dt.strftime("%Y-%m-%d")
        pred_rst.append({
            "dt": _dt,
            "Fernandez (AI)": r.K,
            "Macri (AI)": r.M,
        })
    all_data.append(pd.DataFrame(pred_rst))
    data = pd.concat(all_data, sort=True)
    data = data.set_index("dt")
    data = data[to_show_cols]
    
    data = data.sort_index()
    data = data.round(3)
    data = data.dropna(how='all')

    if last:
        last_data = pd.read_csv(f"web/data/{last}/p1-h.csv").set_index("dt")
        _d = last_data[["Fernandez (AI)", "Macri (AI)"]]
        last_dt = _d.dropna().tail(1).index[0]
        print("last dt:", last_dt)
        data = data[data.index > last_dt]
        data = last_data.append(data, sort=False)
        data = data.sort_index()
        data = data.round(3)

    print("save:", f"web/data/{now}/p1-h.csv")
    data.to_csv(f"web/data/{now}/p1-h.csv")

    data = data[data.index >= "2019-03-01"]
    print("save:", f"web/data/{now}/p1.csv")
    data.to_csv(f"web/data/{now}/p1.csv")

    d2 = data[to_show_cols2]
    d2 = d2.dropna(how='all')
    print("save:", f"web/data/{now}/p1-2.csv")
    d2.to_csv(f"web/data/{now}/p1-2.csv")

    # print(to_show_cols1)
    d1 = data[to_show_cols1]
    print("save:", f"web/data/{now}/p1-3.csv")
    d1.to_csv(f"web/data/{now}/p1-3.csv")


    support_data = []
    for r in sess.query(Weekly_Predict):
        dt = r.dt.strftime("%Y-%m-%d")
        if "2019-05-01" <= dt <= "2019-05-24":
            continue
        if dt < "2019-05-25":
            support_data.append({
                "dt": dt,
                "Fernandez (AI)": int(r.U_Cristina / 3),
                "Macri (AI)": int(r.U_Macri / 3),
            })
        else:
            support_data.append({
                "dt": dt,
                "Fernandez (AI)": r.U_Cristina,
                "Macri (AI)": r.U_Macri,
            })

    support_data = pd.DataFrame(support_data).set_index("dt")
    support_data = support_data[support_data.index >= "2019-08-12"]
    support_data = support_data.sort_index()
    
    print("save:", f"web/data/{now}/p2.csv")
    support_data.to_csv(f"web/data/{now}/p2.csv")


def make_fitting_plot(now=None):

    # Aggregate
    data = pd.read_csv("web/data/2019-05-14/p1.csv")
    data = data[["dt", "Fernandez (AI)", "Macri (AI)", "Fernandez (Aggregate Polls)", "Macri (Aggregate Polls)"]]
    data = data.dropna(how="all")

    for i, row in data.iterrows():
        if not math.isnan(row["Macri (Aggregate Polls)"]):
            data.loc[i, "dt"] = pendulum.parse(data.loc[i, "dt"]).add(days=-1).to_date_string()

    data = data.set_index("dt")
    data.index = pd.to_datetime(data.index)

    # fitting 07-20
    # M = pd.DataFrame(index=pd.date_range(start='3/1/2019', end='7/1/2019'))
    data["Macri (AI)"] = data["Macri (AI)"] * 0.6983851034507896 + 0.15303884489382164
    data["Fernandez (AI)"] = 1 - data["Macri (AI)"]

    data = data[["Fernandez (AI)", "Macri (AI)", "Fernandez (Aggregate Polls)", "Macri (Aggregate Polls)"]]
    data = data.round(3)
    
    print("save:", f"web/data/{now}/p1-f.csv")
    data.to_csv(f"web/data/{now}/p1-f.csv")

    # Postive
    data = pd.read_csv("web/data/2019-05-14/positive.csv")
    for i, row in data.iterrows():
        if not math.isnan(row["Macri (Elypsis)"]):
            data.loc[i, "dt"] = pendulum.parse(data.loc[i, "dt"]).add(days=-5).to_date_string()

    data = data.set_index("dt")
    data.index = pd.to_datetime(data.index)
    data = data.dropna(how="all")

    # fitting 08-04
    # M = pd.DataFrame(index=pd.date_range(start='3/1/2019', end='7/29/2019'))
    data["Macri (AI)"] = data["Macri (AI)"] * 1.0266156670377462 - 0.022566497940193053
    data["Fernandez (AI)"] = 1 - data["Macri (AI)"]

    data = data.round(3)
    print("save:", f"web/data/{now}/p1-f-positive.csv")
    data.to_csv(f"web/data/{now}/p1-f-positive.csv")


    # Invention
    data = pd.read_csv("web/data/2019-05-14/Ely-weekly.csv")
    for i, row in data.iterrows():
        if not math.isnan(row["Macri (Elypsis)"]):
            data.loc[i, "dt"] = pendulum.parse(data.loc[i, "dt"]).add(days=-21).to_date_string()

    data = data.set_index("dt")
    data.index = pd.to_datetime(data.index)
    data = data.dropna(how="all")

    # fitting 08-04
    # M = pd.DataFrame(index=pd.date_range(start='3/1/2019', end='7/29/2019'))
    data["Macri (AI)"] = data["Macri (AI)"] * (-0.25670139581719714) + 0.6129199344921367
    data["Fernandez (AI)"] = 1 - data["Macri (AI)"]

    data = data.round(3)
    data = data[data.index >= "2019-03-01"]
    print("save:", f"web/data/{now}/p1-f-invention.csv")
    data.to_csv(f"web/data/{now}/p1-f-invention.csv")

    
        
def make_history_predict(now):
    sess = get_session()
    pred_rst = []
    for r in sess.query(History_Predict):
        # print(r)
        _dt = r.dt.strftime("%Y-%m-%d")
        K = r.U_Cristina
        M = r.U_Macri
        U = r.U_unclassified + r.U_irrelevant
        pred_rst.append({
            "dt": _dt,
            "Fernandez (AI)": K / (K + M + U) * 100,
            "Macri (AI)": M / (K + M + U) * 100,
            "Others (AI)": U / (K + M + U) * 100,
        })
    data = pd.DataFrame(pred_rst).set_index("dt")
    data["Fernandez (PASO)"] = 47.65 # 49.49
    data["Macri (PASO)"] = 32.08 # 32.94
    data["Others (PASO)"] = 20.27 # 17.57
    data = data.round(2)
    
    print("save:", f"web/data/{now}/p-his.csv")
    data.to_csv(f"web/data/{now}/p-his.csv")


def make_main_plot_Elypsis(last, now=None):
    # last_data = pd.read_csv(f"web/data/{last}/positive-all.csv").set_index("dt")
    # print(last_data)
    # _d = last_data[["Fernandez (AI)", "Macri (AI)"]]
    # last_dt = _d.dropna().tail(1).index[0]
    # print(last_dt, last_data)

    positive_data = pd.read_csv("data/E-positive.csv")
    # 推后7天
    for i, row in positive_data.iterrows():
        positive_data.loc[i, "dt"] = pendulum.parse(positive_data.loc[i, "dt"]).add(days=7).to_date_string()
        # print(positive_data.loc[i, "dt"])

    positive_data = positive_data.set_index("dt")
    _sum = positive_data["Cristina (Elypsis)"] + positive_data["Macri (Elypsis)"]
    positive_data["Macri (Elypsis)"] = positive_data["Macri (Elypsis)"] / _sum
    positive_data["Fernandez (Elypsis)"] = positive_data["Cristina (Elypsis)"] / _sum

    # _A = [0.422, 0.243]
    _A = [1, 0]
    pred_rst = []
    sess = get_session()
    for r in sess.query(Percent):
        _dt = r.dt.strftime("%Y-%m-%d")
        _M = r.M * _A[0] + _A[1]
        _K = 1 - _M
        pred_rst.append({
            "dt": _dt,
            "Fernandez (AI)": _K,
            "Macri (AI)": _M,
        })
    sess.close()

    data = pd.DataFrame(pred_rst).set_index("dt")
    # print(data)
    
    if now is None:
        now = pendulum.now().to_date_string()
    
    # print("last dt:", last_dt)
    # data = data[data.index > last_dt]
    # data = last_data.append(data, sort=False)
    data = data.append(positive_data, sort=False)
    data = data[["Fernandez (AI)", "Macri (AI)", "Fernandez (Elypsis)", "Macri (Elypsis)"]]
    data = data.sort_index()
    data = data.round(3)
    data = data.dropna(how='all')

    print("save:", f"web/data/{now}/positive-all.csv")
    data.to_csv(f"web/data/{now}/positive-all.csv")

    data = data[data.index >= "2019-03-01"]
    print("save:", f"web/data/{now}/positive.csv")
    data.to_csv(f"web/data/{now}/positive.csv")

    # ---------------------------------- raw

    positive_data = pd.read_csv("data/E-positive.csv")
    for i, row in positive_data.iterrows():
        positive_data.loc[i, "dt"] = pendulum.parse(positive_data.loc[i, "dt"]).add(days=7).to_date_string()
        # print(positive_data.loc[i, "dt"])

    positive_data["Fernandez (Elypsis)"] = positive_data["Cristina (Elypsis)"]

    pred_rst = []
    sess = get_session()
    for r in sess.query(Percent):
        _dt = r.dt.strftime("%Y-%m-%d")
        pred_rst.append({
            "dt": _dt,
            "Fernandez (AI)": r.K,
            "Macri (AI)": r.M,
        })
    sess.close()


    positive_data = positive_data.set_index("dt")
    data = pd.DataFrame(pred_rst).set_index("dt")
    # print(data)
    
    if now is None:
        now = pendulum.now().to_date_string()
    

    data = data.append(positive_data, sort=False)
    data = data[["Fernandez (AI)", "Macri (AI)", "Fernandez (Elypsis)", "Macri (Elypsis)"]]
    data = data.sort_index()
    data = data.round(3)
    data = data.dropna(how='all')

    print("save:", f"web/data/{now}/positive-raw-all.csv")
    data.to_csv(f"web/data/{now}/positive-raw-all.csv")

    data = data[data.index >= "2019-03-01"]
    print("save:", f"web/data/{now}/positive-raw.csv")
    data.to_csv(f"web/data/{now}/positive-raw.csv")


    # ------------------------------------ voting
    voting_data = pd.read_csv("data/E-voting.csv")
    for i, row in voting_data.iterrows():
        voting_data.loc[i, "dt"] = pendulum.parse(voting_data.loc[i, "dt"]).add(days=7).to_date_string()
        # print(positive_data.loc[i, "dt"])

    voting_data = voting_data.set_index("dt")
    _sum = voting_data["Cristina (Elypsis)"] + voting_data["Macri (Elypsis)"]
    voting_data["Fernandez (Elypsis)"] = voting_data["Cristina (Elypsis)"] / _sum
    voting_data["Macri (Elypsis)"] = voting_data["Macri (Elypsis)"] / _sum

    data = pd.DataFrame(pred_rst).set_index("dt")
    data = data.append(voting_data, sort=False)
    data = data.sort_index()
    data = data.round(3)
    data = data[["Fernandez (AI)", "Macri (AI)", "Fernandez (Elypsis)", "Macri (Elypsis)"]]
    data = data.dropna(how='all')

    print("save:", f"web/data/{now}/Ely-weekly-all.csv")
    data.to_csv(f"web/data/{now}/Ely-weekly-all.csv")

    data = data[data.index >= "2019-03-01"]
    print("save:", f"web/data/{now}/Ely-weekly.csv")
    data.to_csv(f"web/data/{now}/Ely-weekly.csv")

    # ------------------ 3 classes
    voting_data = pd.read_csv("data/E-voting.csv")
    for i, row in voting_data.iterrows():
        voting_data.loc[i, "dt"] = pendulum.parse(voting_data.loc[i, "dt"]).add(days=7).to_date_string()
        # print(positive_data.loc[i, "dt"])

    voting_data = voting_data.set_index("dt")
    voting_data["Fernandez (Elypsis)"] = voting_data["Cristina (Elypsis)"]

    pred_rst = []
    sess = get_session()
    for r in sess.query(Percent):
        _dt = r.dt.strftime("%Y-%m-%d")
        _d = get_percent(sess, _dt, clas=3)
        pred_rst.append({
            "dt": _dt,
            "Fernandez (AI)": _d[0],
            "Macri (AI)": _d[1],
            "Undecided (AI)": _d[2],
        })
    sess.close()
        
    data = pd.DataFrame(pred_rst).set_index("dt")
    data = data.append(voting_data, sort=False)
    data = data.sort_index()
    data = data.round(3)
    data = data[["Fernandez (AI)", "Macri (AI)", "Fernandez (Elypsis)", "Macri (Elypsis)", "Undecided (AI)", "Undecided (Elypsis)"]]
    data = data.dropna(how='all')

    print("save:", f"web/data/{now}/Ely3-weekly-all.csv")
    data.to_csv(f"web/data/{now}/Ely3-weekly-all.csv")

    data = data[data.index >= "2019-03-01"]
    print("save:", f"web/data/{now}/Ely3-weekly.csv")
    data.to_csv(f"web/data/{now}/Ely3-weekly.csv")


# def make_main_plot_voting(last, now=None):

#     last_data = pd.read_csv(f"web/data/{last}/Ely3-weekly-all.csv").set_index("dt")
#     last_data = last_data[["Cristina (Elypsis)", "Macri (Elypsis)"]]
#     last_data = last_data.dropna()
#     # last_dt = last_data.tail(1).index[0]
#     _sum = last_data["Cristina (Elypsis)"] + last_data["Macri (Elypsis)"] 
#     last_data["Cristina (Elypsis)"] = last_data["Cristina (Elypsis)"] / _sum
#     last_data["Macri (Elypsis)"] = last_data["Macri (Elypsis)"] / _sum

#     pred_rst = []
#     sess = get_session()
#     for r in sess.query(Weekly_Predict):
#         pred_rst.append({
#             "dt": r.dt.strftime("%Y-%m-%d"),
#             "Cristina (AI)": r.U_Cristina / (r.U_Cristina + r.U_Macri),
#             "Macri (AI)": r.U_Macri / (r.U_Cristina + r.U_Macri),
#         })

#     pred = pd.DataFrame(pred_rst)
#     data = pred.set_index("dt")
    
#     if now is None:
#         now = pendulum.now().to_date_string()
    
#     # data = data[data.index > last_dt]
#     data = last_data.append(data, sort=False)
#     data = data.sort_index()
#     data = data.round(3)
#     data = data[["Cristina (AI)", "Macri (AI)", "Cristina (Elypsis)", "Macri (Elypsis)"]]
#     data = data.dropna(how='all')

#     print("save:", f"web/data/{now}/Ely-weekly-all.csv")
#     data.to_csv(f"web/data/{now}/Ely-weekly-all.csv")

#     data = data[data.index >= "2019-03-01"]
#     print("save:", f"web/data/{now}/Ely-weekly.csv")
#     data.to_csv(f"web/data/{now}/Ely-weekly.csv")

#     # --------------------------------- Ely3
#     last_data = pd.read_csv(f"web/data/{last}/Ely3-weekly-all.csv").set_index("dt")
#     last_dt = last_data.tail(1).index[0]

#     pred_rst = []
#     sess = get_session()
#     for r in sess.query(Weekly_Predict):
#         pred_rst.append({
#             "dt": r.dt.strftime("%Y-%m-%d"),
#             "Cristina (AI)": r.U_Cristina / (r.U_Cristina + r.U_Macri + r.U_unclassified),
#             "Macri (AI)": r.U_Macri / (r.U_Cristina + r.U_Macri + r.U_unclassified),
#             "Undecided (AI)": r.U_unclassified / (r.U_Cristina + r.U_Macri + r.U_unclassified),
#         })

#     pred = pd.DataFrame(pred_rst)
#     data = pred.set_index("dt")
#     # print(data)
    
#     if now is None:
#         now = pendulum.now().to_date_string()
    
#     # print("last dt:", last_dt)
#     data = data[data.index > last_dt]
#     data = last_data.append(data, sort=False)
#     data = data.sort_index()
#     data = data.round(3)
#     data = data[["Cristina (AI)", "Macri (AI)", "Cristina (Elypsis)", "Macri (Elypsis)", "Undecided (AI)", "Undecided (Elypsis)"]]
#     data = data.dropna(how='all')

#     print("save:", f"web/data/{now}/Ely3-weekly-all.csv")
#     data.to_csv(f"web/data/{now}/Ely3-weekly-all.csv")

#     data = data[data.index >= "2019-03-01"]
#     print("save:", f"web/data/{now}/Ely3-weekly.csv")
#     data.to_csv(f"web/data/{now}/Ely3-weekly.csv")
    

def make_dayN_plot(n, now=False):
    p = []
    sess = get_session()
    if n == 3:
        rsts = sess.query(Day3_Predict)
    elif n == 7:
        rsts = sess.query(Day7_Predict)
    elif n == 14:
        rsts = sess.query(Day14_Predict)
    elif n == 30:
        rsts = sess.query(Day30_Predict)

    for r in rsts:
        _sum = r.U_Cristina + r.U_Macri
        p.append({
            "dt": r.dt.strftime("%Y-%m-%d"),
            "Cristina (AI)": round(r.U_Cristina / _sum, 3),
            "Macri (AI)": round(r.U_Macri / _sum, 3),
            # "Undecided (AI)": round(r.U_unclassified / _sum, 3),
        })

    if not now:
        now = pendulum.now().to_date_string()

    data = pd.DataFrame(p).set_index("dt")
    data = data[data.index >= "2019-03-01"]
    data = data.sort_index()
    print("save:", f"web/data/{now}/day{n}.csv")
    data.to_csv(f"web/data/{now}/day{n}.csv")

    sess.close()


def moving_average(n, now=False):
    if not now:
        now = pendulum.now().to_date_string()
    d = pd.read_csv(f"web/data/{now}/day{n}.csv").set_index("dt")

    # d["3-days"] = d["Macri (AI)"].rolling(3).mean()
    d["MA7"] = d["Macri (AI)"].rolling(7).mean()
    d["MA14"] = d["Macri (AI)"].rolling(14).mean()
    d["MA30"] = d["Macri (AI)"].rolling(30).mean()
    # d["60-days"] = d["Macri (AI)"].rolling(60).mean()
    d = d.drop("Cristina (AI)", axis=1)

    d["Macri (AI)"].rolling(1).mean().dropna()[0]

    for i in range(30):
        if np.isnan(d.iloc[i, 1]): 
            d.iloc[i, 1] = d["Macri (AI)"].rolling(i+1).mean().dropna()[0]
        if np.isnan(d.iloc[i, 2]): 
            d.iloc[i, 2] = d["Macri (AI)"].rolling(i+1).mean().dropna()[0]
        if np.isnan(d.iloc[i, 3]):
            d.iloc[i, 3] = d["Macri (AI)"].rolling(i+1).mean().dropna()[0]
            
    d = d.round(3)
    d.to_csv(f"web/data/{now}/ma{n}.csv")


def percentage_change(n, now=False):
    if not now:
        now = pendulum.now().to_date_string()
    d = pd.read_csv(f"web/data/{now}/day{n}.csv").set_index("dt")

    d.index = pd.to_datetime(d.index)
    AI = d[["Cristina (AI)", "Macri (AI)"]].dropna(axis=0, how='all')
    AI = AI[AI.index.dayofweek == 0]

    d = pd.read_csv(f"web/data/{now}/positive.csv").set_index("dt")
    d.index = pd.to_datetime(d.index)
    EL = d[["Cristina (Elypsis)", "Macri (Elypsis)"]].dropna(axis=0, how='all')

    d1 = (AI.pct_change()[1:] * 100).round(2)
    d1 = d1[["Macri (AI)"]]
    d2 = (EL.pct_change()[1:] * 100).round(2)
    d2["Elypsis (no lag)"] = d2["Macri (Elypsis)"]
    d2 = d2[["Elypsis (no lag)"]]
    d3 = d2.shift(1).dropna()
    d3["Elypsis (7-days in advance)"] = d3["Elypsis (no lag)"]
    d3 = d3[["Elypsis (7-days in advance)"]]
    d4 = d2.shift(2).dropna()
    d4["Elypsis (14-days in advance)"] = d4["Elypsis (no lag)"] * 0.627 - 0.287
    d4 = d4[["Elypsis (14-days in advance)"]]

    d1 = d1.append(d3, sort=False).append(d4, sort=False)
    d1 = d1[d1.index <= "2019-06-17"]
    d1 = d1.dropna(how="all").round(2)
    d1 = d1[["Macri (AI)", "Elypsis (14-days in advance)"]]

    d1.to_csv(f"web/data/{now}/lag{n}.csv")


def make_stat_plot(now=False):
    p3, p4, p5, p6, p7, p8, p9 = [], [], [], [], [], [], []
    sess = get_session()
    rsts = sess.query(Stat)
    # last_data = pd.read_csv(f"web/data/{now}/p8.csv").set_index("dt")
    # last_dt = last_data.tail(1).index[0]

    for r in rsts:
        dt = r.dt.strftime("%Y-%m-%d")
        # if dt <= "2019-02-27":
        #   continue

        if "2019-04-30" <= dt <= "2019-05-07":
            continue
 
        if "2019-06-16" <= dt < "2019-07-20":
            p3.append({
                "dt": dt,
                "Fernandez (AI)": r.K_tweet_count,
                "Macri (AI)": int(r.M_tweet_count * 2)
            })
            p9.append({
                "dt": dt,
                "Fernandez (AI)": r.K_user_count,
                "Macri (AI)": int(r.M_user_count * 2)
            })

        else:
            p3.append({
                "dt": dt,
                "Fernandez (AI)": r.K_tweet_count,
                "Macri (AI)": r.M_tweet_count,
            })
            p9.append({
                "dt": dt,
                "Fernandez (AI)": r.K_user_count,
                "Macri (AI)": r.M_user_count, 
            })

        p4.append({
            "dt": dt,
            "classified": r.K_tweet_count + r.M_tweet_count,
            "collected": r.tweet_count
        })
        p5.append({
            "dt": dt,
            "classified": r.K_user_count + r.M_user_count,
            "collected": r.user_count
        })
        p6.append({
            "dt": dt,
            "classified": r.cla_tweet_cum_count,
            "collected": r.tweet_cum_count
        })
        p7.append({
            "dt": dt,
            "classified": r.cla_user_cum_count,
            "collected": r.user_cum_count
        })


    p8 = []
    sess = get_session()
    for r in sess.query(Percent):
        _dt = r.dt.strftime("%Y-%m-%d")
        _d = get_percent(sess, _dt, clas=3)
        p8.append({
            "dt": _dt,
            "Fernandez (AI)": _d[0],
            "Macri (AI)": _d[1],
            "Undecided (AI)": _d[2],
            "Lavagna (AI)": 0.01
        })
    
    showcols = ["Fernandez (AI)", "Macri (AI)", "Undecided (AI)", "Lavagna (AI)", 
                "Fernandez (Aggregate Polls)", "Macri (Aggregate Polls)", "Undecided (Aggregate Polls)"]
    care = ["Elypsis", "Isonomia", "Imagen y Gestión Política", "M&F", "Opinaia", "Giacobbe"]
    for _care in care:
        showcols.append(f"Fernandez ({_care})")
        showcols.append(f"Macri ({_care})")
        showcols.append(f"Undecided ({_care})")

    from collections import defaultdict
    aggregate_polls = defaultdict(list)
    
    for r in sess.query(Other_Poll).filter(Other_Poll.dt >= "2019-03-01"):
        if r.K + r.M >= 0.9 or r.K + r.M < 0.75:
            continue
        if r.name in care:
            p8.append({
                "dt": r.dt,
                f"Fernandez ({r.name})": r.K / (r.K + r.M + r.U),
                f"Macri ({r.name})": r.M / (r.K + r.M + r.U),
                f"Undecided ({r.name})": r.U / (r.K + r.M + r.U)
            })
        aggregate_polls[r.dt].append({
            "dt": r.dt,
            "Fernandez (Aggregate Polls)": r.K / (r.K + r.M + r.U),
            "Macri (Aggregate Polls)": r.M / (r.K + r.M + r.U),
            "Undecided (Aggregate Polls)": r.U / (r.K + r.M + r.U),
        })

    for k, v in aggregate_polls.items():
        if len(v) == 1:
            p8.append(v[0])
        else:
            _K = 0
            _M = 0
            _U = 0
            for _v in v:
                _K += _v["Fernandez (Aggregate Polls)"]
                _M += _v["Macri (Aggregate Polls)"]
                _U += _v["Undecided (Aggregate Polls)"]
            _K /= len(v)
            _M /= len(v)
            _U /= len(v)

            p8.append({
                "dt": k,
                "Fernandez (Aggregate Polls)": _K,
                "Macri (Aggregate Polls)": _M,
                "Undecided (Aggregate Polls)": _U,
            })
            
    sess.close()


    if not now:
        now = pendulum.now().to_date_string()

    for i, p in enumerate([p3, p4, p5, p6, p7]):
        data = pd.DataFrame(p).set_index("dt")
        data = data[data.index >= "2019-03-01"]
        data = data.sort_index()
        print("save:", f"web/data/{now}/p{i+3}.csv")
        data.to_csv(f"web/data/{now}/p{i+3}.csv")


    data = pd.DataFrame(p8).set_index("dt")
    data = data.round(3)
    data = data[showcols]
    data = data.sort_index()
    # print(data)
    print("save:", f"web/data/{now}/p8.csv")
    data.to_csv(f"web/data/{now}/p8.csv")
    
    data = pd.DataFrame(p9).set_index("dt")
    data = data[data.index >= "2019-03-01"]
    data = data.sort_index()
    print("save:", f"web/data/{now}/p_users.csv")
    data.to_csv(f"web/data/{now}/p_users.csv")

    _sum = data["Fernandez (AI)"] + data["Macri (AI)"]
    data["Fernandez (AI)"] = data["Fernandez (AI)"] / _sum
    data["Macri (AI)"] = data["Macri (AI)"] / _sum
    data = data.round(3)
    print("save:", f"web/data/{now}/day1.csv")
    data.to_csv(f"web/data/{now}/day1.csv")

        
def make_bot_stat_plot(now=False):
    p2, p3, p4, p5, p6, p7, p9 = [], [], [], [], [], [], []
    sess = get_session()
    rsts = sess.query(Bot_Weekly_Predict)
    for r in rsts:
        # print(r.dt)
        dt = r.dt.strftime("%Y-%m-%d")
        if "2019-04-30" <= dt <= "2019-05-07":
            continue
        p2.append({
            "dt": dt,
            "Fernandez (Bots)": r.U_Cristina,
            "Macri (Bots)": r.U_Macri,
        })

    rsts = sess.query(Bot_Stat)
    for r in rsts:
        dt = r.dt.strftime("%Y-%m-%d")
        # if dt <= "2019-02-27":
        #     continue
        if "2019-04-30" <= dt <= "2019-05-07":
            continue
 
        if "2019-06-16" <= dt < "2019-07-20":
            p3.append({
                "dt": dt,
                "Fernandez (AI)": r.K_tweet_count,
                "Macri (AI)": r.M_tweet_count * 2
            })
            p9.append({
                "dt": dt,
                "Fernandez (AI)": r.K_user_count,
                "Macri (AI)": r.M_user_count * 2
            })

        else:
            p3.append({
                "dt": dt,
                "Fernandez (AI)": r.K_tweet_count,
                "Macri (AI)": r.M_tweet_count,
            })
            p9.append({
                "dt": dt,
                "Fernandez (AI)": r.K_user_count,
                "Macri (AI)": r.M_user_count, 
            })

        p4.append({
            "dt": dt,
            "volume": r.K_tweet_count + r.M_tweet_count
        })
        p5.append({
            "dt": dt,
            "volume": r.K_user_count + r.M_user_count
        })
        p6.append({
            "dt": dt,
            "classified": r.cla_tweet_cum_count,
            "collected": r.tweet_cum_count
        })
        p7.append({
            "dt": dt,
            "classified": r.cla_user_cum_count,
            "collected": r.user_cum_count
        })

    if not now:
        now = pendulum.now().to_date_string()
    for i, p in enumerate([p2, p3, p4, p5, p6, p7]):
        data = pd.DataFrame(p)
        # print(data)
        data = data.set_index("dt")
        data = data[data.index >= "2019-03-01"]
        data = data.sort_index()
        print("save:", f"web/data/{now}/p{i+2}_bots.csv")
        data.to_csv(f"web/data/{now}/p{i+2}_bots.csv")

    data = pd.DataFrame(p9).set_index("dt")
    data = data[data.index >= "2019-03-01"]
    data = data.sort_index()
    print("save:", f"web/data/{now}/p_users_bots.csv")
    data.to_csv(f"web/data/{now}/p_users_bots.csv")
    

def make_train_plot():
    def get_cumulative_v(file_name):
        data = json.load(open(file_name))
        period = pendulum.period(pendulum.date(2019, 2, 24), pendulum.date(2019, 4, 29))
        new_data = {}
        last_v = 0
        for dt in period:
            dt = dt.to_date_string()
            try:
                v = data[dt]
            except KeyError:
                v = 0
            last_v = last_v + v
            new_data[dt] = last_v
        _data = {
            "dt": [],
            "v": [],
        }
        for dt, v in new_data.items():
            _data["dt"].append(dt)
            _data["v"].append(v)

        new_data = pd.DataFrame(_data).set_index("dt")
        if file_name.endswith("Kc.txt"):
            col = "K"
        elif file_name.endswith("Mc.txt"):
            col = "M"
        elif file_name.endswith("Ac.txt"):
            col = "A"
        elif file_name.endswith("KAc.txt"):
            col = "K and A"
        new_data = new_data.rename(columns = {
            "v": col,
        })
        # print(new_data)
        return new_data

    d1 = get_cumulative_v("data/Kc.txt")
    d2 = get_cumulative_v("data/Mc.txt")
    d3 = get_cumulative_v("data/Ac.txt")
    d4 = get_cumulative_v("data/KAc.txt")
    d = pd.concat([d1, d2, d3, d4], axis=1)
    print(d)
    d.to_csv("web/data/p1_train.csv")


if __name__ == "__main__":
    
    dt = "2019-06"
    make_main_plot_v3(last=None, now=dt)
    make_main_plot_Elypsis(last=dt, now=dt)
    make_stat_plot(now=dt)
    make_bot_stat_plot(now=dt)
    make_fitting_plot(now=dt)
    make_history_predict(now=dt)

    dt = "2019-05-14"
    make_main_plot_v3(last=dt, now=dt)
    make_main_plot_Elypsis(last=dt, now=dt)
    make_stat_plot(now=dt)
    make_bot_stat_plot(now=dt)
    make_fitting_plot(now=dt)
    make_history_predict(dt)

    # make_dayN_plot(n=3, now=dt)
    # make_dayN_plot(n=7, now=dt)
    # make_dayN_plot(n=14, now=dt)
    # make_dayN_plot(n=30, now=dt)

    # moving_average(n=1, now=dt)
    # moving_average(n=3, now=dt)
    # moving_average(n=7, now=dt)
    # moving_average(n=14, now=dt)
    # moving_average(n=30, now=dt)

    # percentage_change(n=1, now=dt)
    # percentage_change(n=3, now=dt)
    # percentage_change(n=7, now=dt)
    # percentage_change(n=14, now=dt)
    # percentage_change(n=30, now=dt)
