# 美国总统大选预测

2020年基础算法
1. 确定选举相关关键词，尤其是候选人名字和昵称；
2. 标注种子hashtag；
3. hashtag传播算法；
4. 标注训练数据；
5. 训练文本观点分类模型；
6. 推测每条文本的观点；
7. 按时间窗口或累计初始日期推测每个用户在某时刻的观点；
8. 计算选举日所有曾经发言用户的观点；
9. 通过转发网络计算用户的政治观点；
10. 结合地区和人口统计学特征，加权计算用户的观点。


关键代码流程
- read_raw_data.py 获取原始数据
- classifer.py 分类器模型
- SQLite_handler.py 使用分类器将原始数据分类，并导入至数据库
- prediction_from_db.py 生成每日结果以及累计结果
- prediction_reweight.py 调整用户权重重新预测

辅助代码
- analyze_user.py 分析用户特征，包括地域、年龄、性别等，调用analyze_user_location，analyze_user_face.py
- collect_location.py 需要解释用户的location，转换成相应的address，state和county
- collect_user.py 重新收集用户的profile，尤其是针对头像的url。因为url是临时的，有可能存在失效的情况
- analyze_hashtag.py 分析话题标签，用于标注训练数据以及提取需要标注的hashtag
- get_ht_network.py 和 run_stat_sign_cooc.py 用以实现共现网络

每日执行代码（自动化展示）
- daily_predict.py 分类并入库，用户数据收集，执行累计预测，生成结果csv，上传展示网站


In the 38 machine where are the raw data of US 2020, there is no space left. I would need to zip
all the file in 
/external2/zhenkun/US_election_data/raw_data
and 
/home/zhenkun/US_election/raw_data/raw_data
and 
/home/zhenkun/US_election/raw_data 202009 and 202010

Now, we have two main models trained by the dataset before August.
v1: Biden versus Trump
v2: Democrats versus Trump

But the plots in the website shows results base on an old model trained by April (Democrats versus Trump). 
So, I try to combine two main models in different ways and send you the results on next Friday (Dec 11).
We also need one paper for US 2020 election prediction.

v1、v2中已经包括了核心结果，可以推测出bots
从第四个月才开始有v1，也就是说前面只有v2的比较
