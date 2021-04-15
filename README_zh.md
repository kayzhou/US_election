# 美国总统大选预测

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