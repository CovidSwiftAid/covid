# -*- coding: utf-8 -*-
import jieba
import jieba.posseg as pseg
import pymysql
from scrapy.utils.project import get_project_settings
from datetime import datetime, timedelta
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LinearRegression
import random
import numpy as np
import wordcloud  # 词云
import oss2  # 传OSS
import geocode  # 地理编码
import mysqlConfig


class AliyunOss(object):
    def __init__(self):
        self.access_key_id = "LTAI4GGsTQ35tQcWWDVNKwqG"
        self.access_key_secret = "reWjqrK73PE0ZvJQH0Hwjr9eyuWbuc"
        self.auth = oss2.Auth(self.access_key_id, self.access_key_secret)
        self.bucket_name = "shu-covid"
        self.endpoint = "https://oss-cn-shanghai.aliyuncs.com"
        self.bucket = oss2.Bucket(self.auth, self.endpoint, self.bucket_name)

    def put_object_from_file(self, name, file):
        """
        上传本地文件
        :param name: 需要上传的文件名
        :param file: 本地文件名
        :return: 阿里云文件地址
        """
        self.bucket.put_object_from_file(name, file)
        return "https://{}.{}/{}".format(self.bucket_name, self.endpoint, name)


def get_time():
    settings = get_project_settings()  # 获取爬虫的时间
    start_date = settings.get('START_DATE', datetime.now().strftime('%Y-%m-%d'))
    start_time = settings.get('START_TIME')
    start_date_str = start_date + "-" + start_time
    start_time = datetime.strptime(start_date_str, '%Y-%m-%d-%H')
    end_time = start_time + timedelta(hours=1)
    start_time = start_time + timedelta(hours=-11)
    print(start_time, end_time)
    return start_time, end_time


def linear(day_like, day_comment, day_repost, day_sum):
    # 加载数据集
    feature_closed, target_closed = read_data()
    closed_list = [[day_like, day_comment, day_repost, day_sum]]
    closed_array = np.array(closed_list)

    # 标准化
    std = StandardScaler()
    x_train_closed = std.fit_transform(feature_closed)
    x_test_closed = std.transform(closed_array)
    y_train_closed = std.fit_transform(target_closed.reshape(-1, 1))  # 必须传二维

    # 正规方程的解法
    # 建立模型
    lr = LinearRegression()  # 通过公式求解
    lr.fit(x_train_closed, y_train_closed)

    # 预测结果
    y_predict_closed = lr.predict(x_test_closed)  # 这个结果是标准化之后的结果，需要转换
    y_predict_inverse_closed = std.inverse_transform(y_predict_closed)

    return y_predict_inverse_closed


def create_data():
    with open('data.txt', 'w') as f:
        for index in range(100):
            day_like = random.randint(0, 100)
            day_comment = random.randint(0, 100)
            day_repost = random.randint(0, 100)
            day_sum = random.randint(1, 5)
            ran = random.randint(1, 100) / 100
            closed_rate = (day_like + day_comment + day_repost) / 30 + day_sum * 10 + 20 + ran

            f.write(str(day_like) + '\t' + str(day_comment) + '\t' + str(day_repost) + '\t' + str(day_sum) + '\t' + str(
                closed_rate) + '\n')


def read_data():
    with open('data.txt', 'r') as f:
        feature_closed_list = []
        target_closed_list = []
        for data in f.readlines():
            data = data.strip().split('\t')
            target_closed_list.append(float(data[4]))
            feature_closed_list.append([float(data[0]), float(data[1]), float(data[2]), float(data[3])])
    feature_closed_array = np.array(feature_closed_list)
    target_closed_array = np.array(target_closed_list)

    return feature_closed_array, target_closed_array


if __name__ == '__main__':
    create_data()
    w = wordcloud.WordCloud(font_path="simsun.ttc", background_color="white")

    start_time, end_time = get_time()
    jieba.enable_paddle()

    db = pymysql.connect(**mysqlConfig.mysql_config)
    cursor = db.cursor()
    create_table = """
                    CREATE TABLE IF NOT EXISTS real_time_weibo_after_processing (
                    id int NOT NULL AUTO_INCREMENT,
                    place varchar(100) NOT NULL,
                    closed_rate varchar(20),
                    positive_rate varchar(20),
                    text mediumtext,
                    longitude varchar(20) DEFAULT NULL,
                    latitude varchar(20) DEFAULT NULL,
                    level varchar(20) DEFAULT NULL,
                    PRIMARY KEY (`id`)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"""
    cursor.execute(create_table)
    sql = "TRUNCATE TABLE real_time_weibo_after_processing"  # 清空表
    cursor.execute(sql)
    sql = "select * from real_time_weibo order by id desc limit 500"
    cursor.execute(sql)
    data = cursor.fetchall()
    sql = "select * from risk_place"
    cursor.execute(sql)
    risk_data = cursor.fetchall()
    # print(risk_data)
    risk_set = set()
    for place in risk_data:
        risk_set.add(place[2])
        risk_set.add(place[3])
        risk_set.add(place[4])
        risk_set.add(place[5])

    print(data)
    remove_place_set = {'中国', '封城', '封村', '中华人民共和国', '美国', '日本', '丹麦', '学校食堂', '泰国', '俄罗斯', '大城市', '居住小区', '天南地北'}
    per_set = set()  # 地名集合
    per_dict = {}
    for i in data:
        weibo_id = i[0]
        user_id = i[2]
        text = i[4]

        word_list = jieba.lcut(text)
        for word in word_list:
            if len(word) == 1:  # 不加判断会爆
                continue
            words = pseg.cut(word, use_paddle=True)  # paddle模式
            word, flag = list(words)[0]
            isExist = True
            if flag == 'LOC':
                for place in risk_set:
                    if word in place:
                        isExist = False
                if isExist and word not in remove_place_set:
                    per_set.add(word)
                    per_dict[word] = per_dict.get(word, 0) + 1

    # print(per_dict)
    w.generate_from_frequencies(per_dict)
    w.to_file("wordcloud.png")
    aliyunoss = AliyunOss()
    img = aliyunoss.put_object_from_file("images/wordcloud.png", "wordcloud.png")
    print(img)

    for place in per_set:
        location = geocode.ExcuteSingleQuery([place])
        if location:
            day_like, day_comment, day_repost, day_sum = 0, 0, 0, 0
            text = []
            for weibo in data:
                if place in weibo[4]:
                    day_sum += 1
                    day_like += weibo[13]
                    day_comment += weibo[14]
                    day_repost += weibo[15]
                    text.append({
                        "user_name": weibo[3],
                        "weibo_text": weibo[4].replace("\'", "").replace("\"", ""),
                        "created_at": str(weibo[11])
                    })

            day_sum = day_sum if day_sum <= 5 else 5
            day_like = day_like if day_like <= 100 else 100
            day_comment = day_comment if day_comment <= 100 else 100
            day_repost = day_repost if day_repost <= 100 else 100
            closed_rate = linear(day_like, day_comment, day_repost, day_sum)

            sql = 'insert into real_time_weibo_after_processing(place,closed_rate,positive_rate,text,longitude,latitude,level) values(\"' + str(
                place) + '\", \"' + str(closed_rate[0][0]) + '\", \"' + str(0) + '\", \"' + str(text) + '\", \"' + \
                  location[0][0] + '\", \"' + location[0][1] + '\", \"' + location[0][2] + '\")'
            print(sql)
            cursor.execute(sql)
    db.commit()
    cursor.execute(sql)
    cursor.close()
    db.close()
