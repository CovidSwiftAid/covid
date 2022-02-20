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

mysql_config = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': '136418xx',
    'charset': 'utf8mb4',
    'database': 'covid'
}


def get_time():
    settings = get_project_settings()  # 获取爬虫的时间
    start_date = settings.get('START_DATE', datetime.now().strftime('%Y-%m-%d'))
    start_time = settings.get('START_TIME')
    start_date_str = start_date + "-" + start_time
    start_time = datetime.strptime(start_date_str, '%Y-%m-%d-%H')
    end_time = start_time + timedelta(hours=1)
    start_time = start_time + timedelta(hours=-23)
    print(start_time, end_time)
    return start_time, end_time


def linear(day_like, day_comment, day_repost, day_sum, hour_like, hour_comment, hour_repost, hour_sum):
    # 加载数据集
    feature_closed, target_closed, feature_positive, target_positive = read_data()
    closed_list = [[day_like, day_comment, day_repost, day_sum]]
    positive_list = [[hour_like, hour_comment, hour_repost, hour_sum, day_sum]]
    closed_array = np.array(closed_list)
    positive_array = np.array(positive_list)

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

    # 标准化
    x_train_positive = std.fit_transform(feature_positive)
    x_test_positive = std.transform(positive_array)
    y_train_positive = std.fit_transform(target_positive.reshape(-1, 1))  # 必须传二维

    # 正规方程的解法
    # 建立模型
    lr = LinearRegression()  # 通过公式求解
    lr.fit(x_train_positive, y_train_positive)

    # 预测结果
    y_predict_positive = lr.predict(x_test_positive)  # 这个结果是标准化之后的结果，需要转换
    y_predict_inverse_positive = std.inverse_transform(y_predict_positive)

    return y_predict_inverse_closed, y_predict_inverse_positive


def create_data():
    with open('data.txt', 'w') as f:
        for index in range(100):
            hour_like = random.randint(0, 10)
            hour_comment = random.randint(0, 10)
            hour_repost = random.randint(0, 10)
            hour_sum = random.randint(0, 2)
            day_like = random.randint(0, 100)
            day_comment = random.randint(0, 100)
            day_repost = random.randint(0, 100)
            day_sum = random.randint(1, 5)
            ran = random.randint(1, 100) / 100
            closed_rate = (day_like + day_comment + day_repost) / 30 + day_sum * 10 + 20 + ran
            positive_rate = (hour_like + hour_comment + hour_repost) / 3 + day_sum * 5 + hour_sum * 5 + 10 + ran

            f.write(
                str(hour_like) + '\t' + str(hour_comment) + '\t' + str(hour_repost) + '\t' + str(hour_sum) + '\t' + str(
                    day_like) + '\t' + str(day_comment) + '\t' + str(day_repost) + '\t' + str(day_sum) + '\t' + str(
                    closed_rate) + '\t' + str(positive_rate) + '\n')


def read_data():
    with open('data.txt', 'r') as f:
        feature_closed_list = []
        target_closed_list = []
        feature_positive_list = []
        target_positive_list = []
        for data in f.readlines():
            data = data.strip().split('\t')
            target_closed_list.append(float(data[8]))
            feature_closed_list.append([float(data[4]), float(data[5]), float(data[6]), float(data[7])])
            target_positive_list.append(float(data[9]))
            feature_positive_list.append(
                [float(data[0]), float(data[1]), float(data[2]), float(data[3]), float(data[7])])
    feature_closed_array = np.array(feature_closed_list)
    target_closed_array = np.array(target_closed_list)
    feature_positive_array = np.array(feature_positive_list)
    target_positive_array = np.array(target_positive_list)
    # print(feature_array, target_array)
    return feature_closed_array, target_closed_array, feature_positive_array, target_positive_array


if __name__ == '__main__':
    create_data()

    start_time, end_time = get_time()
    jieba.enable_paddle()

    db = pymysql.connect(**mysql_config)
    cursor = db.cursor()
    create_table = """
                    CREATE TABLE IF NOT EXISTS real_time_weibo_after_processing (
                    place varchar(100) NOT NULL,
                    closed_rate varchar(20),
                    positive_rate varchar(20),
                    text varchar(12000)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"""
    cursor.execute(create_table)
    sql = "TRUNCATE TABLE real_time_weibo_after_processing"  # 清空表
    cursor.execute(sql)
    sql = "select * from real_time_weibo where created_at>'" + str(start_time) + "' and created_at<'" + str(
        end_time) + "'"
    cursor.execute(sql)
    data = cursor.fetchall()
    sql = "select * from real_time_weibo where created_at>'" + str(
        end_time + timedelta(hours=-1)) + "' and created_at<'" + str(
        end_time) + "'"
    cursor.execute(sql)
    hour_data = cursor.fetchall()
    # print(data)
    per_set = set()  # 地名集合
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
            if flag == 'LOC':
                per_set.add(word)
    print(per_set)
    for place in per_set:
        hour_like = hour_comment = hour_repost = hour_sum = 0
        day_like = day_comment = day_repost = day_sum = 0
        text = []
        for weibo in data:
            if place in weibo[4]:
                day_sum += 1
                day_like += weibo[13]
                day_comment += weibo[14]
                day_repost += weibo[15]
                text.append({
                    "user_name": weibo[3],
                    "weibo_text": weibo[4].replace("\'", "\""),
                    "created_at": str(weibo[11])
                })
        #     closed_rate = (day_sum if day_sum <= 5 else 5) * 10 + (
        #             (day_like if day_like <= 100 else 100) + (day_comment if day_comment <= 100 else 100) + (
        #         day_repost if day_repost <= 100 else 100)) / 30 + 20
        # print(place, "day_data", day_like, day_comment, day_repost, day_sum)
        # print("封闭率：", closed_rate)
        for weibo in hour_data:
            if place in weibo[4]:
                hour_sum += 1
                hour_like += weibo[13]
                hour_comment += weibo[14]
                hour_repost += weibo[15]
        #     positive_rate = (day_sum if day_sum <= 6 else 6) * 5 + (hour_sum if hour_sum <= 2 else 2) * 5 + (
        #             (hour_like if hour_like <= 100 else 100) + (hour_comment if hour_comment <= 100 else 100) + (
        #         hour_repost if hour_repost <= 100 else 100)) / 30 + 10
        # print("hour_data", hour_like, hour_comment, hour_repost, hour_sum)
        # print("确诊率：", positive_rate)
        day_sum = day_sum if day_sum <= 5 else 5
        hour_sum = hour_sum if hour_sum <= 2 else 2
        day_like = day_like if day_like <= 100 else 100
        day_comment = day_comment if day_comment <= 100 else 100
        day_repost = day_repost if day_repost <= 100 else 100
        hour_like = hour_like if hour_like <= 10 else 10
        hour_comment = hour_comment if hour_comment <= 10 else 10
        hour_repost = hour_repost if hour_repost <= 10 else 10
        closed_rate, positive_rate = linear(day_like, day_comment, day_repost, day_sum, hour_like, hour_comment,
                                            hour_repost, hour_sum)
        print(place, closed_rate, positive_rate)
        print(text)

        # sql = "INSERT INTO real_time_weibo_after_processing(place,closed_rate,positive_rate,text) VALUES('%s','%s','%s','%s')"
        # cursor.execute(sql % (str(place), str(closed_rate[0][0]), str(positive_rate[0][0]), str(text)))
        sql = 'insert into real_time_weibo_after_processing(place,closed_rate,positive_rate,text) values(\"' + str(
            place) + '\", \"' + str(closed_rate[0][0]) + '\", \"' + str(positive_rate[0][0]) + '\", \"' + str(
            text) + '\")'
        print(sql)
        cursor.execute(sql)
    db.commit()
    cursor.execute(sql)
    cursor.close()
    db.close()
