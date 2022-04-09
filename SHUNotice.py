from urllib import request
from bs4 import BeautifulSoup
import pymysql
import mysqlConfig


def db_create():
    db = pymysql.connect(**mysqlConfig.mysql_config)
    cursor = db.cursor()
    create_table = """
                            CREATE TABLE IF NOT EXISTS SHU_covid (
                            id int NOT NULL AUTO_INCREMENT,
                            content text DEFAULT NULL,
                            PRIMARY KEY (`id`)
                            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                            """
    cursor.execute(create_table)
    db.commit()
    cursor.close()
    db.close()


def SHU_covid_spider():
    headers = {  # 伪装成浏览器爬取数据
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36'}
    url = 'https://news.shu.edu.cn/info/1021/63907.htm'
    url = request.Request(url, headers=headers)  # 伪装成浏览器获取信息
    content = request.urlopen(url).read()
    html = content.decode('utf-8')
    soup = BeautifulSoup(html, 'html.parser')
    covid_list = soup.find(id='vsb_content_2').get_text().strip().split('\n')
    return covid_list


def db_insert_covid(covid_tuple_new):
    db = pymysql.connect(**mysqlConfig.mysql_config)
    cursor = db.cursor()
    get_info = """select * from SHU_covid"""
    cursor.execute(get_info)
    covid_tuple_old = cursor.fetchall()
    covid_list_old = []
    for new in covid_tuple_old:
        covid_list_old.append(new[1])

    def similarity(a, b):
        return (item for item in a if item not in b)
    covid_tuple = similarity(covid_tuple_new, covid_list_old)
    insert_sql = 'INSERT INTO SHU_covid(content) VALUES(%s)'
    insert_res = cursor.executemany(insert_sql, covid_tuple)
    print(insert_res)
    db.commit()
    cursor.close()
    db.close()


if __name__ == '__main__':
    db_create()
    covid_list = SHU_covid_spider()
    # print(covid_list)
    db_insert_covid(tuple(covid_list))

# alter table friends  AUTO_INCREMENT=10;
