import pymysql
import requests
import time
import json
import hashlib

mysql_config = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': '136418xx',
    'charset': 'utf8mb4',
    'database': 'covid'
}


def get_risk_area():
    """
    :return: risk_h,risk_m 中高风险地区详细数据
    """
    # 当前时间戳
    o = '%.3f' % (time.time() / 1e3)
    e = o.replace('.', '')
    i = "23y0ufFl5YxIyGrI8hWRUZmKkvtSjLQA"
    a = "123456789abcdefg"
    # 签名1
    s1 = hashlib.sha256()
    s1.update(str(e + i + a + e).encode("utf8"))
    s1 = s1.hexdigest().upper()
    # 签名2
    s2 = hashlib.sha256()
    s2.update(str(e + 'fTN2pfuisxTavbTuYVSsNJHetwq5bJvCQkjjtiLM2dCratiA' + e).encode("utf8"))
    s2 = s2.hexdigest().upper()
    # post请求数据
    post_dict = {
        'appId': 'NcApplication',
        'key': '3C502C97ABDA40D0A60FBEE50FAAD1DA',
        'nonceHeader': '123456789abcdefg',
        'paasHeader': 'zdww',
        'signatureHeader': s1,
        'timestampHeader': e
    }
    headers = {
        'Content-Type': 'application/json; charset=utf-8',
        'Referer': 'http://bmfw.www.gov.cn/',
        'Origin': 'http://bmfw.www.gov.cn',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36',
        'x-wif-nonce': 'QkjjtiLM2dCratiA',
        'x-wif-paasid': 'smt-application',
        'x-wif-signature': s2,
        'x-wif-timestamp': e,
    }
    url = "http://103.66.32.242:8005/zwfwMovePortal/interface/interfaceJson"
    req = requests.post(url=url, data=json.dumps(post_dict), headers=headers)
    resp = req.text
    res = json.loads(resp)
    # print(res)
    utime = res['data']['end_update_time']  # 更新时间
    hcount = res['data'].get('hcount', 0)  # 高风险地区个数
    mcount = res['data'].get('mcount', 0)  # 低风险地区个数
    # 具体数据
    hlist = res['data']['highlist']
    mlist = res['data']['middlelist']

    risk_h = []
    risk_m = []

    for hd in hlist:
        type = "高风险"
        province = hd['province']
        city = hd['city']
        county = hd['county']
        area_name = hd['area_name']
        communitys = hd['communitys']
        for x in communitys:
            risk_h.append([utime, province, city, county, x, type])

    for md in mlist:
        type = "中风险"
        province = md['province']
        city = md['city']
        county = md['county']
        area_name = md['area_name']
        communitys = md['communitys']
        for x in communitys:
            risk_m.append([utime, province, city, county, x, type])

    return risk_h, risk_m


if __name__ == '__main__':
    res = get_risk_area()
    print(res)
    db = pymysql.connect(**mysql_config)
    cursor = db.cursor()
    create_table = """
                        CREATE TABLE IF NOT EXISTS risk_place (
                        id int NOT NULL AUTO_INCREMENT,
                        update_time varchar(32) DEFAULT NULL,
                        city varchar(20) DEFAULT NULL,
                        province varchar(20) DEFAULT NULL,
                        county varchar(20) DEFAULT NULL,
                        area_name varchar(64) DEFAULT NULL,
                        type varchar(16) DEFAULT NULL,
                        PRIMARY KEY (`id`)
                        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                        """
    cursor.execute(create_table)
    sql = "TRUNCATE TABLE risk_place"  # 清空表
    cursor.execute(sql)
    insert_sql = 'INSERT INTO risk_place(update_time, city, province, county, area_name, type) VALUES(%s, %s, %s, %s, %s, %s)'
    try:
        insert_res1 = cursor.executemany(insert_sql, res[0])
        print(insert_res1)
        insert_res2 = cursor.executemany(insert_sql, res[1])
        print(insert_res2)
        db.commit()
    except Exception as e:
        db.rollback()
    finally:
        cursor.close()
        db.close()
