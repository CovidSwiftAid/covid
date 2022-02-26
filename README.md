suspectLocationProcess.py 处理爬取的含有“疫情 封”字段的微博
含有nlp、线性回归、词云、去除已经是中高风险地区的疑似地点

overallSpider.py 爬取中国地区的各个省市的确诊数量以及全国的疫情数据

riskPlaceSpider.py 爬取中高风险地区的数据

allNews.py 爬取微博中的主流媒体有关疫情的新闻

weibo、crawls文件夹，爬取含有“疫情 封”字段的微博，作为疑似地点的待处理数据