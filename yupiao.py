from urllib import request
import time
import datetime
import json
import pymysql

station_bianli = ['CSQ','ZZQ','XTQ','HYQ','SYQ','YYQ','VGQ','DIQ','AEQ','CZQ' ,'AOQ','HHQ','LDQ','JIQ']
station={'广州南':'IZQ' ,'广州':'GZQ', '广州西':'GXQ','广州北':'GBQ', '广州东':'GGQ',\
        '长沙':'CSQ', '长沙南':'CWQ', '长沙西':'RXQ', \
        '株洲南':'KVQ','株洲':'ZZQ','株洲西':'ZAQ',\
        '湘潭':'XTQ','湘潭北':'EDQ',\
        '衡阳':'HYQ', '衡阳东':'HVQ',\
        '邵阳北':'OVQ', '邵阳':'SYQ',\
        '岳阳':'YYQ', '岳阳东':'YIQ',\
        '常德':'VGQ', '张家界':'DIQ',\
        '益阳':'AEQ', \
        '郴州西':'ICQ', '郴州':'CZQ' ,\
        '永州':'AOQ', \
        '怀化南':'KAQ' , '怀化':'HHQ',\
        '娄底':'LDQ', '娄底南':'UOQ', \
        '吉首':'JIQ'}
# 通过爬虫爬取数据
def getTrains(next, i):
    # 请求地址

    # url = 'https://kyfw.12306.cn/otn/leftTicket/query?leftTicketDTO.train_date=2018-08-31&leftTicketDTO.from_station=GZQ&leftTicketDTO.to_station=ZZQ&purpose_codes=ADULT'
    url = 'https://kyfw.12306.cn/otn/leftTicket/queryA?leftTicketDTO.train_date={}&leftTicketDTO.from_station=GZQ&leftTicketDTO.to_station={}&purpose_codes=ADULT'.format(next,i)


    # 请求头
    headers = {
        'User-Agent': r'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36'
    }
    # 设置请求
    req = request.Request(url, headers=headers)
    # 发送请求
    html = request.urlopen(req).read().decode('utf-8-sig')
    print(html)

    # 格式化数据
    dict = json.loads(html)

    # 获取想要的数据
    result = dict['data']['result']

    return result

def get(next, i):
    # 复制接口数据result里边的一条数据出来分析
    results = getTrains(next,i)
    c = 0
    index = 0
    trains = []

    for i in results:
        trains.append([])

        for n in i.split('|'):
            trains[index].append(n)      
            c += 1
        c = 0
        index += 1

    db = pymysql.connect(host='120.77.202.135', user='root', password='123456', db='ticketcollection',charset="utf8")
    cursor = db.cursor()
    # 对处理好的列车数组进行循环遍历
    for train in trains:
        

        from_station1 = (list(station.keys())[list(station.values()).index(train[6])])
        to_station1 = (list(station.keys())[list(station.values()).index(train[7])])
        # print(to_station1)
        # SQL 插入语句
        sql = "INSERT INTO ticket(trainNB,from_station,to_station,facheshijian,daodashijian, \
                    lishishijian,shangwuzuo,yidengzuo,erdengzuo,gaojiruanwo,ruanwo,yingwo,yingzuo,wuzuo,riqi) \
                    VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')" % \
                     (train[3],from_station1,to_station1,train[8],train[9],train[10],train[32],train[31],train[30],train[21],train[23],train[28],train[29],train[26], next)
        try:
           
            cursor.execute(sql)
            db.commit()
        except:
            # Rollback in case there is any error
            db.rollback()

    # 关闭数据库连接
    db.close()

now = datetime.datetime.now()
delta = datetime.timedelta(days=0)
n_days = now + delta
next = n_days.strftime('%Y-%m-%d')
print(next)
get(next, station_bianli[0])

# while True:
#     for i in station_bianli:
#         for d in range(7):
#             now = datetime.datetime.now()
#             delta = datetime.timedelta(days=d)
#             n_days = now + delta
#             next = n_days.strftime('%Y-%m-%d')
#             get(next,i)
#     #m每隔小时执行一次
#     time.sleep(3600)
#     db = pymysql.connect(host='127.0.0.1', user='root', password='123456', db='ticketcollection', charset="utf8")
#     cursor = db.cursor()
#     sql = "delete from ticket"
#     try:
#         # 执行sql语句
#         cursor.execute(sql)
#         # 提交到数据库执行
#         db.commit()
#     except:
#         # Rollback in case there is any error
#         db.rollback()
#
#     # 关闭数据库连接
#     db.close()
