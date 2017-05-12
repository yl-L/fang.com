# -*- coding:utf-8 -*-

import urllib
import json
import pymysql
import traceback
import threadpool
import re
import pandas as pd
from time import ctime, sleep


def sqldb(city):
    con = pymysql.connect(host='*************', port=****, user="*************", passwd="*************",
                          db="*************",
                          charset='utf8')
    df1 = pd.read_sql('SELECT id,位置,经纬度,区位 FROM %s' % city + " WHERE 经纬度 IN ('无数据', 'OVER_QUERY_LIMIT', 'REQUEST_DENIED')", con)
    con.close()
    print('%s %s获取成功' % (ctime(), city))
    return df1


def get_api(city, key_list, cut=0):
    if len(key_list) != 0:
        a = sqldb(city)
        rep = {}
        key_num = 0
        data_info = []
        thread_list = []
        pool = threadpool.ThreadPool(35)
        qu_list = ['']
        for q in range(len(a)):
            id = a.iloc[q, 0]
            local = a.iloc[q, 1].split('-')[0].replace('&', '')
            local_data = a.iloc[q, 2]
            qu = a.iloc[q, 3]
            if qu != qu_list[-1]:
                qu_list.append(qu)
                rep = {}
            # print(id,local,local_data)
            if local_data in ['无数据', 'OVER_QUERY_LIMIT', 'REQUEST_DENIED']:
                if len(key_list) != 0:
                    if local in rep:
                        try:
                            data_info.append([id, rep['%s' % local][0]])
                        except Exception:
                            thread_list.append(([id, local, key_num, rep, data_info, qu], None))
                            requests = threadpool.makeRequests(get_data, thread_list)
                            pool.putRequest(requests[-1])
                        print('%s!!!!!!!!!!!!!!!!!!!!' % id)
                    else:
                        thread_list.append(([id, local, key_num,  rep, data_info, qu], None))
                        # get_data(id, local, key_num,  rep, data_info)
                        requests = threadpool.makeRequests(get_data, thread_list)
                        pool.putRequest(requests[-1])
                else:
                    break
            else:
                pass
            if len(thread_list) > 105:
                pool.wait()
                thread_list = []
                if len(data_info) >= 500:
                    post(data_info, city)
                    data_info = []
                    print(rep)
        pool.wait()
        if data_info:
            post(data_info, city)
    else:
        cut = 1
    if not cut:
        check_zero_results(city)


def get_data(id, local, key_num, rep, data_info, qu):
    t1 = 1
    local = local.strip(' -')
    if local == '':
        with open('./error1.txt', 'a+', encoding='utf-8') as fe:
            fe.write('%s %s \n' % (id, qu))
    else:
        key = key_list[key_num]
        url = 'https://maps.googleapis.com/maps/api/geocode/json?address=%s&key=%s' % (local, key)
        try:
            state, hjson = get_location(url, id)
        except Exception as e:
            get_data(id, local, key_num, rep, data_info, qu)
        while state is None:
            state, hjson = get_location(url, id)
        if state == 'OK':
            rep['%s' % local] = []
            info = hjson["results"]  # [-1]['geometry']['location']
            t = 0
            for o in range(len(info)):
                b = info[o]
                c = re.search('[%s]{2,}' % (qu+'市'), str(b))
                if c is not None:
                    t = 1
                    address = str(b['geometry']['location']['lat']) + ',' + str(b['geometry']['location']['lng'])
                    data_info.append([id, address])
                    rep['%s' % local].append(address)
                    break
            if not t:
                address = 'ZERO_RESULTS'
                data_info.append([id, address])
                rep['%s' % local].append(address)
            else:
                if not rep['%s' % local]:
                    print('%s我出错啦' % id)
                    print(info)
                    try:
                        print(c)
                    except Exception:
                        pass
        elif state == 'OVER_QUERY_LIMIT' or state == 'REQUEST_DENIED':
            try:
                key_list.remove(key)
            except Exception:
                pass
            if len(key_list) == 0:
                print('key已用完')
                t1 = 0
                # replace_data(rep, city)
            else:
                key = key_list[key_num]
                print('正在使用第%s个key：%s' % (key_num, key))
                get_data(id, local, key_num, rep, data_info, qu)
        else:
            rep['%s' % local] = []
            data_info.append([id, str(state)])
            rep['%s' % local].append(state)
        if data_info and t1:
            print('1', data_info[-1])


def replace_data(rep, city):  # 当keys用尽时，遍历数据，替换rep中所有数据
    print('正在遍历rep中元素并提交')
    a, key_list = sqldb(city)
    data_info = []
    for q in range(len(a)):
        id = a.iloc[q, 0]
        local = a.iloc[q, 1]
        local_data = a.iloc[q, 2]
        if local_data in ['无数据', 'OVER_QUERY_LIMIT', 'REQUEST_DENIED']:
            if local in rep:
                data_info.append([id, rep['%s' % local][0]])
    post(data_info, city)


def get_location(url, id):
    url1 = urllib.request.quote(url, safe=':/&?=')
    print('%s正在更改数据 ' % id, url)
    req = urllib.request.Request(url1)
    req.add_header('User-Agent',
                   'Mozilla/5.0 (iPhone; CPU iPhone OS 9_1 like Mac OS X) AppleWebKit/601.1.46 '
                   '(KHTML, like Gecko) Version/9.0 Mobile/13B143 Safari/601.1')
    req.add_header('accept-language', 'zh-CN,zh;q=0.8')
    try:
        data = urllib.request.urlopen(req).read().decode('utf-8')
        hjson = json.loads(data)
        state = hjson["status"]
        return state, hjson
    except Exception as e:
        print('正在睡眠3s')
        print(e)
        print(traceback.format_exc())
        print(url1)
        sleep(3)
        get_location(url, id)


def post(data_info, city):
    print('正在输入到MySQL')
    d = str(data_info).replace('[', '(').replace(']', ')').replace('((', '(').replace('))', ')')
    con = pymysql.connect(host='107.191.61.150', port=3306, user="soufangwang", passwd="XI6DLZ7PAcXLteVi",
                          db="ershoufang", charset='utf8')
    cur = con.cursor()
    sql = 'insert into %s (id,经纬度) values %s on duplicate key update id=values(id),经纬度=values(经纬度)' % (city, d)
    cur.execute(sql)
    cur.close()
    con.commit()
    con.close()


def main(key_list):
    con = pymysql.connect(host='*************', port=****, user="*************", passwd="*************",
                          db="*************", charset='utf8')
    df1 = pd.read_sql('show tables', con)
    con.close()
    with open('./api已完成城市.txt', encoding='utf-8') as fe:
        already_list = list(map(lambda x: x.split('  ')[0], fe.readlines()))
        print(already_list)
    for q in range(len(df1)):
        city = df1.iloc[q, 0]
        if city not in already_list:
            print(ctime(), city)
            sleep(5)
            get_api(city, key_list)


def check_zero_results(city):
    con = pymysql.connect(host='*************', port=****, user="*************", passwd="*************",
                          db="*************",
                          charset='utf8')
    a = pd.read_sql('SELECT id,经纬度 FROM %s' % city, con)
    b = pd.read_sql('SELECT id,经纬度 FROM %s' % city + " WHERE 经纬度 = 'ZERO_RESULTS'", con)
    con.close()
    score = len(b) / len(a)
    print('%s %s获取成功  %s' % (ctime(), city, score))
    with open('./api已完成城市.txt', 'a+', encoding='utf-8') as f:
        f.write('%s  %s\n' % (city, score))


if __name__ == '__main__':
    key_list = [{YOUR_KEY LIST}]
    # city_list = []  # '惠州','苏州','广州','青岛','大连','徐州','嘉兴','太原','宁波','常州','厦门','南宁','南昌','南通',
    # for city in city_list:  # 需要重跑   '乌鲁木齐','哈尔滨',
    # get_api(city, key_list)
    main(key_list)
