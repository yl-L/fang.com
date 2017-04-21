# -*- coding:utf-8 -*-

import urllib
import re
import pandas as pd
from time import ctime, sleep
import json, pymysql
import csv, traceback



def sqldb(city):
    con = pymysql.connect(host='45.76.212.82', port=3306, user="soufangwang", passwd="XI6DLZ7PAcXLteVi", db="soufang",
                          charset='utf8')
    df1 = pd.read_sql('SELECT id,位置,经纬度 FROM %s'%city, con)#,index_col = 'id')
    con.close()
    print('获取成功')
    return df1,key_list



def get_api():
    a, key_list = sqldb(city)
    change_info = 0
    rep = {}
    key_num = 0
    #testkey()
    data_info = []
    for q in range(len(a)):
        id = a.iloc[q,0]
        local = a.iloc[q,1]
        local_data = a.iloc[q,2]
        #print(id,local,local_data)
        if local_data == '无数据' or local_data == 'OVER_QUERY_LIMIT':
            if local in rep:
                data_info.append([id,rep['%s' % local][0]])
            else:
                sleep(1)
                local = local.strip(' ')
                key = key_list[key_num]
                url = 'https://maps.googleapis.com/maps/api/geocode/json?address=%s&key=%s'%(local,key)
                state,hjson = get_location(change_info,url)
                if state == 'OK':
                    rep['%s' % local] = []
                    info = hjson["results"][-1]['geometry']['location']
                    address = str(info['lat'])+','+str(info['lng'])
                    data_info.append([id, address])
                    change_info += 1
                    rep['%s' % local].append(address)
                elif state == 'OVER_QUERY_LIMIT':
                    truelen = len(key_list) - 1
                    if key_num == truelen:
                        print('key已用完')
                        post(data_info)
                        replace_data(rep)
                        break
                    else:
                        key_num += 1
                        key1 = key_list[key_num]
                        print('正在使用第%s个key：%s' % (key_num,key1))
                        change_url = 'https://maps.googleapis.com/maps/api/geocode/json?address=%s&key=%s' % (local, key1)
                        state, hjson = get_location(change_info,change_url)
                        if state == 'OK':
                            rep['%s' % local] = []
                            info = hjson["results"][-1]['geometry']['location']
                            address = str(info['lat']) + ',' + str(info['lng'])
                            data_info.append([id, address])
                            change_info += 1
                            rep['%s' % local].append(address)
                        else:
                            rep['%s' % local] = []
                            data_info.append([id, str(state)])
                            change_info += 1
                            rep['%s' % local].append(state)
                else:
                    rep['%s' % local] = []
                    data_info.append([id, str(state)])
                    change_info += 1
                    rep['%s' % local].append(state)
                if data_info:
                    print(data_info[-1])
        else:
            pass
        if len(data_info)>=500:
            post(data_info)
            data_info=[]
            print(rep)
        #print(data_info)
    if data_info:
        post(data_info)


def replace_data(rep):#当keys用尽时，遍历数据，替换rep中所有数据
    print('正在遍历rep中元素并提交')
    a,key_list = sqldb(city)
    data_info = []
    for q in range(len(a)):
        id = a.iloc[q,0]
        local = a.iloc[q,1]
        local_data = a.iloc[q,2]
        if local_data == '无数据' or local_data == 'OVER_QUERY_LIMIT':
            if local in rep:
                data_info.append([id, rep['%s' % local][0]])
    post(data_info)

def get_location(change_info,url):
    url1 = urllib.request.quote(url, safe=':/&?=')
    print('已经更改%s条数据  ' % change_info, url)
    sleep(0.5)
    req = urllib.request.Request(url1)
    req.add_header('User-Agent',
                   'Mozilla/5.0 (iPhone; CPU iPhone OS 9_1 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Version/9.0 Mobile/13B143 Safari/601.1')
    try:
        data = urllib.request.urlopen(url1).read().decode('utf-8')
    except Exception as e:
        print('正在睡眠3s')
        print(traceback.format_exc())
        sleep(3)
        try:
            data = urllib.request.urlopen(url1).read().decode('utf-8')
        except Exception as e:
            print('第二次睡眠')
            sleep(3)
            data = urllib.request.urlopen(url1).read().decode('utf-8')
    hjson = json.loads(data)
    state = hjson["status"]
    return state,hjson


def post(data_info):
    print('正在输入到MySQL')
    d = str(data_info).replace('[', '(').replace(']', ')').replace('((', '(').replace('))', ')')
    con = pymysql.connect(host='45.76.212.82', port=3306, user="soufangwang", passwd="XI6DLZ7PAcXLteVi", db="soufang",
                          charset='utf8')
    cur = con.cursor()
    sql = 'insert into %s (id,经纬度) values %s on duplicate key update id=values(id),经纬度=values(经纬度)' % (city, d)
    cur.execute(sql)
    cur.close()
    con.commit()
    con.close()
#[[1, '22.814961,113.809134'], [2, '22.738056,114.121444'], [3, '22.7210111,114.2080736'], [4, '22.7210111,114.2080736'], [5, '22.9727592,114.0082092'], [6, '22.778509,114.022952'], [7, 'ZERO_RESULTS'], [8, '23.0100737,114.0742398']]
if __name__ == '__main__':
    key_list = ['AIzaSyA3hEATmoJ9h_6WOaIlCOcsMuqPqPWlqz0','AIzaSyA_2Z7d5dF_-NRhDE3nQQYKYHUQxtloEF4','AIzaSyBo9d7Nh_Ow4FpiXDwNZysXHUFJp6xsLds','AIzaSyDLEvItzGYGunJHiR9oud_qHLUEA3FbdXQ','AIzaSyBJBd6MjiuvXkV4OwW1pfOGE1or05BnKOk','AIzaSyAVJfWviP4bnisiOaErTrkBteMHf1Nu-Nk','AIzaSyDTSn21SVlFmhqYFq4ff-1IHBN4zA3if_I','AIzaSyCyT3iIevSX8EbJQxGeSa-NIxPpDjb-XxQ','AIzaSyDjjRvB3plmkfVDruUqTMj4DWDR0e1D4fI','AIzaSyBOjlXXh7Xh1H9aYA8Q1z3NrnLDvuZ6HbA','AIzaSyAc3CRTjwMys6RHBxYp4LSypQZUON32ULw','AIzaSyClRUFVR4_lOX-FwpKqh36b1UqfOhXGOAo','AIzaSyAi_M-thaECXGQDdlFbJ18IDDF1Cmu6qzo','AIzaSyBjZ8n3SUos9bpJ-tUoTFBPW9Jl2kTZ7Jk','AIzaSyAmNTMgmEk6BY3IAwTX7P2lHhgW79m7vws']
    city = '防城港'
    get_api()
