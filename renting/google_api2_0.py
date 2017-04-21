# -*- coding:utf-8 -*-

import urllib
import re
import pandas as pd
from time import ctime, sleep
import json
import csv, traceback



def read_csv(key_list):
    filename = './1/shanghaijiading.csv'
    df=pd.read_csv('%s'%filename,encoding='gbk')
    local = list(df['位置'])

    data = list(df['经纬度'])
    print(local)
    print(dict(df))
    #get_api(local,data,filename,key_list)


def get_api(local_info,data_info,filename,key_list):
    address_info = ['经纬度']
    change_info = 0
    rep = {}
    key_num = 0
    for i in range(len(local_info)):
        local = local_info[i]
        local_data = data_info[i]
        #print(rep)
        if local_data == '无数据' or local_data == 'OVER_QUERY_LIMIT':
            if local in rep:
                address_info.append(rep['%s' % local][0])
            else:
                sleep(1)
                local = local.strip(' ')
                rep['%s' % local] = []
                key = key_list[key_num]
                url = 'https://maps.googleapis.com/maps/api/geocode/json?address=%s&key=%s'%(local,key)
                url1 = urllib.request.quote(url,safe=':/&?=')
                print('已经更改%s条数据  '%change_info,url)
                sleep(0.5)
                req = urllib.request.Request(url1)
                req.add_header('User-Agent','Mozilla/5.0 (iPhone; CPU iPhone OS 9_1 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Version/9.0 Mobile/13B143 Safari/601.1')
                try:
                    data = urllib.request.urlopen(url1).read().decode('utf-8')
                except Exception as e:
                    print('正在睡眠3s')
                    print(traceback.format_exc())
                    sleep(3)
                    data = urllib.request.urlopen(url1).read().decode('utf-8')
                hjson = json.loads(data)
                state = hjson["status"]
                if state == 'OK':
                    info = hjson["results"][-1]['geometry']['location']
                    address = str(info['lat'])+','+str(info['lng'])
                    data_info[i] = address
                    change_info += 1
                    rep['%s' % local].append(address)
                elif state == 'OVER_QUERY_LIMIT':
                    truelen = len(key_list) - 1
                    if key_num == truelen:
                        print('key已用完')
                        break
                    else:
                        key_num += 1
                        print('正在使用第%s个key：%s' % (key_num,key_list[key_num]))
                else:
                    data_info[i] = str(state)
                    change_info += 1
                    rep['%s' % local].append(state)
                print(data_info)
        else:
            pass
        #print(data_info)
    data_info.insert(0,'经纬度')
    write(data_info,filename)


def write(address,file):
    h = {}
    #writer = csv.DictWriter(open('1.csv', 'a+'), fieldnames=address)
    #e = writer.writerows(address)
    l=[]
    with open('%s'% file, newline='') as csvfile:
         spamreader = csv.reader(csvfile, delimiter=',', quotechar='"')
         i = -1
         lenth = len(address)
         for row in spamreader:
             i += 1
             row.pop(-1)
             if i<lenth:
                 row.append(address[i])
                 l = l + [row]
             else:
                 row.append('无数据')
                 l = l + [row]
         #print(l)
    with open('%s'%file, 'w', newline='') as csvfile:
        spamwriter = csv.writer(csvfile, delimiter=',',
                                quotechar='"', quoting=csv.QUOTE_MINIMAL)
        for row in l:
            print('这是row',row)
            spamwriter.writerow(row)

if __name__ == '__main__':
    key_list = ['AIzaSyA3hEATmoJ9h_6WOaIlCOcsMuqPqPWlqz0','AIzaSyA_2Z7d5dF_-NRhDE3nQQYKYHUQxtloEF4','AIzaSyBo9d7Nh_Ow4FpiXDwNZysXHUFJp6xsLds','AIzaSyDLEvItzGYGunJHiR9oud_qHLUEA3FbdXQ','AIzaSyBJBd6MjiuvXkV4OwW1pfOGE1or05BnKOk','AIzaSyAVJfWviP4bnisiOaErTrkBteMHf1Nu-Nk','AIzaSyDTSn21SVlFmhqYFq4ff-1IHBN4zA3if_I','AIzaSyCyT3iIevSX8EbJQxGeSa-NIxPpDjb-XxQ','AIzaSyDjjRvB3plmkfVDruUqTMj4DWDR0e1D4fI','AIzaSyBOjlXXh7Xh1H9aYA8Q1z3NrnLDvuZ6HbA','AIzaSyAc3CRTjwMys6RHBxYp4LSypQZUON32ULw','AIzaSyClRUFVR4_lOX-FwpKqh36b1UqfOhXGOAo']
    read_csv(key_list)
