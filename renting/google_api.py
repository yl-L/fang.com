# -*- coding:utf-8 -*-

import urllib
import re
import pandas as pd
from time import ctime, sleep
import json
from test1 import write


def read_csv():
    filename = './1/shanghaijiading.csv'
    df=pd.read_csv('%s'%filename,encoding='gbk')
    j = list(df['位置'])
    print(j)
    get_api(j,filename)


def get_api(local_info,filename):
    i = 0
    address_info = ['经纬度']
    rep = {}
    for local in local_info:
        print(rep)
        i += 1
        if local in rep:
            address_info.append(rep['%s' % local][0])
        else:
            sleep(1)
            local = local.strip(' ')
            rep['%s' % local] = []
            url = 'https://maps.googleapis.com/maps/api/geocode/json?address=%s&key={YOUR KEY}'%local
            url1 = urllib.request.quote(url,safe=':/&?=')
            print(url1)
            req = urllib.request.Request(url1)
            req.add_header('User-Agent','Mozilla/5.0 (iPhone; CPU iPhone OS 9_1 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Version/9.0 Mobile/13B143 Safari/601.1')
            data = urllib.request.urlopen(url1).read().decode('utf-8')
            hjson = json.loads(data)
            state = hjson["status"]
            if state == 'OK':
                info = hjson["results"][-1]['geometry']['location']
                address = str(info['lat'])+','+str(info['lng'])
                address_info.append(address)
                rep['%s' % local].append(address)
            else:
                address_info.append(state)
                rep['%s' % local].append(state)
            print(address_info)
    print(address_info)
    write(address_info,filename)
if __name__ == '__main__':
    read_csv()
