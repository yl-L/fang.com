# -*- coding:utf-8 -*-
import urllib, gzip, traceback, re
from time import ctime, sleep
import subprocess
from threading import Lock, current_thread, Thread


def get_urldata(main_url):
    try:
        child = subprocess.Popen(['python', '1.py', "%s"%main_url])
        child.wait(15)
    except Exception as e:
        child.kill()
        print('获取超时，重新获取')
        get_urldata(main_url)
    with open("./cache.txt", encoding='gbk')as f:
        a = ''.join(f.readlines())
    return a


def limit_area(main_url, url, low, high):
    print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!进入limit_area')
    url_list = []
    page_list = []
    data = get_urldata(url)
    url_info = url.split('/')[-2]
    print('这是',url_info)
    l = re.findall(r'<p id="shangQuancontain" class="contain">([\s,\S]*?)</p>', data)
    partition_area = re.findall(r'"(/%s-b.*?)" >.{1,5}</a>' % url_info, l[0].replace('  class="org bold"',''))
    detail_url = list(map(lambda x:main_url + x, partition_area))
    for url1 in detail_url:
        data1 = get_urldata(url1)
        if not re.findall(r'很抱歉，没有找到与', data1):
            try:
                page = int(re.findall(r'"txt">共([0-9]{1,4})页</span>', data1)[0])
            except IndexError:
                sleep(2)
                data1 = get_urldata(url1)
                page = int(re.findall(r'"txt">共([0-9]{1,4})页</span>', data1)[0])
            print(url1)
            print('这是page', page)
            if page < 100:
                url_list.append(url1)
                page_list.append(page)
            else:
                url_list,page_list = add_limit(url1, url_list, page_list, low, high)
    print(url_list)
    print(len(url_list),len(page_list))
    return url_list, page_list


def add_limit(url, url_list, page_list, low, high):
    print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!进入add_limit')
    limit = ['g21', 'g22', 'g23', 'g24', 'g299']
    for i in limit:
        detail_url = url+i
        print(detail_url)
        data = get_urldata(detail_url)
        if not re.findall(r'很抱歉，没有找到与', data):
            page = int(re.findall(r'"txt">共([0-9]{1,4})页</span>', data)[0])
            print(page)
            print('这是添加限制的url',detail_url)
            if page < 100:
                url_list.append(detail_url)
                page_list.append(page)
            else:#添加更进一步限制，例如北京等城市
                url_list, page_list = add_more_limit(detail_url, url_list, page_list, low, high)
        else:
            print('该页没有房源信息，不添加')
    print(url_list)
    print(len(url_list),len(page_list))
    return url_list, page_list


def add_more_limit(url, url_list, page_list, low, high):
    print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!进入add_more_limit')
    if high < 250:
        detail_url = url + '-j2%s-k2%s' % (low, high)
        print(detail_url)
        data = get_urldata(detail_url)
        if not re.findall(r'很抱歉，没有找到与', data):
            page = int(re.findall(r'"txt">共([0-9]{1,4})页</span>', data)[0])
            print(page)
            if page < 100:
                url_list.append(detail_url)
                page_list.append(page)
                print('这是第二次添加限制的url', detail_url)
                low = high +1
                high = low + 19
                add_more_limit(url, url_list, page_list, low, high)
            else:
                high = (high - low - 1) // 2 + low
                add_more_limit(url, url_list, page_list, low, high)
        else:
            print('该页没有房源信息，不添加')
            low = high
            high = low + 20
            add_more_limit(url, url_list, page_list, low, high)
    else:
        detail_url = url + '-j2%s' % high
        print(detail_url)
        data = get_urldata(detail_url)
        if not re.findall(r'很抱歉，没有找到与', data):
            page = int(re.findall(r'"txt">共([0-9]{1,4})页</span>', data)[0])
            url_list.append(detail_url)
            page_list.append(page)
        else:
            print('该页没有房源信息，不添加')
    return url_list, page_list
# g21,g22,g23,g24,g299
# 'a573','-a574','-a575','-a576','-a577','-a578','-a579','-a580','-a5120'
