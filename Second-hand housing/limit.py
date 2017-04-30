# -*- coding:utf-8 -*-
import urllib, gzip, traceback, re
from time import ctime, sleep
import subprocess
from threading import Lock, current_thread, Thread
from urllib import request


def limit_area(main_url, url, low, high):
    print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!进入limit_area')
    url_list = []
    page_list = []
    data = get_urldata(url,error_num=0, must=1)
    url_info = url.split('/')[-2]
    print('这是',url_info)
    l = re.findall(r'<p id="shangQuancontain" class="contain">([\s,\S]*?)</p>', data)
    partition_area = re.findall(r'"(/%s-b.*?)" >.{1,5}</a>' % url_info, l[0].replace('  class="org bold"',''))
    detail_url = list(map(lambda x:main_url + x, partition_area))
    for url1 in detail_url:
        data1 = get_urldata(url1,error_num=0, must=1)
        if not re.findall(r'很抱歉，没有找到与', data1):
            try:
                page = int(re.findall(r'"txt">共([0-9]{1,4})页</span>', data1)[0])
            except IndexError:
                sleep(2)
                data1 = get_urldata(url1,error_num=0, must=1)
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
        data = get_urldata(detail_url,error_num=0, must=1)
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
        data = get_urldata(detail_url,error_num=0, must=1)
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
        data = get_urldata(detail_url,error_num=0, must=1)
        if not re.findall(r'很抱歉，没有找到与', data):
            page = int(re.findall(r'"txt">共([0-9]{1,4})页</span>', data)[0])
            url_list.append(detail_url)
            page_list.append(page)
        else:
            print('该页没有房源信息，不添加')
    return url_list, page_list


def get_urldata(main_url,error_num,must):#获取网页信息
    print(ctime(), '这是url', main_url)
    req = urllib.request.Request(main_url)
    req.add_header('User-Agent',
                   'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1) ; 360SE)')
    try:#网页压缩的情况下使用
        print('!!!!!!!!!!!!!!!!!!开始try!!!!!!!!!!!!!!!!!!')
        data = request.urlopen(req,timeout=10).read()
        data = gzip.decompress(data).decode("gb18030")
        if re.findall('二手房', data) and re.findall('body', data) and re.findall('fang.com', data) and isinstance(data,str):
            print('获取正常')
            pass
        else:
            get_urldata(main_url, error_num, must)
        print('结束get_urldata')
        return data
    except TimeoutError:
        print('!!!!!!!!!!!!!!TimeoutError!!!!!!!!!!!!!!!!!!!!!!')
        sleep(1)
        print('访问请求失败，睡眠1s')
        if must == 1:
            get_urldata(main_url, error_num, must)
        else:
            if error_num < 8:
                error_num += 1
                get_urldata(main_url, error_num, must)
            else:
                data = ''
                return data
    except OSError as e:#网页未压缩的情况下使用
        print('!!!!!!!!!!!!!!超时!!!!!!!!!!!!!!!!!!!!!!')
        print(e)
        try:
            data = request.urlopen(req, timeout=10).read().decode('gb18030')
            return data
        except Exception:
            if must == 2:
                data = 0
                return data
            if must == 1:
                get_urldata(main_url, error_num, must)
            else:
                if error_num < 8:
                    error_num += 1
                    get_urldata(main_url, error_num, must)
                else:
                    data = ''
                    return data
    except UnicodeEncodeError:
        print('!!!!!!!!!!!!!!!!!!!写入文件错误，自动跳过!!!!!!!!!!!!!!!!!!!!!!')
        data = ''
        return data
    except Exception as e:
        print('!!!!!!!!!!!!!!Exception!!!!!!!!!!!!!!!!!!!!!')
        print(traceback.format_exc())
        sleep(1)
        print('访问请求失败，睡眠1s')
        if must == 1:
            get_urldata(main_url, error_num, must)
        else:
            if error_num < 8:
                error_num += 1
                get_urldata(main_url, error_num, must)
            else:
                data = ''
                return data

# g21,g22,g23,g24,g299
# 'a573','-a574','-a575','-a576','-a577','-a578','-a579','-a580','-a5120'
