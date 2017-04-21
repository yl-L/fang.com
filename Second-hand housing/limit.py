# -*- coding:utf-8 -*-
import urllib, gzip, traceback, re
from time import ctime, sleep


def get_urldata(main_url):#获取网页信息
    sleep(2)
    req = urllib.request.Request(main_url)
    req.add_header('User-Agent','Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36')
    try:#网页压缩的情况下使用
        data = urllib.request.urlopen(req).read()
        data = gzip.decompress(data).decode("gb18030")
        return data
    except TimeoutError:
        sleep(3)
        print('访问请求失败，睡眠3s')
        data = urllib.request.urlopen(req).read()
        data = gzip.decompress(data).decode("gb18030")
        return data
    except OSError:#网页未压缩的情况下使用
        try:
            data = urllib.request.urlopen(req).read().decode("gb18030")
            return data
        except Exception as e:
            data = urllib.request.urlopen(req).read()
            data = gzip.decompress(data).decode("gb18030")
            return data
    except Exception as e:
        print(traceback.format_exc())
        sleep(3)
        print('访问请求失败，睡眠3s')
        data = urllib.request.urlopen(req).read()
        data = gzip.decompress(data).decode("gb18030")
        return data


def limit_area(main_url, url, low, high):
    url_list = []
    page_list = []
    data = get_urldata(url)
    url_info = url.split('/')[-2]
    print(url_info)
    l = re.findall(r'<p id="shangQuancontain" class="contain">((.*?\s){2,120}?</p>)', data)
    partition_area = re.findall(r'"(/%s-b.*?)" >.{1,5}</a>' % url_info, l[0][0].replace('  class="org bold"',''))
    detail_url = list(map(lambda x:main_url + x, partition_area))
    for url1 in detail_url:
        data1 = get_urldata(url1)
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
                low = high
                high = low + 20
                add_more_limit(url, url_list, page_list, low, high)
            else:
                high = (high - low - 1) // 2 + low - 1
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
