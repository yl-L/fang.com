# -*- coding:utf-8 -*-

import urllib
import gzip, traceback
import re, pypinyin
import pandas as pd
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
        data = urllib.request.urlopen(req).read().decode("gb18030")
        return data
    except :
        print(traceback.format_exc())
        sleep(3)
        print('访问请求失败，睡眠3s')
        data = urllib.request.urlopen(req).read()
        data = gzip.decompress(data).decode("gb18030")
        return data

def get_areaurl(main_url):#获取该城市所有地区链接
    data = get_urldata(main_url)
    urllist = re.findall(r'<a href="(.*?)"',re.findall(r'<a href="javascript:void.*?不限(.*)', data)[0])
    detail_url = []
    for i in urllist:
        detail_url.append(main_url+i)
    return detail_url

def  main():#留用最后汇总城市
    pass



def check_page(url,low,high,url_list,high_price,page_list):#检查页面是否超过80（上限100）
    detail_url = url + 'c2%s-d2%s' % (low, high)
    print('这是url',detail_url)
    data = get_urldata(detail_url)
    page = int(re.findall(r'"txt">共([0-9]{1,4})页</span>', data)[0])
    print('这是page',page)
    not_found = re.findall(r'很抱歉，没有找到符合条件的房源', data)
    if low < high_price:
        if page < 100:#只有当小于100（页面不满时）爬取
            if page == 1 and  not_found:#当只有一页且显示没有房源时
                if high <= low:  # 如果高低价位重合，则添加新的限制板块
                    # 更加细分  调用方法
                    print('chaoguo')
                    url_list = add_limit(detail_url, url_list)
                    print('chaoguole')
                    low = low + 1
                    high = low + 3000
                    check_page(url, low, high, url_list, high_price, page_list)
                else:
                    low = high + 1
                    high = low + 2999
                    print(high, low)
                    print('空  不放入链接',not_found)
                    check_page(url, low, high, url_list, high_price,page_list)
            else:#当只有多页且显示有房源时
                low = high + 1
                high = low + 2999
                url_list.append(detail_url)
                page_list.append(page)
#                print('正在放入链接', url_list)
                check_page(url, low, high, url_list,high_price,page_list)
        else:
            if high <= low:#如果高低价位重合，则添加新的限制板块
                #更加细分  调用方法
                print('chaoguo')
                url_list = add_limit(detail_url,url_list)
                print('chaoguole')
                low = low+1
                high = low + 3000
                check_page(url, low, high, url_list, high_price, page_list)
            else:#如果页面数为100，则继续细分价格区间
                high = (high-low-1)//2+low-1
                check_page(url,low,high,url_list,high_price,page_list)
    return url_list, page_list

def add_limit(url,url_list):
    limit = ['-g21','-g22','-g23','-g24','-g299']
    for i in limit:
        detail_url = url+i
        data = get_urldata(detail_url)
        page = int(re.findall(r'"txt">共([0-9]{1,4})页</span>', data)[0])
        print(page)
        print('这是添加限制的url',detail_url)
        url_list.append(detail_url)
    return url_list


# g21,g22,g23,g24,g299
# a573,a574,a575,a576,a577,a578,a579,a580,a5120

def get_detailpage(main_url):
    start_time = ctime()
    area_url = get_areaurl(main_url)
    print('这是area_url', area_url)
    finally_list = []
    for url in area_url:
        url_list = []
        page_list = []
        if url != 'http://zu.sz.fang.com/house-a085/':
            low = 0
            high = 1000
            price_url = url + 'h33'
            data = get_urldata(price_url)
            high_price = int(re.findall(r'"price">([0-9]*?)</span>',data)[0])
            print(high_price)
            filename = re.findall(r'content="(.*)租房" />',data)[0]
            print(filename)
            if True:#filename == '北京朝阳':
                print('开始获取 ',filename)
                ulist, plist = check_page(url,low,high,url_list,high_price,page_list)
    #            print('这是list', ulist)
                download_info(ulist,plist,filename)
                end_time = ctime()
                print('开始于', start_time)
                print('结束于', end_time)
            #break

def download_info(url_list,page_list,filename):
    info_list = {}
    info_list['标题'] = []
    info_list['出租形式'] = []
    info_list['户型'] = []
    info_list['平方'] = []
    info_list['层数'] = []
    info_list['朝向'] = []
    info_list['价格'] = []
    info_list['位置'] = []
    info_list['经纬度'] = []
    i = -1
    for url in url_list:
        i += 1
        page = int(page_list[i])
        for page_info in range(1,page+1):
            page_url = url+'-i3%s'%page_info
            data = get_urldata(page_url)
#            print(page_url)
            biaoti_info = re.findall(r'blank" title="(.*?)">', data)
            jiage_info = re.findall(r'price">([0-9]*)</span>', data)
            list = re.findall(r'mt20 bold">\s*(.*)\r', data)#[0]
            weizhi_info = re.findall(r'blank"><span>(.*)</p>', data)
            for weizhi in weizhi_info:
                info_list['位置'].append(re.sub(r'<.*?>', '', weizhi))
                info_list['经纬度'].append('无数据')
            for biaoti in biaoti_info:
                info_list['标题'].append(biaoti)
            for jiage in jiage_info:
                info_list['价格'].append(jiage)
            for single in list:
                single = single.split('<span class="splitline">|</span>')
                # 当前可用，但应设计更好的判断方法
                while len(single) < 5:#存在朝向不存在的情况
                    single.append('无朝向数据')
                info_list['出租形式'].append(single[0])
                info_list['户型'].append(single[1])
                info_list['平方'].append(single[2])
                info_list['层数'].append(single[3])
                info_list['朝向'].append(single[4])
            #print(info_list)
            #print(ctime(),len(info_list['标题']),len(info_list['出租形式']),len(info_list['户型']),len(info_list['平方']),len(info_list['层数']),len(info_list['朝向']),len(info_list['价格']),len(info_list['位置']))
            #break
        #print(ctime(),len(info_list['标题']),len(info_list['出租形式']),len(info_list['户型']),len(info_list['平方']),len(info_list['层数']),len(info_list['朝向']),len(info_list['价格']),len(info_list['位置']))
        print(info_list)#break
    #a = pd.DataFrame(info_list)
    #Ffilename = ''.join(pypinyin.lazy_pinyin(filename))#csv文件名为中文的话  读取时会报OSError
    #a.to_csv('./房价/%s.csv'%Ffilename)
#朝向#平方 户型 整租 价格 标题 位置 层数

def mysql():
    con = MySQLdb.connect(host='45.77.22.77',port = 3306, user="root", passwd="YES", db="Hotel")
    cur = con.cursor()
    cur.execute("insert into hotel_hotel (name,score,img,address,description) values (%s,%s,%s,%s,%s)",
                (name, score, img, finallyaddress, dis))
    con.commit()
    con.close()
    print('成功添加No.%d' % i, name)

if __name__ == '__main__':
    main_url = 'http://zu.sz.fang.com'
    get_detailpage(main_url)
    #main()