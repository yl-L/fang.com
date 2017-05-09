# -*- coding:utf-8 -*-

import urllib, gzip, traceback, re, pymysql
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
    if high <= high_price:
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
                    print(high, low)
                    low = high + 1
                    high = low + 2999
                    print('空  不放入链接',not_found)
                    check_page(url, low, high, url_list, high_price,page_list)
            else:#当只有多页且显示有房源时
                low = high + 1
                high = low + 2999
                url_list.append(detail_url)
                page_list.append(page)
               # print('正在放入链接', url_list)
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
        if page<100:
            url_list.append(detail_url)
        else:#添加更进一步限制，例如北京等城市
            url_list = add_more_limit(url,url_list)
    return url_list

def add_more_limit():
    limit = ['-a573','-a574','-a575','-a576','-a577','-a578','-a579','-a580','-a5120']
    for i in limit:
        detail_url = url+i
        data = get_urldata(detail_url)
        page = int(re.findall(r'"txt">共([0-9]{1,4})页</span>', data)[0])
        print(page)
        print('这是添加限制的url',detail_url)
    return url_list
# g21,g22,g23,g24,g299
# 'a573','-a574','-a575','-a576','-a577','-a578','-a579','-a580','-a5120'

def get_detailpage(main_url):
    start_time = ctime()
    area_url = get_areaurl(main_url)
    print('这是area_url', area_url)
    finally_list = []
    for url in area_url:
        url_list = []
        page_list = []# 81 79 82 75
        already_list = ['http://zu.cq.fang.com/house-a057/', 'http://zu.cq.fang.com/house-a058/', 'http://zu.cq.fang.com/house-a059/', 'http://zu.cq.fang.com/house-a061/', 'http://zu.cq.fang.com/house-a056/', 'http://zu.cq.fang.com/house-a060/', 'http://zu.cq.fang.com/house-a062/', 'http://zu.cq.fang.com/house-a063/']
        if url not in already_list:
            low = 0
            high = 1000
            price_url = url + 'h33'
            data = get_urldata(price_url)
            high_price = int(re.findall(r'"price">([0-9]*?)</span>',data)[0])
            print(high_price)
            city = re.findall(r'province=.*city=(.{1,5});coord=',data)[0]
            qu = re.findall(r'class="term">(.{1,6})</a>',data)[0]
            print(city,qu)
            print('开始获取 ',city,qu)
            ulist, plist = check_page(url,low,high,url_list,high_price,page_list)
           # print('这是list', ulist)
            download_info(ulist,plist,city,qu)
            end_time = ctime()
            print('开始于', start_time)
            print('结束于', end_time)
            # break

def download_info(url_list,page_list,city,qu):
    info_list = []
    #print(page_list)
    #print(url_list)
    i = -1
    for url in url_list:
        i += 1
        #print(i)
        #print(page_list[i])
        page = int(page_list[i])
        for page_info in range(1,page+1):
            page_url = url+'-i3%s'%page_info
            #print(page_url)
            data = get_urldata(page_url)
           # print(page_url)
            biaoti_info = re.findall(r'blank" title="(.*?)">', data)
            jiage_info = re.findall(r'price">([0-9]*)</span>', data)
            list = re.findall(r'mt20 bold">\s*(.*)\r', data)#[0]
            weizhi_info = re.findall(r'blank"><span>(.*)</p>', data)
            for num in range(len(weizhi_info)):
                weizhi = re.sub(r'<.*?>', '', weizhi_info[num])
                single = list[num].split('<span class="splitline">|</span>')
                while len(single) < 5:#存在朝向不存在的情况
                    single.append('无朝向数据')
                info = [biaoti_info[num],qu,int(jiage_info[num]),weizhi,single[0],single[1],single[2],single[3],single[4],'无数据']
                #print(info)
                info_list.append(info)
            print(ctime(),'完成第%s页'%page_info)
    mysql(city,info_list)





def mysql(city,data):
    con = pymysql.connect(host='********', port=****, user="********", passwd="********", db="********",charset='utf8')
    cur = con.cursor()
    #标题,区位,价格,位置,出租形式,户型,平方,层数,朝向,经纬度                                                                                    
    sql = "CREATE TABLE if not exists %s"%city + "(id int(10) primary key  auto_increment not null,标题 CHAR(50),区位 CHAR(10),价格 INT(10),位置 CHAR(50),出租形式 CHAR(6),户型 CHAR(6),平方 CHAR(6),层数 CHAR(20),朝向 CHAR(6),经纬度 CHAR(50))"
    cur.execute(sql)
    sql1 = "insert into %s"%city + " (标题,区位,价格,位置,出租形式,户型,平方,层数,朝向,经纬度) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    cur.executemany(sql1,data)
    print('链接成功')
    cur.close()
    con.commit()
    con.close()

if __name__ == '__main__':
    main_url = 'http://zu.cq.fang.com'
    get_detailpage(main_url)
    #main()
