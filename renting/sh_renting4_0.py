# -*- coding:utf-8 -*-
import gzip
import traceback
import re
import pymysql
import smtplib
import threadpool
import pandas as pd
from time import ctime, sleep
from email.mime.text import MIMEText
from urllib import request
from functools import reduce


def get_urldata(main_url, must=1, error=0):
    # 获取网页信息
    req = request.Request(main_url)
    req.add_header('User-Agent', 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; '
                                 'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1) ; 360SE)')
    try:
        # 网页压缩的情况下使用
        print('开始获取%s' % main_url)
        data = request.urlopen(req, timeout=15).read()
        data = gzip.decompress(data).decode("gb18030")
        return data
    except TimeoutError:
        sleep(1)
        print('访问请求失败，睡眠1s')
        error += 1
        if error < 5:
            get_urldata(main_url, must, error)
        else:
            data = '0'
            return data
    except OSError as e:
        #  网页未压缩的情况下使用
        print('OSerror!!!!!!!!!!!!!!!')
        if str(e) == 'timed out':
            print('超时重新获取')
            error += 1
            if must == 1:
                if error < 5:
                    get_urldata(main_url, must, error)
                else:
                    data = ' '
                    return data
            else:
                data = ' '
                return data
        elif str(e) in ["Not a gzipped file (b'\\r\\n')", "Not a gzipped file (b' <')"]:
            try:
                data = request.urlopen(req, timeout=15).read().decode("gb18030")
                return data
            except Exception as e:
                print(e)
                get_urldata(main_url, must, error)
        else:
            print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
            print(e, main_url)
            print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
            if must == 1:
                get_urldata(main_url, must, error)
            else:
                data = ' '
                return data
    except Exception as e:
        print(e)
        print(traceback.format_exc())
        if must == 1:
            get_urldata(main_url, must, error)
        else:
            data = ' '
            return data


def get_areaurl(main_url):
    # 获取该城市所有地区链接
    data = get_urldata(main_url, must=2)
    print(main_url)
    while data is None:
        print(data)
        data = get_urldata(main_url)
    if data != ' ':
        a = re.findall(r'<a href="javascript:void.*?不限(.*)', data)
        urllist = re.findall(r'<a href="(.*?)" >(.{1,13})</a>', a[0])
        city = re.findall(r'content="province=.*?;city=(.*?);.*?"', data)[0]
    else:
        urllist = []
        city = ''
    return urllist, city


def main():
    # 留用最后汇总城市
    with open('./租房城市列表.txt', encoding='gbk') as f:
        a = f.readlines()
    url_list = re.findall(r'(http://[\w./]{5,100})', ''.join(a))
    print('这是总城市信息', url_list)
    with open('./租房已完成城市.txt', encoding='gbk') as fe:
        b = list(map(lambda x: x.replace('\n', ''), fe.readlines()))
    for url in url_list:
        if url not in b:
            get_detailpage(url)


def get_detailpage(main_url):
    start_time = ctime()
    area_url, city = get_areaurl(main_url)
    if city != '':
        print('这是area_url', area_url)
        for url in area_url:
            detail_url = main_url + url[0]
            url_list = []
            page_list = []
            # 81 79 82 75
            qu = url[-1]
            with open('./租房已完成区.txt', encoding='utf-8') as fr:
                already_list = fr.readlines()
                already_list = list(map(lambda x: x.replace("\n", ""), already_list))
            if city + qu not in already_list:
                price_url = str(detail_url) + 'h33'
                data = get_urldata(price_url)
                while data is None:
                    data = get_urldata(price_url)
                if not re.findall(r'很抱歉，没有找到', data):
                    high_price = int(re.findall(r'"price">([0-9]*?)</span>', data)[0])
                    low = 0
                    high = high_price
                    print(high_price)
                    city = re.findall(r'province=.*city=(.{1,7});coord=', data)[0]
                    qu = re.findall(r'class="term">(.{1,13})</a>', data)[0]
                    print(city, qu)
                    print('开始获取 ', city, qu)
                    ulist, plist = check_page(detail_url, low, high, url_list, high_price, page_list)
                    # print('这是list', ulist)
                    download_info(ulist, plist, city, qu)
                end_time = ctime()
                with open('./租房已完成区.txt', 'a+', encoding='utf-8') as fe:
                    fe.write(city + qu + '\n')
                print('开始于', start_time)
                print('结束于', end_time)
                # break
        end_time = ctime()
        with open('./租房已完成城市.txt', 'a+', encoding='utf-8') as f:
            f.write(main_url + '\n')
        sent_email('%s获取完成\n开始于%s\n结束于%s' % (city, start_time, end_time), '租房信息获取完成')
    else:
        print('超时无法获取当前城市，自动跳入下一城市')


def download_info(url_list, page_list, city, qu):
    info_list = []
    # print(page_list)
    # print(url_list)
    print(len(page_list), len(url_list))
    i = -1
    pool = threadpool.ThreadPool(13)
    for url in url_list:
        i += 1
        thread_list = []
        # print(i)
        # print(page_list[i])
        page = int(page_list[i])
        print('开始获取%s共%s页' % (url, page))
        print(page_list[i:])
        print('还剩', reduce(lambda x, y: x + y, page_list[i:]), '页')
        for page_info in range(1, page+1):
            page_url = url+'-i3%s' % page_info  # print(page_url)
            thread_list.append(([page_url, qu, city, info_list], None))
        requests = threadpool.makeRequests(threading_task, thread_list)
        for task in requests:
            pool.putRequest(task)
            sleep(0.5)
        pool.wait()
    # print(info_list)
    mysql(city, info_list)


def threading_task(page_url, qu, city, info_list):
    print('进入多线程')
    data = get_urldata(page_url)
    # print(page_url)
    while data is None:
        data = get_urldata(page_url)
    biaoti_info = re.findall(r'blank" title="(.*?)">', data)
    jiage_info = re.findall(r'price">([0-9]*)</span>', data)
    list1 = re.findall(r'mt20 bold">\s*(.*)\r', data)
    weizhi_info = re.findall(r'blank"><span>(.*)</p>', data)
    for num in range(len(weizhi_info)):
        weizhi = re.sub(r'<.*?>', '', weizhi_info[num])
        single = list1[num].split('<span class="splitline">|</span>')
        while len(single) < 5:  # 存在朝向不存在的情况
            single.append('无朝向数据')
        info = [city+qu, biaoti_info[num], int(jiage_info[num]), weizhi, single[0], single[1], single[2], single[3],
                single[4], '无数据']
        info_list.append(info)
    print(ctime(), '%s完成' % page_url)


def check_page(url, low, high, url_list, high_price, page_list):  # 检查页面是否超过80（上限100）
    if low < high_price:
        detail_url = url + 'c2%s-d2%s' % (low, high)
        data = get_urldata(detail_url)
        while data is None:
            data = get_urldata(detail_url)
        page = int(re.findall(r'"txt">共([0-9]{1,4})页</span>', data)[0])
        print('这是page', page)
        not_found = re.findall(r'很抱歉，没有找到符合条件的房源', data)
        if page < 100:  # 只有当小于100（页面不满时）爬取
            if page == 1 and not_found:  # 当只有一页且显示没有房源时
                print(high, low)
                low = high + 1
                high = high_price
                print('空  不放入链接', not_found)
                check_page(url, low, high, url_list, high_price, page_list)
            else:  # 当只有多页且显示有房源时
                low = high + 1
                high = high_price
                url_list.append(detail_url)
                page_list.append(page)
                print('这是url', detail_url)
                # print('正在放入链接', url_list)
                check_page(url, low, high, url_list, high_price, page_list)
        else:
            if high <= low:  # 如果高低价位重合，则添加新的限制板块
                # 更加细分  调用方法
                print('开始增加限制')
                url_list, page_list = add_limit(detail_url, url_list, page_list)
                low = high + 1
                high = high_price
                check_page(url, low, high, url_list, high_price, page_list)
            else:
                # 如果页面数为100，则继续细分价格区间
                high = (high-low-1)//5+low-1
                check_page(url, low, high, url_list, high_price, page_list)
    return url_list, page_list


def add_limit(url, url_list, page_list):
    limit = ['-g21', '-g22', '-g23', '-g24', '-g299']
    for i in limit:
        detail_url = url+i
        data = get_urldata(detail_url)
        while data is None:
            data = get_urldata(detail_url)
        page = int(re.findall(r'"txt">共([0-9]{1,4})页</span>', data)[0])
        print(page)
        print('这是添加限制的url', detail_url)
        if page < 100:
            url_list.append(detail_url)
            page_list.append(page)
        else:  # 添加更进一步限制，例如北京等城市
            url_list, page_list = add_more_limit(url, url_list, page_list)
    return url_list, page_list


def add_more_limit(url, url_list, page_list):
    limit = ['-a573', '-a574', '-a575', '-a576', '-a577', '-a578', '-a579', '-a580', '-a5120']
    for i in limit:
        detail_url = url+i
        data = get_urldata(detail_url)
        while data is None:
            data = get_urldata(detail_url)
        page = int(re.findall(r'"txt">共([0-9]{1,4})页</span>', data)[0])
        print(page)
        if page < 100:
            url_list.append(detail_url)
            page_list.append(page)
            print('这是添加限制的url', detail_url)
        else:
            print('需要进一步细分，请增加限制模块')
    return url_list, page_list
# g21,g22,g23,g24,g299
# 'a573','-a574','-a575','-a576','-a577','-a578','-a579','-a580','-a5120'


def sent_email(text, subject):
    msg = MIMEText('%s' % text, 'plain', 'utf-8')
    msg['From'] = 'JD.com<sunner649@163.com>'
    msg['To'] = 'sunner648@163.com'
    msg['Subject'] = '%s' % subject
    from_addr = "1157661539@qq.com"
    password = "xynyyffntjscidac"
    to_addr = "sunner648@163.com"
    smtp_server = "smtp.qq.com"
    try:
        server = smtplib.SMTP(smtp_server, 587)
        server.starttls()
        server.login(from_addr, password)
        server.sendmail(from_addr, [to_addr], msg.as_string())
        server.quit()
    except Exception as e:
        print(e)
        print('邮件模块出现错误')
        pass


def mysql(city, data):
    print('准备传输到MySQL')
    con = pymysql.connect(host='107.191.61.150', port=3306, user="soufangwang", passwd="XI6DLZ7PAcXLteVi",
                          db="renting", charset='utf8')
    cur = con.cursor()
    print('创建游标成功')
    # 标题,区位,价格,位置,出租形式,户型,平方,层数,朝向,经纬度
    detail = '(id int(10) primary key  auto_increment not null,区位 CHAR(20),标题 CHAR(50),价格 INT(10),' \
             '位置 CHAR(50),出租形式 CHAR(6),户型 CHAR(6),平方 CHAR(6),层数 CHAR(20),朝向 CHAR(6),经纬度 CHAR(50))'
    sql = "CREATE TABLE if not exists %s" % city + detail
    cur.execute(sql)
    create_sql = " (区位,标题,价格,位置,出租形式,户型,平方,层数,朝向,经纬度) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    sql1 = "insert into %s" % city + create_sql
    cur.executemany(sql1, data)
    print('链接成功')
    cur.close()
    con.commit()
    con.close()


if __name__ == '__main__':
    main()
