# -*- coding:utf-8 -*-

import urllib, gzip, traceback, re, pymysql
import pandas as pd
from time import ctime, sleep
from email.mime.text import MIMEText
import smtplib
from limit import *


def get_areaurl(main_url):#获取该城市所有地区链接
    data = get_urldata(main_url)
    urllist = re.findall(r'<a  href=(.*?) >(.{1,7})</a',re.findall(r'\'/house/\'\)">不限</a>(.*)', data)[0])
    city = re.findall(r'content="province=.*?;city=(.*?);"', data)[0]
    return urllist, city


def main():#留用最后汇总城市
    pass


def get_detailpage(main_url):
    start_time = ctime()
    area_url, city = get_areaurl(main_url)
    print('这是area_url', area_url)
    for url_info in area_url:
        detail_url = main_url + url_info[0]
        url_list = []
        page_list = []# 81 79 82 75
        already_list = []
        if detail_url not in already_list:
            low = 0
            high = 50
            qu = url_info[-1]
            print(city,qu)
            print('开始获取 ', city, qu)
            url_list,page_list = limit_area(main_url, detail_url, low, high)
            print(url_list,page_list)
            download_info(url_list, page_list, city, qu)
            end_time = ctime()
            print('开始于', start_time)
            print('结束于', end_time)
            # break
    sent_email('%s获取完成'%city,'%s获取完成'%city)


def download_info(url_list, page_list, city, qu):
    info_list = []
    #print(page_list)
    #print(url_list)
    i = -1
    for url in url_list:
        i += 1
        #print(i)
        #print(page_list[i])
        page = int(page_list[i])
        for page_info in range(1, page+1):
            page_url = url+'-i3%s' % page_info
            #print(page_url)
            data = get_urldata(page_url)
           # print(page_url)
            biaoti_info = re.findall(r'.htm".*_blank".*?>(.{1,}?)</a>', data)
            jiage_info = re.findall(r'class="price">(.*?)</span>', data)
            danjia_info = re.findall(r'mt5">(.*?)元<span', data)
            list_info = re.findall(r'<p class="mt12">((.*?\s){4,6}?) *</p>', data)#
            list_info = list(map(lambda i: i[0].replace("\r\n                            ", '').replace('\'', '"'), list_info))
            weizhi_info = re.findall(r'title=.*<span>(.*)</span>', data)
            pingfang_info = re.findall(r'<p>([0-9]{1,5}㎡)</p>', data)
            print(len(biaoti_info),len(jiage_info),len(danjia_info),len(list_info),len(weizhi_info),len(pingfang_info))
            for num in range(len(weizhi_info)):
                weizhi = re.sub(r'<.*?>', '', weizhi_info[num])
                single = list_info[num].split('<span class="line">|</span>')
                while len(single) < 5:#存在建筑年代不存在的情况
                    single.append('未知建筑年代')
                info = [biaoti_info[num], qu, jiage_info[num]+'万', float(danjia_info[num]), weizhi, single[0], pingfang_info[num], single[1], single[2], single[3].lstrip("建筑年代："), '无数据']
                #print(info)
                info_list.append(info)
            print(ctime(), '完成第%s页' % page_info)
    mysql(city, info_list)


def sent_email(text,subject):
    msg = MIMEText('%s'%text, 'plain', 'utf-8')
    msg['From'] = 'JD.com<JD@163.com>'
    msg['To'] = 'sunner648@163.com'
    msg['Subject'] = '%s'%subject
    from_addr = "*******@qq.com"
    password = "******"
    to_addr = "******@163.com"
    smtp_server = "smtp.qq.com"
    try:
        server = smtplib.SMTP(smtp_server,587)
        server.starttls()
        server.login(from_addr, password)
        server.sendmail(from_addr, [to_addr], msg.as_string())
        server.quit()
    except Exception as e:
        print('邮件模块出现错误')



def mysql(city,data):
    con = pymysql.connect(host='*******', port=****, user="********", passwd="********", db="********",charset='utf8')
    cur = con.cursor()
    #biaoti_info[num], qu, int(jiage_info[num]), float(danjia_info[num]), weizhi, single[0], single[1], single[2], single[3], '无数据'
    sql = "CREATE TABLE if not exists %s"%city + "(id int(10) primary key  auto_increment not null,标题 CHAR(50),区位 CHAR(10),价格 CHAR(10),单价 FLOAT(10), 位置 CHAR(50),户型 CHAR(8),平方 CHAR(6),层数 CHAR(20),朝向 CHAR(6),建筑年代 CHAR(8), 经纬度 CHAR(50))"
    cur.execute(sql)
    sql1 = "insert into %s"%city + " (标题,区位,价格,单价,位置,户型,平方,层数,朝向,建筑年代,经纬度) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    cur.executemany(sql1,data)
    print('链接成功')
    cur.close()
    con.commit()
    con.close()

if __name__ == '__main__':
    main_url = 'http://esf.fangchenggang.fang.com'
    get_detailpage(main_url)
    #main()
