# -*- coding:utf-8 -*-
import urllib, gzip, traceback, re, pymysql, smtplib, threadpool
import pandas as pd
from time import ctime, sleep
from email.mime.text import MIMEText
from limit import *
from threading import Lock, current_thread, Thread
from functools import reduce


def get_areaurl(main_url):#获取该城市所有地区链接
    data = get_urldata(main_url,error_num=0, must=2)
    if data:
        data1 = re.findall(r'\'/house/\'\)">不限</a>(.*)', data)
        urllist = re.findall(r'<a  href=(/.*?/).*? >(.{1,7})</a',data1[0])
        city = re.findall(r'content="province=.*?;city=(.*?);.*?"', data)[0]
        return urllist, city
    else:
        return data, data

def main():#留用最后汇总城市
    with open('./搜房.txt', encoding='gbk') as f:
        a = f.readlines()
    url_list = re.findall(r'(http://[\w\./]{5,100})', ''.join(a))
    print('这是总城市信息', url_list)
    with open('./已完成城市.txt', encoding='utf-8') as fe:
        b = list(map(lambda x:x.replace('\n',''), fe.readlines()))
    for main_url in url_list:
        if main_url not in b:
            get_detailpage(main_url)


def get_detailpage(main_url):
    global timeout_error
    timeout_error = 0
    start_time = ctime()
    print(start_time)
    area_url, city = get_areaurl(main_url)
    print('这是area_url', area_url)
    if area_url:
        for url_info in area_url:
            detail_url = main_url + url_info[0]
            with open('./已完成区.txt', encoding='utf-8') as fr:
                already_list = fr.readlines()
                already_list = list(map(lambda x:x.replace("\n",""),already_list))
            qu = url_info[-1]
            print(already_list)
            if city + qu not in already_list:
                low = 0
                high = 50
                print('开始获取 ', city, qu)
                print("这是detailurl",detail_url)
                url_list,page_list = limit_area(main_url, detail_url, low, high)
                print(url_list,page_list)
                download_info(main_url,url_list, page_list, city, qu)
                end_time = ctime()
                print('开始于', start_time)
                print('结束于', end_time)
                with open('./已完成区.txt', 'a+', encoding='utf-8') as fe:
                    fe.write(city + qu + '\n')
        with open('./已完成城市.txt', 'a+', encoding='utf-8') as f:
            f.write(main_url + '\n')
        sent_email('%s获取完成\n开始于%s\n结束于%s'%(city,start_time,ctime()), '%s获取完成'%city)
    else:
        print('超时无法获取当前城市，自动跳入下一城市')


def download_info(main_url,url_list, page_list, city, qu):
    #print(page_list)
    #print(url_list)
    i = -1
    pool = threadpool.ThreadPool(15)
    with open("./info.txt", 'w') as f:
        f.write('')
    for url in url_list:
        i += 1
        #print(i)
        print(page_list[i:])
        print('还剩',reduce(lambda x, y: x + y, page_list[i:]),'页')
        page = int(page_list[i])
        thread_list = []
        finally_list = []
        for page_info in range(1, page+1):
            if url.endswith('/'):
                page_url = url + 'i3%s' % page_info
            else:
                page_url = url+'-i3%s' % page_info
            #print(page_url)
            thread_list.append(([page_url, qu, city, finally_list], None))
        '''t1 = Thread(target=threading_task, args=(page_url, qu, city, finally_list))
            thread_list.append(t1)
            #t1.start()
            #t1.join()
            #info = threading_task(page_url,qu)
            #info_list.append(info)
            #print(ctime(), '完成%s页'%page_info)
            #print(thread_list)
        if len(thread_list) <33:
            for j in thread_list:
                j.start()
            for h in thread_list:
                h.join()
        elif len(thread_list) < 66:
            for j in thread_list[0:33]:
                j.start()
            for h in thread_list[0:33]:
                h.join()
            sleep(10)
            for j in thread_list[33:]:
                j.start()
            for h in thread_list[33:]:
                h.join()
        else:
            for j in thread_list[0:33]:
                j.start()
            for h in thread_list[0:33]:
                h.join()
            sleep(10)
            for j in thread_list[33:66]:
                j.start()
            for h in thread_list[33:66]:
                h.join()
            sleep(10)
            for j in thread_list[66:]:
                j.start()
            for h in thread_list[66:]:
                h.join()
        print(len(finally_list))'''
        requests = threadpool.makeRequests(threading_task, thread_list)
        for task in requests:
            global timeout_error
            if timeout_error >50:
                get_detailpage(main_url)
            pool.putRequest(task)
            sleep(0.3)
        pool.wait()
        with open("./info.txt", 'a', encoding='gb18030') as f:
            f.write(''.join(finally_list))
    with open("./info.txt", encoding='gb18030') as f:
        a = ''.join(f.readlines())
    b = a.strip('|@/').split('|@|/@/')
    info_list = list(map(lambda x: x.split("|@|"), b))
    if len(info_list)>2:
        city = city
        mysql(city, info_list)


def threading_task(url, qu, city, finally_list):
    data = get_urldata(url, error_num=0, must=0)
    if data == '':
        global timeout_error
        timeout_error += 1
        print(timeout_error)
    print('进入多线程')
    try:
        list_info = re.findall(r'<p class="mt12">([\s\S]*?)</p>', data)
        list_info = list(map(lambda i: i.replace("\n", '').replace(' ', '').replace('\'', '"'), list_info))
        weizhi_info = re.findall(r'title=.*<span>(.*)</span>', data)
        pingfang_info = re.findall(r'<p>([0-9]{1,5}㎡)</p>', data)
        biaoti_info = re.findall(r'.htm".*_blank".*?>(.{1,}?)</a>', data)
        jiage_info = re.findall(r'class="price">(.*?)</span>', data)
        danjia_info = re.findall(r'mt5">(.*?)元<span', data)
    except Exception:
        try:
            list_info = re.findall(r'<p class="mt12">([\s\S]*?)</p>', data)
            list_info = list(map(lambda i: i.replace("\n", '').replace(' ', '').replace('\'', '"'), list_info))
            weizhi_info = re.findall(r'title=.*<span>(.*)</span>', data)
            pingfang_info = re.findall(r'<p>([0-9]{1,5}㎡)</p>', data)
            biaoti_info = re.findall(r'.htm".*_blank".*?>(.{1,}?)</a>', data)
            jiage_info = re.findall(r'class="price">(.*?)</span>', data)
            danjia_info = re.findall(r'mt5">(.*?)元<span', data)
        except Exception:
            list_info = re.findall(r'<p class="mt12">([\s\S]*?)</p>', data)
            list_info = list(map(lambda i: i.replace("\n", '').replace(' ', '').replace('\'', '"'), list_info))
            weizhi_info = re.findall(r'title=.*<span>(.*)</span>', data)
            pingfang_info = re.findall(r'<p>([0-9]{1,5}㎡)</p>', data)
            biaoti_info = re.findall(r'.htm".*_blank".*?>(.{1,}?)</a>', data)
            jiage_info = re.findall(r'class="price">(.*?)</span>', data)
            danjia_info = re.findall(r'mt5">(.*?)元<span', data)
    print(len(biaoti_info), len(jiage_info), len(danjia_info), len(list_info), len(weizhi_info), len(pingfang_info), url)
    for num in range(len(weizhi_info)):
        weizhi = re.sub(r'<.*?>', '', weizhi_info[num])
        single = list_info[num].split('<spanclass="line">|</span>')
        while len(single) < 5:  # 存在建筑年代不存在的情况
            single.append('未知建筑年代')
        single = list(map(lambda x:x.replace('\n', '').replace('\r', ''),single))
        info = [city + qu, biaoti_info[num], jiage_info[num] + '万', danjia_info[num], weizhi,single[0],
                pingfang_info[num], single[1], single[3].lstrip("建筑年代："), '无数据', '/@/']
        info1 = '|@|'.join(info)
        finally_list.append(info1)
    print(ctime())


def sent_email(text,subject):
    msg = MIMEText('%s'%text, 'plain', 'utf-8')
    msg['From'] = 'JD.com<sunner649@163.com>'
    msg['To'] = 'sunner648@163.com'
    msg['Subject'] = '%s'%subject
    from_addr = "1157661539@qq.com"
    password = "xynyyffntjscidac"
    to_addr = "sunner648@163.com"
    smtp_server = "smtp.qq.com"
    try:
        server = smtplib.SMTP(smtp_server,587)
        server.starttls()
        server.set_debuglevel(1)
        server.login(from_addr, password)
        server.sendmail(from_addr, [to_addr], msg.as_string())
        server.quit()
        print('邮件发送成功')
    except Exception as e:
        print(e)
        print('邮件模块出现错误')



def mysql(city,data):
    print('准备传输到mysql')
    try:
        con = pymysql.connect(host='107.191.61.150', port=3306, user="soufangwang", passwd="XI6DLZ7PAcXLteVi",
                              db="ershoufang", charset='utf8')
        cur = con.cursor()
        print('游标创建成功')
        #biaoti_info[num], qu, int(jiage_info[num]), float(danjia_info[num]), weizhi, single[0], single[1], single[2], single[3], '无数据'
        sql = "CREATE TABLE if not exists %s"%city + "(id int(10) primary key  auto_increment not null,区位 CHAR(10),标题 CHAR(50),价格 CHAR(10),单价 FLOAT(10), 位置 CHAR(50),户型 CHAR(8),平方 CHAR(6),层数 CHAR(20),建筑年代 CHAR(8), 经纬度 CHAR(50))"
        cur.execute(sql)
        print('查询成功')
        sql1 = "insert into %s"%city + " (区位,标题,价格,单价,位置,户型,平方,层数,建筑年代,经纬度) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        cur.executemany(sql1,data)
        print('链接成功')
        cur.close()
        con.commit()
        con.close()
    except Exception as e:
        print('发生错误')
        print(e)
        try:
            cur.close()
            con.rollback()
            con.close()
        except Exception:
            pass
        mysql(city,data)



def hbase():
    pass

if __name__ == '__main__':
    #main_url = 'http://esf.sh.fang.com'
    #get_detailpage(main_url)
    main()
