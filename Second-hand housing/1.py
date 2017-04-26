# -*- coding:utf-8 -*-
import gzip, traceback, re, sys
from time import ctime, sleep
from threading import Lock, current_thread, Thread
from urllib import request


def read_url(req,error_num):#获取网页信息
    #req.add_header('User-Agent','Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36')
    try:#网页压缩的情况下使用
        print('!!!!!!!!!!!!!!!!!!开始try!!!!!!!!!!!!!!!!!!')
        data = request.urlopen(req).read()
        print('get data successful')
        data = gzip.decompress(data).decode("gb18030")
        print('结束get_urldata')
        with open("./cache.txt", 'w') as f:
            f.write(str(data))
    except TimeoutError:
        print('!!!!!!!!!!!!!!TimeoutError!!!!!!!!!!!!!!!!!!!!!!')
        sleep(1)
        print('访问请求失败，睡眠1s')
        if error_num < 10:
            error_num += 1
            read_url(req, error_num)
        else:
            pass
    except OSError:#网页未压缩的情况下使用
        print('!!!!!!!!!!!!!!OSError!!!!!!!!!!!!!!!!!!!!!!')
        try:
            data = request.urlopen(req).read().decode("gb18030")
            print('结束get_urldata')
            with open("./cache.txt", 'w') as f:
                f.write(str(data))
        except Exception as e:
            if error_num < 10:
                error_num += 1
                read_url(req, error_num)
            else:
                pass
    except Exception as e:
        print('!!!!!!!!!!!!!!Exception!!!!!!!!!!!!!!!!!!!!!')
        print(traceback.format_exc())
        sleep(1)
        print('访问请求失败，睡眠1s')
        if error_num < 10:
            error_num += 1
            read_url(req, error_num)
        else:
            pass


def get_urldata():
    main_url = sys.argv[1]
    print('这是main_url',main_url)
    error_num = 0
    t1 = Thread(target=read_url, args=(main_url,error_num))
    t1.start()
    print('这是子进程活动状态',t1.isAlive())
    t1.join(15)
    if t1.isAlive():
        get_urldata()
    else:
        pass


if __name__ == '__main__':
    get_urldata()
