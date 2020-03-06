import datetime
import time

import requests
from queue import Queue
import threading


import DataHandle
from apscheduler.schedulers.blocking import BlockingScheduler

# 从数据库获取，这里模拟数据
stockCodeList = [("600036", "sh"), ("300125", "sz")]
threads = []
queue = Queue(maxsize=1000)  # 设置队列最大空间为1000
result_queue = Queue()
isFirst = True
historyQueue = Queue()


def startJob():
    global isFirst
    while time.localtime().tm_hour < 15:
        producer_thread = threading.Thread(target=producer, args=(queue, [False, []]), name='producer_thread')
        producer_thread.start()
        queue.join()
        isFirst = False
        time.sleep(60)
    isFirst = True
    producer_thread = threading.Thread(target=producer, args=(queue, [False, []]), name='producer_thread')
    producer_thread.start()
    queue.join()
    return


def ceartDb():
    for i in stockCodeList:
        DataHandle.creatDB(i)


def saveHistory(h_q):
    handle = h_q.get()
    re = requests.get(handle[1]).json()
    re = re['data']
    re = (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), re['f46'], re['f44'], re['f45'], re['f116'])
    DataHandle.inserHistoryTrade((handle[0], re))


def producer(in_q, ready_list):  # 生产者
    global isFirst
    while in_q.full() is False:
        if ready_list[0]:
            return
        for i, j in stockCodeList:
            if 'sh' == j:
                secid = '1.' + str(i)
            else:
                secid = '0.' + str(i)
            url1 = 'http://push2.eastmoney.com/api/qt/stock/get?fltt=2&fields=f530,f49,f161,f47,f48,f168&secid=' + secid
            url2 = 'http://push2.eastmoney.com/api/qt/stock/details/get?fields1=f1,f2,f3,f4&fields2=f51,f52,f53,f54,f55&secid=' + secid
            if isFirst:
                url3 = 'http://push2.eastmoney.com/api/qt/stock/get?fltt=2&fields=f44,f45,f46,f116&secid=' + secid
                historyQueue.put((i, url3))
            if len(ready_list[1]) == len(stockCodeList):
                ready_list[0] = True
            if (url1, url2, i) not in ready_list[1]:
                ready_list[1].append((url1, url2, i))
                in_q.put((url1, url2, i))


def consumer(in_q, out_q):  # 消费者
    headers = {}
    while True:
        rdate = in_q.get()
        data1 = requests.get(url=rdate[0], headers=headers)
        datalist = data1.json()['data']
        data2 = requests.get(url=rdate[1], headers=headers)
        prePrice = float(data2.json()['data']['prePrice'])
        readPrice = float(data2.json()['data']['details'][-1].split(',')[1])
        comparetoday = readPrice - prePrice
        percentage = 100 * comparetoday / prePrice
        out_q.put(
            (rdate[2], (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), readPrice, percentage, comparetoday,
                        datalist['f168'], datalist['f47'], datalist['f48'],
                        datalist['f20'], datalist['f19'], datalist['f18'], datalist['f17'], datalist['f16'],
                        datalist['f15'], datalist['f14'], datalist['f13'], datalist['f12'], datalist['f11'],
                        datalist['f40'], datalist['f39'], datalist['f38'], datalist['f37'], datalist['f36'],
                        datalist['f35'], datalist['f34'], datalist['f33'], datalist['f32'], datalist['f31'],
                        datalist['f161'], datalist['f49']
                        )))
        in_q.task_done()  # 通知生产者，队列已消化完


def RealtimeTradeConsumer(re_q):  # 消费者
    while True:
        rdate = re_q.get()
        DataHandle.inserRealtimeTrade(rdate)
        re_q.task_done()  # 通知生产者，队列已消化完


if __name__ == '__main__':
    consumer_thread = threading.Thread(target=saveHistory, args=(historyQueue,), daemon=True)
    consumer_thread.start()
    for index in range(20):  # 20个线程爬取
        consumer_thread = threading.Thread(target=consumer, args=(queue, result_queue,), daemon=True)
        consumer_thread.start()
        threads.append(consumer_thread)

    for index in range(5):  # 5个储存
        consumer_thread2 = threading.Thread(target=RealtimeTradeConsumer, args=(result_queue,), daemon=True)
        consumer_thread2.start()
        threads.append(consumer_thread2)

    scheduler = BlockingScheduler()
    # 开启任务
    scheduler.add_job(startJob, 'cron', hour=16, minute=57)
    # scheduler.add_job(func=startJob, trigger='cron', month='1-12', day='*', hour='9-17', minute='*')
    # 开启新的记录表
    scheduler.add_job(ceartDb, 'cron', hour=2, minute=0)
    scheduler.start()
