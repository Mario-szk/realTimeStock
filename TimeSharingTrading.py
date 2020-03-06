import datetime
import time
import requests
from queue import Queue
import threading
from apscheduler.schedulers.blocking import BlockingScheduler
import DataHandle

# 从数据库获取，这里模拟数据
stockCodeList = [("600036", "sh"), ("300125", "sz")]
threads = []
queue = Queue(maxsize=1000)  # 设置队列最大空间为1000
result_queue = Queue()


def startJob():
    while time.localtime().tm_hour < 15:
        producer_thread = threading.Thread(target=producer, args=(queue, [False, []]), name='producer_thread')
        producer_thread.start()
        queue.join()
        time.sleep(60*5)


def producer(in_q, ready_list):  # 生产者
    while in_q.full() is False:
        if ready_list[0]:
            return
        for i, j in stockCodeList:
            url = 'http://push2ex.eastmoney.com/getStockFenShi?pagesize=100' \
                   '&ut=7eea3edcaed734bea9cbfc24409ed989&dpt=wzfscj&pageindex=0&sort=2&ft=1&market=1&code=' + str(i)
            if len(ready_list[1]) == len(stockCodeList):
                ready_list[0] = True
            if (url, i) not in ready_list[1]:
                ready_list[1].append((url, i))
                in_q.put((url, i))


def consumer(in_q):  # 消费者
    headers = {}
    while True:
        rdate = in_q.get()
        data = requests.get(url=rdate[0], headers=headers).json()
        if data['data'] != None:
            data = data['data']['data']
            for j in data:
                DataHandle.inserTimeShareTrade((rdate[1], (j['t'], float(j['p'])/100, j['v'])))
        in_q.task_done()  # 通知生产者，队列已消化完



if __name__ == '__main__':
    for index in range(20):  # 20个线程爬取
        consumer_thread = threading.Thread(target=consumer, args=(queue,), daemon=True)
        consumer_thread.start()
        threads.append(consumer_thread)

    scheduler = BlockingScheduler()
    # 开启任务
    scheduler.add_job(startJob, 'cron', hour=9, minute=26)
    scheduler.start()