
import time
import pymysql as pyq

stock_timeshare_trade_inser = "(`dealtime`, `transactionprice`, `numberhand`) VALUES (%s, %s, %s)"

stock_timeshare_trade_creattable = '''  (
  `dealtime` int NOT NULL COMMENT '交易时间',
  `transactionprice` double NULL COMMENT '成交价',
  `numberhand` int(0) NULL COMMENT '手数',
  PRIMARY KEY (`dealtime`)
);'''


stock_history_trade_inser = "(`tradetime`, ` openingprice`, `topprice`, " \
                            "`minimumprice`, `totalvalue`) VALUES (%s, %s, %s, %s, %s);"

stock_realtime_trade_inser = "(`tradetime`, `updown`, `percentage`, `comparetoday`, " \
                             "`changehands`, `turnover`, `volumetransaction`, `buy1`," \
                             " `buy1per`, `buy2`, `buy2per`, `buy3`, `buy3per`, `buy4`, " \
                             "`buy4per`, `buy5`, `buy5per`, `sell1`, `sell1per`, `sell2`, " \
                             "`sell2per`, `sell3`, `sell3per`, `sell4`, `sell4per`, `sell5`, " \
                             "`sell5per`, `inner`, `outer`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, " \
                             "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"

stock_history_daily_trade_creattable = "  (" \
                                       "`tradetime` datetime(0) NULL COMMENT '爬取时间'," \
                                       "` openingprice` double NULL COMMENT '开盘价'," \
                                       "`topprice` double NULL COMMENT '最高价'," \
                                       "`minimumprice` double NULL COMMENT '最低价'," \
                                       "`totalvalue` double NULL COMMENT '总市值');"

stock_realtime_trade_creattable = " ( " \
                                  "`tradetime` datetime(0) NULL COMMENT '爬取时间'," \
                                  "`updown` DOUBLE NULL COMMENT '实时涨跌'," \
                                  "`percentage` DOUBLE NULL COMMENT '涨跌百分比%'," \
                                  "`comparetoday` DOUBLE NULL COMMENT '与今开比较'," \
                                  "`changehands` DOUBLE NULL COMMENT '换手率%'," \
                                  "`turnover` BIGINT ( 0 ) NULL COMMENT '成交量'," \
                                  "`volumetransaction` BIGINT ( 0 ) NULL COMMENT '成交额'," \
                                  "`buy1` INT ( 0 ) NULL COMMENT '买1'," \
                                  "`buy1per` DOUBLE NULL COMMENT '买1%'," \
                                  "`buy2` INT ( 0 ) NULL COMMENT '买2'," \
                                  "`buy2per` DOUBLE NULL COMMENT '买2%'," \
                                  "`buy3` INT ( 0 ) NULL COMMENT '买3'," \
                                  "`buy3per` DOUBLE NULL COMMENT '买3%'," \
                                  "`buy4` INT ( 0 ) NULL COMMENT '买4'," \
                                  "`buy4per` DOUBLE NULL COMMENT '买4%'," \
                                  "`buy5` INT ( 0 ) NULL COMMENT '买5'," \
                                  "`buy5per` DOUBLE NULL COMMENT '买5%'," \
                                  "`sell1` INT ( 0 ) NULL COMMENT '卖1'," \
                                  "`sell1per` DOUBLE NULL COMMENT '卖1%'," \
                                  "`sell2` INT ( 0 ) NULL COMMENT '卖2'," \
                                  "`sell2per` DOUBLE NULL COMMENT '卖2%'," \
                                  "`sell3` INT ( 0 ) NULL COMMENT '卖3'," \
                                  "`sell3per` DOUBLE NULL COMMENT '卖3%'," \
                                  "`sell4` INT ( 0 ) NULL COMMENT '卖4'," \
                                  "`sell4per` DOUBLE NULL COMMENT '卖4%'," \
                                  "`sell5` INT ( 0 ) NULL COMMENT '卖5'," \
                                  "`sell5per` DOUBLE NULL COMMENT '卖5%'," \
                                  " `inner` double NULL COMMENT '内盘'," \
                                  " `outer` double NULL COMMENT '外盘'" \
                                  ");"



# 链接数据库
def connect():
    try:
        db = pyq.connect(host='127.0.0.1', port=3306, user='name', password='pwd', database='stock',
                         charset='utf8')
        cursor = db.cursor()
        return {'db': db, 'cursor': cursor}
    except Exception as e:
        print('connect error:', e)


def creatDB(stockCoed):
    con = connect()
    creatsql = "CREATE TABLE IF NOT EXISTS " + "stock_realtime_trade_" + str(
        stockCoed) + "_" + time.strftime("%Y%m%d", time.localtime()) + stock_realtime_trade_creattable
    con['cursor'].execute(creatsql)
    creatsql2 = "CREATE TABLE IF NOT EXISTS stock_history_daily_trade_" + str(
        stockCoed) + stock_history_daily_trade_creattable
    con['cursor'].execute(creatsql2)
    creatsql3 = "CREATE TABLE IF NOT EXISTS stock_timeshare_trade_" + str(stockCoed) + "_" + time.strftime("%Y%m%d", time.localtime()) + stock_timeshare_trade_creattable
    con['cursor'].execute(creatsql3)


def inserRealtimeTrade(data):
    con = connect()
    query = "INSERT INTO stock_realtime_trade_" + str(data[0]) + "_" + time.strftime("%Y%m%d", time.localtime()) + stock_realtime_trade_inser
    try:
        con['cursor'].execute(query, data[1])
        con['db'].commit()
    except Exception as e:
        con['db'].rollback()
        print(e.args)


def inserHistoryTrade(data):
    con = connect()
    query = "INSERT INTO stock_history_daily_trade_" + str(data[0]) + stock_history_trade_inser
    try:
        con['cursor'].execute(query, data[1])
        con['db'].commit()
    except Exception as e:
        con['db'].rollback()
        print(e.args)


def inserTimeShareTrade(data):
    con = connect()
    query = "INSERT INTO stock_timeshare_trade_" + str(data[0]) + "_" + time.strftime("%Y%m%d", time.localtime()) + stock_timeshare_trade_inser
    try:
        con['cursor'].execute(query, data[1])
        con['db'].commit()
    except Exception as e:
        con['db'].rollback()
        print(e.args)
