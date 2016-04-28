#!/usr/bin/env python
# encoding: utf-8

"""
@     file: check_finfo.py
@     time: 4/20/16 3:07 PM
@   author: crazw
@     site: www.crazw.com
@  contact: craazw@gmail.com
@ function: 检测http_cache,mobile_cache,video_cache表中的所有记录,临时插入数据库版
"""
import MySQLdb
import sys
import os
import hashlib
import datetime
import signal


class GracefulInterruptHandler(object):
    def __init__(self, sig=signal.SIGINT):
        self.sig = sig

    def __enter__(self):
        self.interrupted = False
        self.released = False

        self.original_handler = signal.getsignal(self.sig)

        def handler(signum, frame):
            self.release()
            self.interrupted = True

        signal.signal(self.sig, handler)

        return self

    def __exit__(self, type, value, tb):
        self.release()

    def release(self):
        if self.released:
            return False

        signal.signal(self.sig, self.original_handler)

        self.released = True

        return True


class ArgsParse(object):
    """
    解析参数
    """

    def __init__(self):
        self.argv = sys.argv
        self.help = '''
---------------------------------------------------------------------
帮助信息:
    # 参数
    --help, -h 查看此帮助信息;

    --check   http | mobile | video | all
                说明: 选择检测的表;

    --type    finfo | recache
                说明: finfo检测finfo与file_md5的预期情况,
                     recache: 检测cacheid与file_md5的预期情况;
    --create_time
                说明: 限定搜索的开始时间

    # 日志模式(可选)
    --debug
                说明: 开启显示时间戳的走秒输出 + 检测完成报表,
                默认: 只显示检测完成报表;

    # 结果输出(可选)
    -o          说明: 导出生成信息的结果到指定路径, eg: ./result.txt

    --count     说明: 输出不符合预期的TopN;
---------------------------------------------------------------------

    '''

    def __check_args(self):
        table = itype = debug = out = count = create_time = False
        for item in self.argv[1:]:
            if "--check=" in item:
                table = item.split('=')[1]
            elif "--type=" in item:
                itype = item.split('=')[1]
            elif "-o=" in item:
                out = item.split('=')[1]
            elif "--debug" in item:
                debug = True
            elif "--count" in item:
                count = item.split('=')[1]
            elif "--create_time" in item:
                create_time = item.split('=')[1]
            else:
                print self.help
                exit()

        if not table or table not in ["http", "mobile", "video", "all"]:
            print '-' * 69 + '\n'
            print "错误信息:\n\t table参数错误,请重试!"
            print self.help
            exit()

        if not itype or itype not in ["recache", "finfo"]:
            print '-' * 69 + '\n'
            print "错误信息:\n\t type参数错误,请重试!"
            print self.help
            exit()

        if out == '' or (out and os.path.exists(out)):
            print '-' * 69 + '\n'
            print "错误信息:\n\t 输出文件 " + out + " 已存在,请重试!"
            print self.help
            exit()

        return table, debug, itype, out, count, create_time

    def get_args(self):
        __args = dict({})

        # 检查参数
        __args['table'], __args['debug'], __args['itype'], __args['out'], __args['count'], __args[
            'create_time'] = self.__check_args()

        return __args


class MysqlPython(object):
    """ Python Class for MySQL """

    __instance = None
    __host = None
    __user = None
    __passwd = None
    __db = None
    __session = None
    __connection = None

    def __init__(self, host='localhost', user='root', passwd='', db=''):
        self.__host = host
        self.__user = user
        self.__passwd = passwd
        self.__db = db

    # End def __init__

    def __open(self):
        try:
            cnx = MySQLdb.connect(host=self.__host, user=self.__user, passwd=self.__passwd, db=self.__db)
            self.__connection = cnx
            self.__session = cnx.cursor()
        except MySQLdb.Error as e:
            print "Error %d: %s" % (e.args[0], e.args[1])
            exit()

    # End def __open

    def __close(self):
        self.__session.close()
        self.__connection.close()

    # End def __close

    def select(self, query, *args, **kwargs):
        query = query

        try:
            self.__open()
            self.__session.execute(query)
        except Exception, e:
            raise e
        else:
            number_rows = self.__session.rowcount
            number_columns = len(self.__session.description)

            if number_rows >= 1 and number_columns > 1:
                result = [item for item in self.__session.fetchall()]
            else:
                result = [item[0] for item in self.__session.fetchall()]
        finally:
            self.__close()

        return result
        # End def select

    def update(self, query, *args, **kwargs):
        query = query
        try:
            self.__open()
            self.__session.execute(query)
            self.__connection.commit()
        except Exception, e:
            raise e
        else:
            # Obtain rows affected
            update_rows = self.__session.rowcount
        finally:
            self.__close()

        return update_rows

    # End function update

    def insert(self, query, *args, **kwargs):
        query = query
        try:
            self.__open()
            self.__session.execute(query)
            self.__connection.commit()
        except Exception, e:
            print query
            raise e
        finally:
            self.__close()

        return self.__session.lastrowid

    # End def insert

    def delete(self, query, *args, **kwargs):
        query = query

        try:
            self.__open()
            self.__session.execute(query)
            self.__connection.commit()
        except Exception, e:
            raise e
        else:
            # Obtain rows affected
            delete_rows = self.__session.rowcount
        finally:
            self.__close()

        return delete_rows

    # End def delete

    def opt_table(self, query, *args, **kwargs):
        query = query

        try:
            self.__open()
            self.__session.execute(query)
            self.__connection.commit()
        except Exception, e:
            raise e
        finally:
            self.__close()
            # End def create


def md5hex(word):
    """ MD5加密算法，返回32位小写16进制符号 """
    if isinstance(word, unicode):
        word = word.encode("utf-8")
    elif not isinstance(word, str):
        word = str(word)
    m = hashlib.md5()
    m.update(word)
    return m.hexdigest()


def get_microsecond():
    now_time = datetime.datetime.now()
    datetime_str = now_time.strftime('%Y-%m-%d %H:%M:%S')
    microsecond = \
        now_time.day * 24 * 60 * 60 * 1000000 + \
        now_time.hour * 60 * 60 * 1000000 + \
        now_time.minute * 60 * 1000000 + \
        now_time.second * 1000000 + \
        now_time.microsecond

    return datetime_str, microsecond


def create_tmp_table(con):
    delete_tmp_table(con)
    print '===> 创建临时数据库'
    # 创建临时数据库
    create_sql = """CREATE TABLE `test_check_finfo` (
                      `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
                      `finfo` char(50) DEFAULT NULL,
                      `file_md5` char(50) DEFAULT NULL,
                      `filename` varchar(100) DEFAULT NULL,
                      `file_time` datetime DEFAULT NULL,
                      `cache_size` bigint(20) DEFAULT NULL,
                      `create_time` datetime DEFAULT NULL,
                      `uri` varchar(200) DEFAULT NULL,
                      `md5` char(50) DEFAULT NULL,
                      `type` char(2) DEFAULT NULL,
                      PRIMARY KEY (`id`)
                    ) ENGINE=MyISAM DEFAULT CHARSET=utf8;"""

    con.opt_table(create_sql)


def delete_tmp_table(con):
    print '===> 删除临时数据库'
    delete_sql = "DROP TABLE IF EXISTS test_check_finfo"
    con.opt_table(delete_sql)


def finfo_rep(con, table, debug, create_time, itype, out, count):
    # 读取数据
    if create_time:
        sub_create_time = ' WHERE create_time >= "' + create_time + '"'
    else:
        sub_create_time = ''

    sql = 'SELECT file_md5, filename, file_time, cache_size, create_time, uri, md5 FROM ' + table + sub_create_time

    rs = con.select(sql)
    good_item = checked_item = bad_item = 0
    a, last_microsecond = get_microsecond()

    with GracefulInterruptHandler() as h:
        for item in rs:
            if h.interrupted:
                print "Ctrl + C  中断!"
                break

            now_time, now_microsecond = get_microsecond()
            spend_microsecond = now_microsecond - last_microsecond

            checked_item += 1
            if debug and spend_microsecond > 5000000:
                last_microsecond = now_microsecond
                print '===>  ' + now_time + '  已经检测: (' + str(checked_item) + ')   达到预期: (' + str(
                    good_item) + ')   不符预期: (' + str(bad_item) + ')'

            # 去除file_md5, filename, file_time, cache_size为空的
            if None in item or '' in item:
                continue
            else:
                md5_str = item[1] + item[2].strftime('%Y-%m-%d %H:%M:%S') + str(item[3])

            # 转化md5
            if itype == 'finfo':
                finfo = md5hex(md5_str)
            else:
                finfo = item[0]

            file_md5 = item[0]
            filename = item[1]
            file_time = item[2].strftime('%Y-%m-%d %H:%M:%S')
            cache_size = str(item[3])
            new_create_time = item[4].strftime('%Y-%m-%d %H:%M:%S')
            uri = item[5].replace('\"', '\'')
            md5 = item[6]
            tmptype = "0"
            if itype == 'finfo':
                check_sql = 'SELECT file_md5 FROM test_check_finfo WHERE finfo = "' + finfo + '"'
            else:
                check_sql = 'SELECT md5 FROM test_check_finfo WHERE finfo = "' + finfo + '"'

            check_rs = con.select(check_sql)
            # 没有查到, 就插入
            if len(check_rs) == 0:
                good_item += 1
                insert_sql = 'INSERT INTO test_check_finfo ' \
                             '(finfo, file_md5, filename, file_time, cache_size, create_time, uri, md5, type) ' \
                             'VALUES ("' + finfo + '", "' + file_md5 + '", "' + filename + '", "' \
                             + file_time + '", ' + cache_size + ', "' + new_create_time + '", "' \
                             + uri + '", "' + md5 + '", "' + tmptype + '")'
                con.insert(insert_sql)
            else:
                is_exists = False
                if itype == 'finfo':
                    for j in range(len(check_rs)):
                        # 查到且file_md5已存在, 就说明已存在
                        if check_rs[j] == file_md5:
                            is_exists = True
                else:
                    for j in range(len(check_rs)):
                        # 查到且md5已存在, 就说明已存在
                        if check_rs[j] == md5:
                            is_exists = True

                if not is_exists:
                    bad_item += 1
                    tmptype = "1"
                    # 更新已经存在的
                    update_sql = 'UPDATE test_check_finfo SET type = "1" WHERE finfo = "' + finfo + '"'
                    con.update(update_sql)

                    # 插入不符合预期且不重复的条目
                    insert_sql = 'INSERT INTO test_check_finfo ' \
                                 '(finfo, file_md5, filename, file_time, cache_size, create_time, uri, md5, type) ' \
                                 'VALUES ("' + finfo + '", "' + file_md5 + '", "' + filename + '", "' \
                                 + file_time + '", ' + cache_size + ', "' + new_create_time + '", "' \
                                 + uri + '", "' + md5 + '", "' + tmptype + '")'
                    con.insert(insert_sql)
                else:
                    good_item += 1



    # 查询临时表type = 1的数据
    print '*' * 40 + " 不符预期 " + '*' * 40

    if count:
        sub_count = ' LIMIT ' + str(count)
    else:
        sub_count = ''
    rep_sql = 'SELECT finfo, count(finfo) FROM test_check_finfo WHERE type = "1" ' \
              'GROUP BY finfo ORDER BY count(finfo) DESC' + sub_count

    rep_rs = con.select(rep_sql)

    for itype_item in rep_rs:
        itype_finfo = itype_item[0]
        itype_sql = 'SELECT finfo, file_md5, filename, file_time, cache_size, create_time, uri, md5 FROM' \
                    ' test_check_finfo WHERE type = "1" and finfo = "' + itype_finfo + '"'
        itype_rs = con.select(itype_sql)

        if out:
            with open(out, 'a') as g:
                g.write('-' * 40 + table + '-' * 40 + '\n')
                g.write("字段: finfo, file_md5, filename, file_time, cache_size, create_time, uri, md5" + '\n')
                for item in itype_rs:
                    g.write(item[0] + ' | ' + item[1] + ' | ' + item[2] + ' | ' + item[3].strftime(
                        '%Y-%m-%d %H:%M:%S') + ' | ' + str(item[4]) + ' | ' + item[5].strftime(
                        '%Y-%m-%d %H:%M:%S') + ' | ' + item[6] + ' | ' + item[7] + '\n')
        else:
            print '-' * 40 + table + '-' * 40
            print "字段: finfo, file_md5, filename, file_time, cache_size, create_time, uri, md5"
            for item in itype_rs:
                print item[0] + ' | ' + item[1] + ' | ' + item[2] + ' | ' + item[3].strftime(
                    '%Y-%m-%d %H:%M:%S') + ' | ' + str(item[4]) + ' | ' + item[5].strftime(
                    '%Y-%m-%d %H:%M:%S') + ' | ' + item[6] + ' | ' + item[7]

    return checked_item, good_item, bad_item


def main():
    # 解析参数
    ap_class = ArgsParse()
    args = ap_class.get_args()

    print '===> 相关参数\n    ' + str(args)

    # 检测数据
    con = MysqlPython(host="localhost", user='root', passwd='', db='cache')

    if args['table'] == 'all':
        tables = list(['video_cache', 'mobile_cache', 'http_cache'])
    else:
        tables = list([args['table'] + '_cache'])

    for table in tables:
        # 创建临时数据库
        print '\n'
        create_tmp_table(con)

        begin_time = datetime.datetime.now()
        print '\n' + '*' * 90
        print '\n===> 检查表: ' + table + '\n'

        # 检查
        checked_item, good_item, bad_item = finfo_rep(con, table, args['debug'], args['create_time'],
                                                      args['itype'], args['out'], args['count'])

        end_time = datetime.datetime.now()
        cost_time = (end_time - begin_time).seconds
        end_time_str = end_time.strftime('%Y-%m-%d %H:%M:%S')
        print '\n' + '*' * 40 + " 时间统计 " + '*' * 40
        print '===> 开始时间: ' + begin_time.strftime('%Y-%m-%d %H:%M:%S') + ' 共耗时: ' + str(cost_time) + ' 秒'
        print '===> 结束时间: ' + end_time_str + ' 已经检测: (' + str(checked_item) + ')   达到预期: (' + str(
            good_item) + ')   不符预期: (' + str(bad_item) + ')'

        # 删除临时数据库
        delete_tmp_table(con)


if __name__ == '__main__':
    main()
