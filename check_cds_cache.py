#!/usr/bin/env python
# encoding: utf-8

"""
@     file: check_cds_cache.py
@     time: 4/27/16 4:38 PM
@   author: crazw
@     site: www.crazw.com
@  contact: craazw@gmail.com
@ function: 缓存资源的价值评估
"""

import MySQLdb
import sys
import os
import datetime
import commands


class ArgsParse(object):
    """
    解析参数
    """

    def __init__(self):
        self.argv = sys.argv
        self.help = '''
---------------------------------------------------------------------

功能说明:
    缓存资源的价值评估

帮助信息:
    --help, -h  查看此帮助信息;

    # 相关选项
    -d      说明: 限定统计的开始时间, 默认: 昨天, 且: 1代表1天前 , 2代表2天前, 以此类推;

    # 结果输出
    -o      说明: 导出生成信息的结果到指定路径, 默认: ./result/;
    -m      说明: 统计阈值, 默认: 1, 且: 1代表100% , 2代表200%, 以此类推;

---------------------------------------------------------------------

    '''

    def __check_args(self):
        days_ago = 1
        metric = 1
        start_time = end_time = None
        out = './result/'

        for item in self.argv[1:]:
            if "-o=" in item:
                out = item.split('=')[1]
            elif "-m=" in item:
                metric = item.split('=')[1]
            elif "-d=" in item:
                days_ago = item.split('=')[1]
            else:
                print self.help
                exit()

        if metric < 1:
            print '-' * 69 + '\n'
            print "错误信息:\n\t -m 参数不应该低于1,请重试!"
            print self.help
            exit()

        if not os.path.exists(out):
            print '-' * 69 + '\n'
            print "提示信息:\n\t 结果的输出文件夹 " + out + " 不存在,脚本将自动创建!"
            commands.getoutput('mkdir -p ' + out)

        if int(days_ago):
            end_time = datetime.datetime.now().strftime('%Y-%m-%d') + ' 00:00:00'
            start_time = \
                (datetime.datetime.now() - datetime.timedelta(days=int(days_ago))).strftime('%Y-%m-%d') + ' 00:00:00'

        return start_time, end_time, int(metric), out

    def get_args(self):
        __args = dict({})

        # 检查参数
        __args['start_time'], __args['end_time'], __args['metric'], __args['out'] = self.__check_args()

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


class CdsReport(object):
    def __init__(self, args):
        self.con = MysqlPython(host="localhost", user='root', passwd='', db='cache')
        self.metric = args['metric']
        self.out = args['out']
        self.start_time = args['start_time']
        self.end_time = args['end_time']
        self.tables = list(['video_cache', 'mobile_cache', 'http_cache'])

    # 获取磁盘信息, 单位是: T
    @staticmethod
    def __get_disk_info():
        hd=dict({})
        disk_cmd = list([])
        for filed in [2, 3]:
            disk_cmd.append("df | awk -F ' ' '{sum+=$" + str(filed) + "} END {print sum}'")

        hd['capacity'] = round(int(commands.getoutput(disk_cmd[0])) / 1024.0 / 1024 / 1024, 3)
        hd['used'] = round(int(commands.getoutput(disk_cmd[1])) / 1024.0 / 1024 / 1024, 3)
        return hd

    # 获取总的缓存收益情况
    def __get_cache_info(self, table):
        cache_info = dict({})

        all_size_sql = \
            'SELECT sum(cache_size),sum(service_size) FROM ' +\
            table + ' WHERE create_time >= "' +\
            self.start_time + '" AND create_time < "' +\
            self.end_time + '"'

        all_size_rs = self.con.select(all_size_sql)

        if all_size_rs[0][0]:
            # 单位都是T
            cache_info['cache_size'] = round(int(all_size_rs[0][0]) / 1024.0 / 1024 / 1024 / 1024, 3)
            cache_info['service_size'] = round(int(all_size_rs[0][1]) / 1024.0 / 1024 / 1024 / 1024, 3)
            if cache_info['cache_size'] != 0:
                # 返回: 百分比
                cache_info['profit'] = round(100 * cache_info['service_size'] / float(cache_info['cache_size']), 3)
            else:
                print '缓存资源总和为0, 应该是什么出了问题!'
                cache_info['profit'] = 0

            metric_size_sql = all_size_sql + " AND service_size/cache_size > " + str(self.metric)
            metric_size_rs = self.con.select(metric_size_sql)
            if metric_size_rs[0][0]:
                cache_info['cache_metric_size'] = round(int(metric_size_rs[0][0]) / 1024.0 / 1024 / 1024 / 1024, 3)
                cache_info['service_metric_size'] = round(int(metric_size_rs[0][1]) / 1024.0 / 1024 / 1024 / 1024, 3)
            else:
                cache_info['cache_metric_size'] = cache_info['service_metric_size'] = 0
        else:
            cache_info['cache_size'] = cache_info['service_size'] = cache_info['profit'] = \
                cache_info['cache_metric_size'] = cache_info['service_metric_size'] = 0

        return cache_info

    def report_cont(self):
        cache_infos = list([])
        dot_num = 85

        disk_info = self.__get_disk_info()
        print "\n===> 磁盘信息:\n" \
              + "\t总容量(T): " + str(disk_info['capacity']) \
              + "\t已用(T): " + str(disk_info['used'])

        print "\n===> 统计结果:"
        print '\t' + '=' * dot_num + '\n\t' +\
            str('类型').center(10, ' ') + \
            str("缓存总量").center(20, ' ') + \
            str("服务总量").center(20, ' ') + \
            str("收益").center(10, ' ') + \
            str("缓存(收益>=" + str(int(self.metric) * 100) + '%)').center(23, ' ') + \
            str("服务(收益>=" + str(int(self.metric) * 100) + '%)').center(23, ' ') + \
            '\n\t' + '-' * dot_num

        for table in self.tables:
            table_info = self.__get_cache_info(table)
            cache_infos.append(table_info)

        type_list = list(['Video', 'Mobile', 'Http'])
        for i in [0, 1, 2]:

            print '\t' + str(type_list[i]).center(10, ' ') + \
                str(str(cache_infos[i]['cache_size']) + 'T').center(13, ' ') + \
                str(str(cache_infos[i]['service_size']) + 'T').center(15, ' ') + \
                str(str(cache_infos[i]['profit']) + '%').center(13, ' ') + \
                str(str(cache_infos[i]['cache_metric_size']) + 'T').center(15, ' ') + \
                str(str(cache_infos[i]['service_metric_size']) + 'T').center(15, ' ') + \
                '\n\t' + '-' * dot_num
        # print disk_info, cache_infos


def main():
    # 解析参数
    ap_class = ArgsParse()
    args = ap_class.get_args()

    print '\n===> 相关参数\n'\
          + "\t收益阀值: (" + str(int(args['metric']) * 100) + '%)' \
          + "\t开始时间: (" + str(args['start_time']) + ")" \
          + "\t结束时间: (" + str(args['end_time']) + ")"

    cr_class = CdsReport(args)
    cr_class.report_cont()


if __name__ == '__main__':
    main()
