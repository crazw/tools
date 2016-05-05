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
        days_ago = 7
        metric = 1
        c_start_time = c_end_time = s_start_time = s_end_time = None
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
            s_start_time = \
                (datetime.datetime.now() - datetime.timedelta(days=int(days_ago))).strftime('%Y-%m-%d') + ' 00:00:00'
            s_end_time = datetime.datetime.now().strftime('%Y-%m-%d') + ' 00:00:00'

            c_start_time = \
                (datetime.datetime.now() - datetime.timedelta(days=int(days_ago) + 1)).strftime(
                    '%Y-%m-%d') + ' 00:00:00'
            c_end_time = \
                (datetime.datetime.now() - datetime.timedelta(days=int(days_ago))).strftime('%Y-%m-%d') + ' 00:00:00'

        return c_start_time, c_end_time, s_start_time, s_end_time, int(metric), out

    def get_args(self):
        __args = dict({})

        # 检查参数
        __args['c_start_time'], __args['c_end_time'], __args['s_start_time'], \
        __args['s_end_time'], __args['metric'], __args['out'] = self.__check_args()

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
            print query
            raise e
        else:
            number_rows = self.__session.rowcount
            if self.__session.description:
                number_columns = len(self.__session.description)
            else:
                print query
                print self.__session.fetchall()
                exit()

            if number_rows >= 1 and number_columns > 1:
                result = [item for item in self.__session.fetchall()]
            else:
                result = [item[0] for item in self.__session.fetchall()]
        finally:
            self.__close()

        return result
        # End def select

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
        self.out = args['out']
        self.mysqlcon = MysqlPython(host="192.168.1.181", user='root', passwd='0rd1230ac', db='cache')
        self.tables = list(['video', 'mobile', 'http'])
        self.c_start_time = args['c_start_time']
        self.c_end_time = args['c_end_time']
        self.s_start_time = args['s_start_time']
        self.s_end_time = args['s_end_time']
        self.metric = args['metric']

    # 获取磁盘信息, 单位是: T
    @staticmethod
    def __get_disk_info():
        hd = dict({})
        disk_cmd = list([])
        for filed in [2, 3]:
            disk_cmd.append("df | awk -F ' ' '{sum+=$" + str(filed) + "} END {print sum}'")

        hd['capacity'] = round(int(commands.getoutput(disk_cmd[0])) / 1024.0 / 1024 / 1024, 3)
        hd['used'] = round(int(commands.getoutput(disk_cmd[1])) / 1024.0 / 1024 / 1024, 3)
        return hd

    # 获取总的缓存收益情况
    def __get_cache_info(self, table):
        cache_info = dict({})
        cache_info['cache_size'] = \
            cache_info['service_size'] = \
            cache_info['cache_metric_size'] = \
            cache_info['service_metric_size'] = 0

        cache_table = table + '_cache_log'
        service_table = table + '_service_log'

        print '\t -- ' + table
        print '\t -- 判断索引是否已存在?'
        is_exists = None
        check_index = 'SHOW INDEX FROM {0}'.format(cache_table)
        check_index_rs = self.mysqlcon.select(check_index)
        for item in check_index_rs:
            if item[2] == 'md5':
                print '\t -- 索引已存在!!!'
                is_exists = True

        if not is_exists:
            print '\t -- 创建索引...'
            create_cache_index = 'ALTER TABLE {0} ADD INDEX `{1}` (`{2}`)'.format(cache_table, 'md5', 'md5')
            self.mysqlcon.opt_table(create_cache_index)
            create_service_index = 'ALTER TABLE {0} ADD INDEX `{1}` (`{2}`)'.format(service_table, 'md5', 'md5')
            self.mysqlcon.opt_table(create_service_index)

        print '\t -- 统计数据...'
        cache_sql = 'SELECT sum(cache_size) FROM {0} WHERE create_time > "{1}" AND create_time <= "{2}" '.format(
            cache_table, self.c_start_time, self.c_end_time)
        cache_info['cache_size'] += self.mysqlcon.select(cache_sql)[0]

        sql = 'SELECT  a.md5 as md5, a.cache_size as cache_size, sum(b.service_size) as service_size ' \
              'FROM {0} AS a , {1} AS b ' \
              'WHERE a.md5 = b.md5 ' \
              'AND a.create_time > "{2}" ' \
              'AND a.create_time <= "{3}" ' \
              'AND b.create_time > "{4}" ' \
              'AND b.create_time <= "{5}" ' \
              'GROUP BY a.md5'.format(cache_table, service_table, self.c_start_time, self.c_end_time,
                                      self.s_start_time, self.s_end_time)
        rs = self.mysqlcon.select(sql)

        for i in rs:
            cache_size = i[1]
            service_size = i[2]
            cache_info['service_size'] += service_size
            if service_size / cache_size > self.metric:
                cache_info['cache_metric_size'] += cache_size
                cache_info['service_metric_size'] += service_size

        # 单位转换成T
        # 总缓存,总服务,达标缓存,达标服务
        for field in ['cache_size', 'service_size', 'cache_metric_size', 'service_metric_size']:
            cache_info[field] = round(int(cache_info[field]) / 1024.0 / 1024 / 1024 / 1024, 3)

        # 总收益: 服务/缓存
        if cache_info['cache_size'] != 0:
            # 返回: 百分比
            cache_info['profit'] = round(100 * cache_info['service_size'] / float(cache_info['cache_size']), 3)
        else:
            print '缓存资源总和为0, 应该是什么出了问题!'
            cache_info['profit'] = 0

        # 缓存占比
        if cache_info['cache_size'] != 0:
            cache_info['cache_metric_all'] = round(
                100 * cache_info['cache_metric_size'] / float(cache_info['cache_size']), 3)
        else:
            cache_info['cache_metric_all'] = 0

        # 服务占比
        if cache_info['service_size'] != 0:
            cache_info['service_metric_all'] = round(
                100 * cache_info['service_metric_size'] / float(cache_info['service_size']), 3)
        else:
            cache_info['service_metric_all'] = 0

        return cache_info

    def report_cont(self):
        cache_infos = list([])
        dot_num = 105

        print "\n===> 数据处理:"
        for table in self.tables:
            table_info = self.__get_cache_info(table)
            cache_infos.append(table_info)
            print '\t' + '-' * 20

        disk_info = self.__get_disk_info()

        print "\n===> 磁盘信息:\n" \
              + "\t总容量(T): " + str(disk_info['capacity']) \
              + "\t已用(T): " + str(disk_info['used'])

        print "\n===> 统计结果:"
        print '\t' + '=' * dot_num + '\n\t' + \
              str('类型').center(10, ' ') + \
              str("缓存总量").center(20, ' ') + \
              str("服务总量").center(20, ' ') + \
              str("收益").center(10, ' ') + \
              str("缓存(收益>=" + str(int(self.metric) * 100) + '%)').center(23, ' ') + \
              str("占比").center(10, ' ') + \
              str("服务(收益>=" + str(int(self.metric) * 100) + '%)').center(23, ' ') + \
              str("占比").center(10, ' ') + \
              '\n\t' + '-' * dot_num

        for i in [0, 1, 2]:
            print '\t' + str(self.tables[i]).center(10, ' ') + \
                  str(str(cache_infos[i]['cache_size']) + 'T').center(13, ' ') + \
                  str(str(cache_infos[i]['service_size']) + 'T').center(15, ' ') + \
                  str(str(cache_infos[i]['profit']) + '%').center(13, ' ') + \
                  str(str(cache_infos[i]['cache_metric_size']) + 'T').center(15, ' ') + \
                  str(str(cache_infos[i]['cache_metric_all']) + '%').center(13, ' ') + \
                  str(str(cache_infos[i]['service_metric_size']) + 'T').center(13, ' ') + \
                  str(str(cache_infos[i]['service_metric_all']) + '%').center(13, ' ') + \
                  '\n\t' + '-' * dot_num
        # print disk_info, cache_infos
        print ''


def main():
    # 解析参数
    ap_class = ArgsParse()
    args = ap_class.get_args()

    print '\n===> 相关参数\n' \
          + "\t收益阀值: (" + str(int(args['metric']) * 100) + '%)' \
          + "\n\t缓存开始时间: (" + str(args['c_start_time']) + ")" \
          + "\t缓存结束时间: (" + str(args['c_end_time']) + ")" \
          + "\n\t服务开始时间: (" + str(args['s_start_time']) + ")" \
          + "\t服务结束时间: (" + str(args['s_end_time']) + ")"

    # 处理数据
    cr_class = CdsReport(args)
    cr_class.report_cont()


if __name__ == '__main__':
    main()
