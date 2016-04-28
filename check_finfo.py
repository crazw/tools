#!/usr/bin/env python
# encoding: utf-8

"""
@     file: check_finfo.py
@     time: 4/20/16 3:07 PM
@   author: crazw
@     site: www.crazw.com
@  contact: craazw@gmail.com
@ function: 检测http_cache,mobile_cache,video_cache表中的所有记录
"""
import MySQLdb
import sys
import os
import hashlib
import datetime


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
                说明: 选择检测的表 ;

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
    microsecond = now_time.hour * 60 * 1000000 + now_time.minute * 60 * 1000000 + now_time.second * 1000000 + now_time.microsecond
    return datetime_str, microsecond


def finfo_rep(con, table, debug, create_time, itype):
    if create_time:
        sub_create_time = ' WHERE create_time >= "' + create_time + '"'
    else:
        sub_create_time = ''

    sql = 'SELECT file_md5, filename, file_time, cache_size, md5 FROM ' + table + sub_create_time

    rs = con.select(sql)
    tmp_list = list([])
    rs_tmp_list = list([])
    checked_num = 0
    bad_item_len = 0
    good_item_len = 0
    a, last_microsecond = get_microsecond()
    for item in rs:
        now_time, now_microsecond = get_microsecond()
        spend_microsecond = now_microsecond - last_microsecond

        checked_num += 1
        tmp_dict = dict({})
        bad_item_len = len(rs_tmp_list)

        if debug and spend_microsecond > 5000000:
            # print now_microsecond, last_microsecond, spend_microsecond
            last_microsecond = now_microsecond
            print '===>  ' + now_time + '  已经检测: (' + str(checked_num) + ')   达到预期: (' + str(
                good_item_len) + ')   不符预期: (' + str(bad_item_len) + ')'

        # 去除file_md5, filename, file_time, cache_size为空的
        if None in item or '' in item:
            continue
        else:
            md5_str = item[1] + item[2].strftime('%Y-%m-%d %H:%M:%S') + str(item[3])

        # 转化md5
        if itype == 'finfo':
            finfo = md5hex(md5_str)
        else:
            finfo = item[4]

        tmp_dict[finfo] = list([item[0]])
        rs_tmp_dict = dict({})

        # 检查是否重复
        is_exists = False
        is_index = None
        is_rs_exists = False
        is__rs_index = None
        for i in range(len(tmp_list)):
            if finfo in tmp_list[i].keys():
                is_exists = True
                is_index = i

        if not is_exists:
            good_item_len += 1
            tmp_list.append(tmp_dict)
        else:
            if item[0] in tmp_list[is_index][finfo]:
                good_item_len += 1
            else:
                tmp_list[is_index][finfo].append(item[0])
                rs_tmp_dict[finfo] = tmp_list[is_index][finfo]
                for j in range(len(rs_tmp_list)):
                    if finfo in rs_tmp_list[j].keys():
                        is_rs_exists = True
                        is__rs_index = j
                if not is_rs_exists:
                    rs_tmp_list.append(rs_tmp_dict)
                else:
                    rs_tmp_list[is__rs_index][finfo] = tmp_list[is_index][finfo]

    # 结果过滤
    rs2_tmp_list = list([])
    if create_time:
        sub_create_time = '" AND create_time >= "' + create_time
    else:
        sub_create_time = ''

    for i in range(len(rs_tmp_list)):
        rs2_tmp_dict = dict({})
        finfo = rs_tmp_list[i].keys()[0]
        rs2_tmp_dict['info'] = list([])
        for rs_file_md5 in rs_tmp_list[i].values()[0]:
            rs_sql = 'SELECT file_md5, filename, file_time, cache_size, create_time, uri, md5 FROM ' \
                     + table + ' WHERE file_md5 = "' + rs_file_md5 + sub_create_time + '" LIMIT 1'
            rs_item = con.select(rs_sql)
            for j in rs_item:
                rs2_tmp_dict['info'].append(str(finfo) + ' | ' + str(j))

        rs2_tmp_list.append(rs2_tmp_dict)

    return checked_num, rs2_tmp_list, bad_item_len, good_item_len


def main():
    # 解析参数
    ap_class = ArgsParse()
    args = ap_class.get_args()

    print args

    # 检测数据
    con = MysqlPython(host="localhost", user='root', passwd='', db='cache')

    if args['table'] == 'all':
        tables = list(['http_cache', 'mobile_cache', 'video_cache'])
    else:
        tables = list([args['table'] + '_cache'])

    for table in tables:
        begin_time = datetime.datetime.now()
        print '=' * 90
        print '\n===> 检查表: ' + table + '\n'
        # 检查
        checked_num, rs_tmp_list, bad_item_len, good_item_len = \
            finfo_rep(con, table, args['debug'], args['create_time'], args['itype'])

        print '*' * 40 + " 不符预期 " + '*' * 40
        out_lines = 0
        rs_tmp_list = sorted(rs_tmp_list, key=lambda n: len(n['info']), reverse=True)

        for item in rs_tmp_list:
            print out_lines
            item_len = len(item['info'])
            if args['count']:
                if out_lines < int(args['count']):
                    if args['out']:
                        with open(args['out'], 'a') as g:
                            g.write('-' * 40 + table + '-' * 40 + '\n')
                            for i in range(item_len):
                                g.write(str(item['info'][i]) + '\n')
                    else:
                        print '-' * 90
                        for i in range(item_len):
                            print item['info'][i]
                else:
                    break

            # 没有TopN参数时
            else:
                if args['out']:
                    with open(args['out'], 'a') as g:
                        g.write('-' * 40 + table + '-' * 40 + '\n')
                        for i in range(item_len):
                            g.write(str(item['info'][i]) + '\n')
                else:
                    print '-' * 90
                    for i in range(item_len):
                        print item['info'][i]

        end_time = datetime.datetime.now()
        cost_time = (end_time - begin_time).seconds
        end_time_str = end_time.strftime('%Y-%m-%d %H:%M:%S')
        print '\n' + '*' * 40 + " 时间统计 " + '*' * 40
        print '===> 开始时间: ' + begin_time.strftime('%Y-%m-%d %H:%M:%S') + ' 共耗时: ' + str(cost_time) + ' 秒'
        print '===> 结束时间: ' + end_time_str + ' 已经检测: (' + str(checked_num) + ')   达到预期: (' + str(
            good_item_len) + ')   不符预期: (' + str(bad_item_len) + ')'
        print '\n'


if __name__ == '__main__':
    main()
